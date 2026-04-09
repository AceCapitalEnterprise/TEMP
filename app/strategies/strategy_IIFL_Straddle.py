from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import hashlib
import numpy as np
import pandas as pd
import pandas_ta as ta
import math
from datetime import datetime, date, timedelta, time as t
import csv
import time
import os
import requests
import threading
import json
from scipy.stats import norm
from scipy.optimize import newton
from scipy.optimize import brentq
from math import log, sqrt, exp
import logging
from ..common.logging_config import create_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = create_logger(__name__, 'trading_IIFL_straddle_strategy.log')


class StrategyStraddleIIFL(BaseStrategy):
    def __init__(self, params):
        super().__init__()

        # Initialize BreezeConnect
        try:
            self.breeze = BreezeConnect(api_key=params["api_key"])
            self.breeze.generate_session(
                api_secret=params["api_secret"],
                session_token=params["session_token"]
            )
            logger.info("BreezeConnect initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize BreezeConnect: ", exc_info=e)
            raise e

        self.api_secret = params["api_secret"]
        self.session_token = params["session_token"]
        self.breeze.ws_connect()
        self.breeze.on_ticks = self.on_ticks

        # Variables
        start_time = str(params["start_hour"]) + ":" + str(params["start_minute"])
        end_time   = str(params["end_hour"])   + ":" + str(params["end_minute"])
        self.TIME_1 = datetime.strptime(start_time, "%H:%M").time()
        self.TIME_2 = datetime.strptime(end_time,   "%H:%M").time()
        self.EXPIRY     = params["expiry"]
        self.FUT_EXPIRY = params["fut_expiry"]
        self.QTY        = params["qty"]
        self.csv_file   = params["csv_file"]
        self.temp_file  = "open_position_IIFL.csv"
        self.PROFIT_CUT_QTY = 130
        self.ROUND_OFF      = 50
        self.max_trades     = params["max_position"]

        self.expiry  = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry.strftime('%Y-%m-%d')
        self.expiry2 = self.expiry.strftime('%d-%b-%Y')

        self.first_tick_received = threading.Event()
        self.tick_data_lock      = threading.Lock()
        self.tick_data           = {}
        self.TOKENS              = {}
        self.live_ltp            = {"CE": None, "PE": None}
        self.entry_data          = None
        self.sl_exit_time        = None
        self.keep_receiving      = True

        # Greeks Parameters
        self.RATE_LIMIT_DELAY = 3
        self.RISK_FREE_RATE   = 0.07
        self.DIVIDEND         = 0.014
        self.W_DELTA          = 0.5
        self.W_THETA          = 0.3
        self.W_VEGA           = 0.2

        # ── RAS Parameters ──────────────────────────────────────────────
        self.RAS_1MIN_LOOKBACK  = 5        # bars for acceleration at entry
        self.RAS_1MIN_NORM      = 20       # bars for normalisation at entry
        self.RAS_WEIGHT_DELTA   = 0.2
        self.RAS_WEIGHT_GAMMA   = 0.3
        self.RAS_WEIGHT_VEGA    = 0.5
        self.RAS_ENTRY_LIMIT    = 2.0      # block entry if pre-entry RAS > this
        self.RAS_KILL_SWITCH    = 6.0      # exit immediately if live RAS > this

        # RAS runtime state
        self.live_greeks_history   = []    # list of dicts built tick-by-tick
        self.live_ras              = 0.0
        self.entry_ras             = 0.0
        self.ras_kill_switch_fired = False

        # Live spot cache (refreshed every ~60 s)
        self.live_spot            = None
        self.last_spot_fetch_time = None

        # IIFL Parameters
        self.BASE_URL = "https://api.iiflcapital.com/v1"
        self.URL = {
            "NSEEQ":   "https://api.iiflcapital.com/v1/contractfiles/NSEEQ.json",
            "INDICES": "https://api.iiflcapital.com/v1/contractfiles/INDICES.json",
            "NSEFO":   "https://api.iiflcapital.com/v1/contractfiles/NSEFO.json",
        }

        self.CLIENT_ID  = "76860254"
        # self.AUTH_CODE  = params["auth_code"]
        self.APP_SECRET = (
            "uQgJLoEf7OXIZYFqmd0YxD4mK1AKacJctMQpOTkKdDRlLxSPDHW0AGqDlW3OjtUhD2536L34IUrdtb72VkUlafEyNBdsDHobLcxF"
        )

        # self.USER_SESSION = self.get_user_session(
        #     self.CLIENT_ID, self.AUTH_CODE, self.APP_SECRET
        # )

        self.USER_SESSION = params["auth_code"]

        self.headers = {
            
            "Authorization": f"Bearer {self.USER_SESSION}",
        }

    # ═══════════════════════════════════════════════════════════════════
    # Authentication
    # ═══════════════════════════════════════════════════════════════════

    def generate_checksum(self, client_id: str, auth_code: str, app_secret: str) -> str:
        try:
            raw = client_id + auth_code + app_secret
            return hashlib.sha256(raw.encode()).hexdigest()
        except Exception as e:
            logger.error(f"[ERROR] Failed to generate Hashcode: {e}")

    def get_user_session(self, client_id, auth_code, app_secret):
        checksum = self.generate_checksum(client_id, auth_code, app_secret)
        print(f"{'-'*60}")
        print(f"C_Id: {self.CLIENT_ID}")
        print(f"Auth: {self.AUTH_CODE}")
        print(f"Checksum: {checksum}")
        print(f"{'-'*60}")
        try:
            # checksum = self.generate_checksum(client_id, auth_code, app_secret)
            url      = f"{self.BASE_URL}/getusersession"
            payload  = {"checkSum": checksum}
            r = requests.post(url, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data["status"] != "Ok":
                raise Exception(data)
            print("[DEBUG] IIFL Authentication Successful")
            print(f"{'-'*60}")
            print(f"Sess: {data['userSession']}")
            logger.info(f"Checksum: {checksum}")
            logger.info(f"Sess: {data['userSession']}")
            print(f"{'-'*60}")
            return data["userSession"]
        except Exception as e:
            logger.error(f"[ERROR] IIFL Authentication Failed: {e}")

    # ═══════════════════════════════════════════════════════════════════
    # Live WebSocket
    # ═══════════════════════════════════════════════════════════════════

    def get_key_from_params(self, strike, right):
        normalized_right = right.upper().replace('CALL', 'CE').replace('PUT', 'PE')
        return f"{strike}_{normalized_right}"

    def on_ticks(self, ticks):
        if 'strike_price' not in ticks or 'right' not in ticks:
            print(f"Subscription message: {ticks}")
            return

        strike      = ticks['strike_price']
        right       = ticks['right']
        key         = self.get_key_from_params(strike, right)
        current_ltp = ticks.get("last")

        if current_ltp is None:
            return

        with self.tick_data_lock:
            self.tick_data[key] = ticks
            if key.endswith("CE"):
                self.live_ltp["CE"] = current_ltp
            elif key.endswith("PE"):
                self.live_ltp["PE"] = current_ltp

            if self.live_ltp["CE"] is not None and self.live_ltp["PE"] is not None:
                self.first_tick_received.set()

        print(f"[{datetime.now().strftime('%H:%M:%S.%f')}] {key} LTP: {current_ltp}")

    def initiate_ws(self, strike_price):
        try:
            exp    = str(self.expiry2)
            strike = str(strike_price)
            self.live_ltp["CE"] = None
            self.live_ltp["PE"] = None
            self.first_tick_received.clear()

            self.breeze.subscribe_feeds(
                exchange_code="NFO", stock_code="NIFTY",
                expiry_date=exp, strike_price=strike, right="call",
                product_type="options", get_market_depth=False, get_exchange_quotes=True
            )
            self.breeze.subscribe_feeds(
                exchange_code="NFO", stock_code="NIFTY",
                expiry_date=exp, strike_price=strike, right="put",
                product_type="options", get_market_depth=False, get_exchange_quotes=True
            )
            print(f"Subscribed to {strike_price}_CE and {strike_price}_PE")
            self.first_tick_received.wait(timeout=5)
        except Exception as e:
            logger.error(f"[ERROR] Failed to Initiate Web Socket: {e}")

    def deactivate_ws(self, strike_price):
        try:
            exp    = str(self.expiry2)
            strike = str(strike_price)
            self.breeze.unsubscribe_feeds(
                exchange_code="NFO", stock_code="NIFTY",
                expiry_date=exp, strike_price=strike, right="call",
                product_type="options", get_market_depth=False, get_exchange_quotes=True
            )
            self.breeze.unsubscribe_feeds(
                exchange_code="NFO", stock_code="NIFTY",
                expiry_date=exp, strike_price=strike, right="put",
                product_type="options", get_market_depth=False, get_exchange_quotes=True
            )
            with self.tick_data_lock:
                self.tick_data.clear()
                self.live_ltp["CE"] = None
                self.live_ltp["PE"] = None
            print(f"Unsubscribed {strike_price} straddle")
        except Exception as e:
            logger.error(f"[ERROR] Failed to Deactivate Web Socket: {e}")

    # # ═══════════════════════════════════════════════════════════════════
    # # Order Placement
    # # ═══════════════════════════════════════════════════════════════════

    # def fetch_positions(self):
    #     response = requests.get(f"{self.BASE_URL}/positions", headers=self.headers, timeout=10)
    #     response.raise_for_status()
    #     return response.json()["result"]

    # def fetch_order_book(self):
    #     response = requests.get(f"{self.BASE_URL}/orders", headers=self.headers, timeout=10)
    #     response.raise_for_status()
    #     return response.json()

    # def place_order(self, payload):
    #     try:
    #         response = requests.post(
    #             f"{self.BASE_URL}/orders",
    #             headers=self.headers, json=payload, timeout=10
    #         )
    #         response.raise_for_status()
    #         data = response.json()
    #         if data.get("status") != "Ok":
    #             raise Exception(f"Order failed: {data}")
    #         return data["result"][0]["brokerOrderId"]
    #     except Exception as e:
    #         logger.error(f"[ERROR] place_order failed: {e}")

    # def place_sell_order(self, call_instrument_id, put_instrument_id, quantity, exchange="NSEFO"):
    #     try:
    #         print("Placing Sell Order")
    #         Rp = Rc = None
    #         call_payload = [{
    #             "instrumentId": str(call_instrument_id), "exchange": exchange,
    #             "transactionType": "SELL", "quantity": quantity,
    #             "orderComplexity": "REGULAR", "product": "INTRADAY",
    #             "orderType": "MARKET", "validity": "DAY", "orderTag": "STRADDLE_CALL"
    #         }]
    #         put_payload = [{
    #             "instrumentId": str(put_instrument_id), "exchange": exchange,
    #             "transactionType": "SELL", "quantity": quantity,
    #             "orderComplexity": "REGULAR", "product": "INTRADAY",
    #             "orderType": "MARKET", "validity": "DAY", "orderTag": "STRADDLE_PUT"
    #         }]
    #         call_order_id = self.place_order(call_payload)
    #         time.sleep(1)
    #         put_order_id  = self.place_order(put_payload)
    #         time.sleep(2)

    #         order_book = self.fetch_order_book()
    #         for order in order_book["result"]:
    #             if order["brokerOrderId"] == str(call_order_id) and order["instrumentId"] == str(call_instrument_id):
    #                 Rc = float(order.get("averageTradedPrice"))
    #             elif order["brokerOrderId"] == str(put_order_id) and order["instrumentId"] == str(put_instrument_id):
    #                 Rp = float(order.get("averageTradedPrice"))

    #         resp = float(Rp + Rc)
    #         print(f"[SELL] CE: {Rc} | PE: {Rp} | Total: {resp}")
    #         return resp, Rc, Rp
    #     except Exception as e:
    #         logger.error(f"[ERROR] Failed to Place Sell Order: {e}")

    # def square_off_straddle(self, call_instrument_id, put_instrument_id, quantity, exchange="NSEFO"):
    #     try:
    #         Rc = Rp = call_order_id = put_order_id = None
    #         positions = self.fetch_positions()

    #         for pos in positions:
    #             instrument_id = pos["instrumentId"]
    #             if pos["netQuantity"] != 0 and instrument_id == str(call_instrument_id):
    #                 call_payload = [{
    #                     "instrumentId": instrument_id, "exchange": exchange,
    #                     "transactionType": "BUY", "quantity": quantity,
    #                     "orderComplexity": "REGULAR", "product": pos["product"],
    #                     "orderType": "MARKET", "validity": "DAY", "orderTag": "SQUAREOFF_CALL"
    #                 }]
    #                 call_order_id = self.place_order(call_payload)

    #             if pos["netQuantity"] != 0 and instrument_id == str(put_instrument_id):
    #                 put_payload = [{
    #                     "instrumentId": instrument_id, "exchange": exchange,
    #                     "transactionType": "BUY", "quantity": quantity,
    #                     "orderComplexity": "REGULAR", "product": pos["product"],
    #                     "orderType": "MARKET", "validity": "DAY", "orderTag": "SQUAREOFF_PUT"
    #                 }]
    #                 put_order_id = self.place_order(put_payload)

    #         if call_order_id is None and put_order_id is None:
    #             print("No open positions found to square off")
    #             return None

    #         time.sleep(2)
    #         order_book = self.fetch_order_book()
    #         for order in order_book["result"]:
    #             if order["brokerOrderId"] == str(call_order_id) and order["instrumentId"] == str(call_instrument_id):
    #                 Rc = float(order.get("averageTradedPrice"))
    #             elif order["brokerOrderId"] == str(put_order_id) and order["instrumentId"] == str(put_instrument_id):
    #                 Rp = float(order.get("averageTradedPrice"))

    #         resp = float(Rp + Rc)
    #         print(f"[BUY/SquareOff] CE: {Rc} | PE: {Rp} | Total: {resp}")
    #         return resp
    #     except Exception as e:
    #         logger.error(f"[ERROR] Failed to Place Buy Order: {e}")

    # ═══════════════════════════════════════════════════════════════════
    # Black-Scholes & Greek helpers
    # ═══════════════════════════════════════════════════════════════════

    def black_scholes_call(self, S, K, T, r, q, sigma):
        d1 = (log(S / K) + (r - q + sigma**2 / 2) * T) / (sigma * sqrt(T))
        d2 = d1 - sigma * sqrt(T)
        return S * exp(-q * T) * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)

    def black_scholes_put(self, S, K, T, r, q, sigma):
        d1 = (log(S / K) + (r - q + sigma**2 / 2) * T) / (sigma * sqrt(T))
        d2 = d1 - sigma * sqrt(T)
        return K * exp(-r * T) * norm.cdf(-d2) - S * exp(-q * T) * norm.cdf(-d1)

    def calculate_iv(self, option_price, S, K, T, r, q, option_type):
        """Compute implied volatility via Brent's method (supports call & put)."""
        try:
            if option_type == "call":
                fn = lambda sigma: self.black_scholes_call(S, K, T, r, q, sigma) - option_price
            else:
                fn = lambda sigma: self.black_scholes_put(S, K, T, r, q, sigma) - option_price
            return brentq(fn, 0.01, 2.0)
        except Exception as e:
            logger.error(f"[ERROR] Failed to Calculate IV: {e}")
            return None

    def black_scholes_greeks(self, spot, strike, time_to_expiry, volatility,
                              risk_free_rate, dividend, option_type="call"):
        """
        Returns (delta, theta, vega, gamma).
        Gamma is needed for the RAS computation.
        """
        if time_to_expiry <= 0 or volatility is None or volatility <= 0:
            return 0.0, 0.0, 0.0, 0.0

        d1  = (np.log(spot / strike) +
               (risk_free_rate - dividend + 0.5 * volatility**2) * time_to_expiry) / \
              (volatility * np.sqrt(time_to_expiry))
        d2  = d1 - volatility * np.sqrt(time_to_expiry)
        nd1 = math.exp(-(d1**2) / 2) / math.sqrt(2 * math.pi)
        nd2 = 0.5 * (1 + math.erf(d2 / math.sqrt(2)))

        if option_type == "call":
            delta = nd1
            theta = -((spot * volatility * nd1 / (2 * math.sqrt(time_to_expiry)) -
                       risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * nd2)) / 365
        else:
            delta = nd1 - 1
            theta = -(spot * nd1 * volatility / (2 * math.sqrt(time_to_expiry)) +
                      risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * (1 - nd2)) / 365

        vega  = (spot * nd1 * math.sqrt(time_to_expiry)) / 100   # per 1 % IV change
        gamma = nd1 / (spot * volatility * math.sqrt(time_to_expiry))

        return delta, theta, vega, gamma

    def calculate_greeks_sl(self, ent_time, exi_time, nifty, strike,
                             time_to_expiry, volatility, atr, option_type):
        try:
            delta, theta, vega, gamma = self.black_scholes_greeks(
                nifty, strike, time_to_expiry, volatility,
                self.RISK_FREE_RATE, self.DIVIDEND, option_type
            )
            print(f"Delta: {delta:.4f} | Theta: {theta:.4f} | Vega: {vega:.4f} | Gamma: {gamma:.6f}")

            total_minutes = 369
            minutes_left  = (exi_time.hour * 60 + exi_time.minute) - \
                            (ent_time.hour * 60 + ent_time.minute)
            time_factor   = minutes_left / total_minutes if minutes_left > 0 else 0.01

            spot_move  = atr
            vol_factor = atr / nifty

            sl_offset = (self.W_DELTA * abs(delta) * spot_move +
                         self.W_THETA * abs(theta) * time_factor +
                         self.W_VEGA  * vega * vol_factor)
            sl = min(sl_offset * 6, 25)
            return sl
        except Exception as e:
            logger.error(f"[ERROR] Failed to Calculate SL: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════
    # ─── RAS  (Regime Acceleration Score) ───────────────────────────
    # ═══════════════════════════════════════════════════════════════════

    def compute_ras(self, greeks_history, lookback_bars, norm_window_bars):
        """
        Combined Regime Acceleration Score.

        greeks_history : list of dicts with keys net_delta, net_gamma, net_vega.
        lookback_bars  : number of bars used to measure acceleration.
        norm_window_bars: rolling window for std normalisation.

        Returns a single float RAS score.
        """
        history = greeks_history[-norm_window_bars:]
        if len(history) < lookback_bars + 1:
            return 0.0

        arr_d = np.array([g["net_delta"] for g in history])
        arr_g = np.array([g["net_gamma"] for g in history])
        arr_v = np.array([g["net_vega"]  for g in history])

        # Acceleration = latest value minus value `lookback_bars` ago
        delta_accel = abs(arr_d[-1] - arr_d[-(lookback_bars + 1)])
        gamma_accel = abs(arr_g[-1] - arr_g[-(lookback_bars + 1)])
        vega_accel  = abs(arr_v[-1] - arr_v[-(lookback_bars + 1)])

        # Normalise by rolling std (guard against zero-std)
        std_d = np.std(arr_d) if np.std(arr_d) > 1e-9 else 1e-9
        std_g = np.std(arr_g) if np.std(arr_g) > 1e-9 else 1e-9
        std_v = np.std(arr_v) if np.std(arr_v) > 1e-9 else 1e-9

        ras = (self.RAS_WEIGHT_DELTA * (delta_accel / std_d) +
               self.RAS_WEIGHT_GAMMA * (gamma_accel / std_g) +
               self.RAS_WEIGHT_VEGA  * (vega_accel  / std_v))
        return ras

    def ras_regime_label(self, ras):
        """Human-readable regime from RAS score."""
        if ras < 2.0: return "COMPRESSION"
        if ras < 6.0: return "TRANSITION"
        return "EXPANSION / TAIL_RISK"

    def build_greeks_snapshot(self, spot, strike, tte, ce_price, pe_price):
        """
        Compute net Greeks for one time-step and return a dict suitable for
        the greeks_history list used by compute_ras().
        Falls back to IV = 0.15 if brentq fails.
        """
        iv_ce = self.calculate_iv(ce_price, spot, strike, tte,
                                  self.RISK_FREE_RATE, self.DIVIDEND, "call") or 0.15
        iv_pe = self.calculate_iv(pe_price, spot, strike, tte,
                                  self.RISK_FREE_RATE, self.DIVIDEND, "put") or 0.15

        d_ce, _, v_ce, g_ce = self.black_scholes_greeks(
            spot, strike, tte, iv_ce, self.RISK_FREE_RATE, self.DIVIDEND, "call"
        )
        d_pe, _, v_pe, g_pe = self.black_scholes_greeks(
            spot, strike, tte, iv_pe, self.RISK_FREE_RATE, self.DIVIDEND, "put"
        )

        return {
            "net_delta": d_ce + d_pe,
            "net_gamma": g_ce + g_pe,
            "net_vega":  v_ce + v_pe,
        }

    def build_preentry_greeks_history(self, strike, check_time, expiry_dt, lookback_min=25):
        """
        Fetch last `lookback_min` minutes of 1-min option bars and build a
        list of greeks snapshots for the pre-entry RAS calculation.
        """
        hist_start = check_time - timedelta(minutes=lookback_min)

        try:
            hist_ce = self.get_1_min_option_historical(
                self.expiry, strike, "call", hist_start, check_time
            )
            hist_pe = self.get_1_min_option_historical(
                self.expiry, strike, "put",  hist_start, check_time
            )

            if (not hist_ce or "Success" not in hist_ce or not hist_ce["Success"] or
                    not hist_pe or "Success" not in hist_pe or not hist_pe["Success"]):
                print("[RAS Pre-entry] Option history unavailable — skipping RAS check")
                return []

            df_ce = pd.DataFrame(hist_ce["Success"])
            df_pe = pd.DataFrame(hist_pe["Success"])

            df_ce['datetime'] = pd.to_datetime(df_ce['datetime'])
            df_ce.set_index('datetime', inplace=True)
            df_pe['datetime'] = pd.to_datetime(df_pe['datetime'])
            df_pe.set_index('datetime', inplace=True)

            # Fetch matching spot data
            nifty_raw = self.get_1_min_historical(hist_start, check_time)
            if not nifty_raw or "Success" not in nifty_raw or not nifty_raw["Success"]:
                print("[RAS Pre-entry] Spot data unavailable — skipping RAS check")
                return []

            df_spot = pd.DataFrame(nifty_raw["Success"])
            df_spot['datetime'] = pd.to_datetime(df_spot['datetime'])
            df_spot.set_index('datetime', inplace=True)

            common_index   = df_ce.index.intersection(df_pe.index)
            greeks_history = []

            for ts in common_index:
                try:
                    ce_price = float(df_ce.loc[ts, 'close'])
                    pe_price = float(df_pe.loc[ts, 'close'])

                    spot_ts = df_spot.index.asof(ts)
                    if pd.isnull(spot_ts):
                        continue
                    spot = float(df_spot.loc[spot_ts, 'close'])

                    tte = (expiry_dt - ts).total_seconds() / (365 * 24 * 60 * 60)
                    if tte <= 0:
                        continue

                    snap = self.build_greeks_snapshot(spot, strike, tte, ce_price, pe_price)
                    greeks_history.append(snap)
                except Exception:
                    continue

            print(f"[RAS Pre-entry] Built {len(greeks_history)} 1-min Greek bars")
            return greeks_history

        except Exception as e:
            logger.error(f"[ERROR] build_preentry_greeks_history: {e}")
            return []

    def get_live_spot(self):
        """
        Return the most recent Nifty spot close.
        Result is cached for ~60 s to avoid hammering the API every second.
        """
        now = datetime.now()
        if (self.live_spot is not None and
                self.last_spot_fetch_time is not None and
                (now - self.last_spot_fetch_time).seconds < 60):
            return self.live_spot

        try:
            data = self.get_1_min_historical(now - timedelta(minutes=3), now)
            if data and "Success" in data and data["Success"]:
                df = pd.DataFrame(data["Success"])
                self.live_spot            = float(df.iloc[-1]['close'])
                self.last_spot_fetch_time = now
                return self.live_spot
        except Exception as e:
            logger.error(f"[ERROR] get_live_spot: {e}")

        return self.live_spot   # stale value is better than None

    # ═══════════════════════════════════════════════════════════════════
    # Position state persistence
    # ═══════════════════════════════════════════════════════════════════

    def save_entry_data_to_csv(self, entry_data):
        try:
            with open(self.temp_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(entry_data.keys())
                writer.writerow(entry_data.values())
        except Exception as e:
            print(f"Failed to Save Entry Data", exc_info=e)
            raise

    def load_entry_data_from_csv(self):
        try:
            if not os.path.exists(self.temp_file):
                return None
            with open(self.temp_file, "r") as f:
                reader = csv.DictReader(f)
                rows   = list(reader)
                if not rows:
                    return None
                row = rows[0]
                row["trade_rem"] = int(row["trade_rem"])
                row["time"]      = datetime.strptime(row["time"], "%Y-%m-%d %H:%M:%S.%f")
                row["atm"]       = int(row["atm"])
                row["entry"]     = float(row["entry"])
                row["current"]   = float(row["current"])
                row["cut"]       = int(row["cut"])
                row["Qty_B"]     = int(row["Qty_B"])
                row["max_pnl"]   = float(row["max_pnl"])
                row["sl"]        = float(row["sl"])
                row["tsl"]       = float(row["tsl"])
                row["ce_token"]  = int(row["ce_token"])
                row["pe_token"]  = int(row["pe_token"])
                row["pnl"]       = float(row["pnl"])
                # entry_ras may not exist in older CSVs — default to 0
                row["entry_ras"] = float(row.get("entry_ras", 0.0))
                return row
        except Exception as e:
            print(f"Failed to Load Entry Data", exc_info=e)
            raise

    def clear_position_state(self):
        try:
            if os.path.exists(self.temp_file):
                os.remove(self.temp_file)
        except Exception as e:
            print(f"Failed to Delete Entry Data", exc_info=e)
            raise

    # ═══════════════════════════════════════════════════════════════════
    # Utility helpers
    # ═══════════════════════════════════════════════════════════════════

    def round_to_nearest_50(self, x):
        return int(round(x / 50.0)) * 50

    def write_to_csv(self, data):
        """
        Extended header now includes Entry_RAS, Exit_RAS, RAS_KillSwitch.
        Callers must pass 16 values (was 13).
        """
        headers = [
            'Date', 'Strike', 'Entry Time', 'Entry premium', 'SL',
            'Exit Time', 'Exit premium', 'Exit Type', 'Max PnL',
            'Partial Booking', 'PnL', 'Quantity', 'Total',
            'Entry_RAS', 'Exit_RAS', 'RAS_KillSwitch'          # ← NEW
        ]
        try:
            try:
                with open(self.csv_file, 'x', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(headers)
                    print("Created new CSV file")
            except FileExistsError:
                pass
            with open(self.csv_file, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(data)
                print(f"Trade data written to CSV: {data}")
        except Exception as e:
            print(f"Error writing to CSV: {str(e)}")

    def get_instrument_id(self, exchange, symbol, expiry=None, strike=None, opt=None):
        try:
            data   = requests.get(self.URL[exchange]).json()
            symbol = symbol.upper()

            if exchange == "NSEEQ":
                return next((i["instrumentId"] for i in data
                             if i["underlyingInstrumentSymbol"] == symbol), None)
            if exchange == "INDICES":
                return next((i["instrumentId"] for i in data
                             if symbol in (i["underlyingInstrumentSymbol"],
                                           i["tradingSymbol"].replace(" INDEX", ""))), None)
            if exchange == "NSEFO":
                exp = datetime.strptime(expiry, "%Y-%m-%d").strftime("%d-%b-%Y")
                return next((i["instrumentId"] for i in data
                             if i["underlyingInstrumentSymbol"] == symbol
                             and exp in i["expiry"]
                             and (opt is None or (i["optionType"] == opt and
                                                  float(i["strikePrice"]) == float(strike)))), None)
        except Exception as e:
            logger.error(f"[ERROR] Failed to Fetch Instrument ID: {e}")
            return None

    def fetch_market_feed_scrip(self, id):
        try:
            url    = f"{self.BASE_URL}/marketdata/marketquotes"
            params = [{"instrumentId": id, "exchange": "NSEFO"}]
            response = requests.post(url, headers=self.headers, json=params)
            data     = response.json()
            if data["status"] == "Ok":
                return data["result"][0]["ltp"]
            print("Error:", data)
            return None
        except Exception as e:
            logger.error(f"[ERROR] Fetching Live data: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════
    # Historical data fetchers
    # ═══════════════════════════════════════════════════════════════════

    def get_historical_candles(self, exchange, instrument_id, interval, from_date, to_date):
        try:
            url     = f"{self.BASE_URL}/marketdata/historicaldata"
            payload = {
                "exchange":     exchange,
                "instrumentId": instrument_id,
                "interval":     interval,
                "fromDate":     from_date,
                "toDate":       to_date,
            }
            r = requests.post(url, json=payload, headers=self.headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data["status"] != "Ok":
                raise Exception(data)
            return data["result"][0]["candles"]
        except Exception as e:
            logger.error(f"[ERROR] Failed to Fetch Daily Historical Data: {e}")
            return None

    def get_1_min_historical(self, from_date, to_date, exchange="NSE", interval="1minute"):
        try:
            return self.breeze.get_historical_data_v2(
                interval=interval,
                from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                stock_code="NIFTY",
                exchange_code=exchange,
                product_type="cash"
            )
        except Exception as e:
            logger.error(f"[ERROR] Failed to Fetch Historical Data: {e}")
            return None

    def get_1_min_option_historical(self, expiry, strike, right, from_date, to_date):
        try:
            return self.breeze.get_historical_data_v2(
                interval="1minute",
                from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                stock_code="NIFTY",
                exchange_code="NFO",
                product_type="options",
                expiry_date=expiry,
                right=right,
                strike_price=strike
            )
        except Exception as e:
            logger.error(f"[ERROR] Failed to Fetch Historical Option Data: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════
    # Market condition checks
    # ═══════════════════════════════════════════════════════════════════

    def check_GAP(self):
        try:
            now   = datetime.now()
            s     = (now - timedelta(days=10)).date()
            e     = now.date()
            start = s.strftime('%d-%b-%Y')
            end   = e.strftime('%d-%b-%Y')
            hist  = self.get_historical_candles("NSEEQ", 999920000, "1 day", start, end)
            df    = pd.DataFrame(hist, columns=["Datetime","Open","High","Low","Close","Volume"])
            df["datetime"] = pd.to_datetime(df["Datetime"])
            df.set_index("datetime", inplace=True)
            if len(df) < 3:
                return None
            today    = df.iloc[-1]['Open']
            previous = df.iloc[-2]['Close']
            gap      = ((today - previous) / previous) * 100
            GAP_day  = abs(gap) <= 1.25
            print(f"[DEBUG] GAP: {gap:.5f} | Straddle Day => {GAP_day}")
            return GAP_day
        except Exception as e:
            logger.error(f"[ERROR] GAP checking: {e}")
            return None

    def check_atr_adx(self):
        try:
            date       = datetime.now()
            x          = t(14, 5)
            nifty_data = self.get_1_min_historical(
                (date - timedelta(days=4)).replace(hour=9, minute=15), date
            )
            df = pd.DataFrame(nifty_data["Success"])
            if df.empty:
                raise ValueError("NIFTY data not available")
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            df["ADX"]     = ta.adx(df["high"], df["low"], df["close"])["ADX_14"]
            df["ATR"]     = ta.atr(df["high"], df["low"], df["close"])
            df["log_ret"] = np.log(df["close"] / df["close"].shift(1))
            df["RV"]      = df["log_ret"].rolling(window=10).std() * np.sqrt(252 * 390)
            df.dropna(inplace=True)

            if len(df) < 15:
                return None
            last   = df.iloc[-5]
            prev10 = df.iloc[-15]
            atr_down = prev10["ATR"] > last["ATR"]
            adx_down = prev10["ADX"] > last["ADX"]
            rv_down  = prev10["RV"]  > last["RV"]
            print(f"[DEBUG] ATR: {prev10['ATR']:.2f} > {last['ATR']:.2f} => {atr_down}")
            print(f"[DEBUG] ADX: {prev10['ADX']:.2f} > {last['ADX']:.2f} => {adx_down}")
            print(f"[DEBUG] RV:  {prev10['RV']:.2f}  > {last['RV']:.2f}  => {rv_down}")
            if (date.time() < x) and rv_down and atr_down and adx_down:
                print("Entry conditions matched")
                return last["close"]
            return None
        except Exception as e:
            logger.error(f"[ERROR] ATR/ADX: {e}")
            return None

    def check_nearest_ltp(self, atm, Cc, Cu, Cd, Pc, Pu, Pd):
        try:
            cc = self.fetch_market_feed_scrip(Cc)
            cu = self.fetch_market_feed_scrip(Cu)
            cd = self.fetch_market_feed_scrip(Cd)
            pc = self.fetch_market_feed_scrip(Pc)
            pu = self.fetch_market_feed_scrip(Pu)
            pd = self.fetch_market_feed_scrip(Pd)

            m1 = abs(cc - pc)
            m2 = abs(cd - pd)
            m3 = abs(cu - pu)
            m  = min(m1, m2, m3)
            print(f" ATM: {m1:.2f} | Down: {m2:.2f} | Up: {m3:.2f} | Best: {m:.2f}")

            if m == m1 and m <= 0.2 * max(cc, pc):
                return Cc, Pc, atm
            elif m == m2 and m <= 0.2 * max(cd, pd):
                return Cd, Pd, atm - 50
            elif m == m3 and m <= 0.2 * max(cu, pu):
                return Cu, Pu, atm + 50
            print("Failed premium matching")
            return None, None, None
        except Exception as e:
            logger.error(f"[ERROR] Premium matching: {e}")
            return None, None, None

    # ═══════════════════════════════════════════════════════════════════
    # Main strategy loop
    # ═══════════════════════════════════════════════════════════════════

    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(db_session, deployed_strategy_id)

            try:
                print("[START] Waiting for signal...")
                GAP = self.check_GAP()

                # ── Resume open position from disk ──────────────────────
                self.entry_data = self.load_entry_data_from_csv()
                if self.entry_data:
                    self.TOKENS["CE"]  = self.entry_data["ce_token"]
                    self.TOKENS["PE"]  = self.entry_data["pe_token"]
                    self.max_trades    = self.entry_data["trade_rem"]
                    self.entry_ras     = self.entry_data.get("entry_ras", 0.0)
                    atm_strike         = self.entry_data["atm"]
                    # live_greeks_history starts empty on resume;
                    # it will re-populate within the first ~RAS_1MIN_NORM ticks.
                    self.live_greeks_history = []
                    print(f"[RESUME] Open position CE:{self.TOKENS['CE']} PE:{self.TOKENS['PE']}")
                    self.initiate_ws(atm_strike)
                    logger.info("[RESUME] Loaded open position from CSV")

                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()

                        # ══════════════════════════════════════════════
                        # ENTRY BLOCK
                        # ══════════════════════════════════════════════
                        if (self.TIME_1 < now.time() < self.TIME_2 and GAP and self.max_trades > 0 and self.entry_data is None and now.second == 0):
                        # if (self.TIME_1 < now.time() < self.TIME_2 and self.max_trades > 0 and self.entry_data is None and now.second == 0):

                            # Cool-down after SL exit
                            if self.sl_exit_time and (now - self.sl_exit_time).seconds < 300:
                                time.sleep(1)
                                continue

                            print(f"Checking ATR/ADX at {now.time()}")
                            # spot = self.check_atr_adx()
                            spot=23150

                            if spot is not None:
                                atm = self.round_to_nearest_50(spot)
                                print(f"Spot: {spot:.2f} | ATM: {atm}")

                                # Fetch instrument IDs for ATM ± 1 strike
                                Cc = self.get_instrument_id("NSEFO", "NIFTY", self.EXPIRY, atm,                  "CE")
                                time.sleep(0.2)
                                Cu = self.get_instrument_id("NSEFO", "NIFTY", self.EXPIRY, atm + self.ROUND_OFF, "CE")
                                time.sleep(0.2)
                                Cd = self.get_instrument_id("NSEFO", "NIFTY", self.EXPIRY, atm - self.ROUND_OFF, "CE")
                                time.sleep(0.2)
                                Pc = self.get_instrument_id("NSEFO", "NIFTY", self.EXPIRY, atm,                  "PE")
                                time.sleep(0.2)
                                Pu = self.get_instrument_id("NSEFO", "NIFTY", self.EXPIRY, atm + self.ROUND_OFF, "PE")
                                time.sleep(0.2)
                                Pd = self.get_instrument_id("NSEFO", "NIFTY", self.EXPIRY, atm - self.ROUND_OFF, "PE")

                                C, P, atm = self.check_nearest_ltp(atm, Cc, Cu, Cd, Pc, Pu, Pd)
                                self.TOKENS["CE"] = C
                                self.TOKENS["PE"] = P

                                if not self.TOKENS["CE"] or not self.TOKENS["PE"]:
                                    print("[ABORT] Invalid instrument ID. Skipping.")
                                    time.sleep(1)
                                    continue

                               
                                expiry_dt = self.expiry + timedelta(hours=15, minutes=30)
                                preentry_history = self.build_preentry_greeks_history(
                                    atm, now, expiry_dt, lookback_min=25
                                )
                                self.entry_ras = self.compute_ras(
                                    preentry_history,
                                    lookback_bars=self.RAS_1MIN_LOOKBACK,
                                    norm_window_bars=self.RAS_1MIN_NORM
                                )
                                print(f"[RAS Pre-entry] RAS = {self.entry_ras:.4f} "
                                      f"| Regime = {self.ras_regime_label(self.entry_ras)}")

                                if self.entry_ras > self.RAS_ENTRY_LIMIT:
                                    print(f"[BLOCKED] Pre-entry RAS {self.entry_ras:.4f} > "
                                          f"{self.RAS_ENTRY_LIMIT} — skipping entry")
                                    logger.info(f"[RAS BLOCKED] RAS={self.entry_ras:.4f} "
                                                f"Regime={self.ras_regime_label(self.entry_ras)}")
                                    time.sleep(1)
                                    continue
                                # ─────────────────────────────────────

                                self.initiate_ws(atm)
                                time.sleep(3)

                                if self.live_ltp["CE"] and self.live_ltp["PE"]:
                                    ce_entry      = self.live_ltp["CE"]
                                    pe_entry      = self.live_ltp["PE"]
                                    entry_premium = ce_entry + pe_entry

                                    if entry_premium > 50:
                                        entry_time = now
                                        exit_time  = datetime.combine(now.date(), self.TIME_2)
                                        tte = (expiry_dt - now).total_seconds() / (365 * 24 * 60 * 60)

                                        # ATR / IV for Greek SL
                                        start_hist = now - timedelta(minutes=30)
                                        hist_ce    = self.get_1_min_option_historical(
                                            self.expiry, atm, "call", start_hist, now
                                        )
                                        df_ce = pd.DataFrame(hist_ce["Success"])
                                        if df_ce.empty:
                                            raise ValueError("CE data not available")
                                        df_ce['datetime'] = pd.to_datetime(df_ce['datetime'])
                                        df_ce.set_index('datetime', inplace=True)
                                        df_ce['ATR'] = ta.atr(df_ce['high'], df_ce['low'],
                                                               df_ce['close'], length=14)
                                        atr_ce = df_ce.iloc[-1]['ATR']
                                        vol_ce = self.calculate_iv(
                                            ce_entry, spot, atm, tte,
                                            self.RISK_FREE_RATE, self.DIVIDEND, 'call'
                                        )

                                        hist_pe = self.get_1_min_option_historical(
                                            self.expiry, atm, "put", start_hist, now
                                        )
                                        df_pe = pd.DataFrame(hist_pe["Success"])
                                        if df_pe.empty:
                                            raise ValueError("PE data not available")
                                        df_pe['datetime'] = pd.to_datetime(df_pe['datetime'])
                                        df_pe.set_index('datetime', inplace=True)
                                        df_pe['ATR'] = ta.atr(df_pe['high'], df_pe['low'],
                                                               df_pe['close'], length=14)
                                        atr_pe = df_pe.iloc[-1]['ATR']
                                        vol_pe = self.calculate_iv(
                                            pe_entry, spot, atm, tte,
                                            self.RISK_FREE_RATE, self.DIVIDEND, 'put'
                                        )

                                        sl_ce = self.calculate_greeks_sl(
                                            entry_time, exit_time, spot, atm, tte,
                                            vol_ce or 0.15, atr_ce, "call"
                                        )
                                        sl_pe = self.calculate_greeks_sl(
                                            entry_time, exit_time, spot, atm, tte,
                                            vol_pe or 0.15, atr_pe, "put"
                                        )
                                        STOP_LOSS = -((sl_ce + sl_pe) / 2)
                                        print(f"Stop Loss: {STOP_LOSS:.4f}")

                                        # Place orders
                                        # entry_premium, ce_entry, pe_entry = self.place_sell_order(self.TOKENS["CE"], self.TOKENS["PE"], self.QTY )  # Selling Straddle
                                        time.sleep(1)

                                        # Seed live greeks history from preentry bars
                                        # (pad each 1-min bar into 60 identical 1-sec slots)
                                        self.live_greeks_history = []
                                        for snap in preentry_history:
                                            for _ in range(60):
                                                self.live_greeks_history.append(snap)
                                        # Trim to norm window
                                        max_len = self.RAS_1MIN_NORM * 60
                                        self.live_greeks_history = self.live_greeks_history[-max_len:]

                                        self.live_ras              = self.entry_ras
                                        self.ras_kill_switch_fired = False
                                        self.live_spot             = spot

                                        self.max_trades -= 1
                                        self.entry_data = {
                                            "trade_rem": self.max_trades,
                                            "time":      entry_time,
                                            "atm":       atm,
                                            "entry":     entry_premium,
                                            "current":   0,
                                            "cut":       0,
                                            "Qty_B":     0,
                                            "max_pnl":   0,
                                            "sl":        STOP_LOSS,
                                            "tsl":       STOP_LOSS,
                                            "ce_token":  self.TOKENS["CE"],
                                            "pe_token":  self.TOKENS["PE"],
                                            "pnl":       0,
                                            "entry_ras": round(self.entry_ras, 4),
                                        }
                                        self.save_entry_data_to_csv(self.entry_data)
                                        self.sl_exit_time = None

                                        logger.info(
                                            f"[ENTRY] Time:{now.time()} | ATM:{atm} | "
                                            f"Premium:{entry_premium:.2f} | Qty:{self.QTY} | "
                                            f"SL:{STOP_LOSS:.4f} | RAS:{self.entry_ras:.4f}"
                                        )
                                        print(f"[ENTRY] ATM:{atm} Prem:{entry_premium:.2f} "
                                              f"SL:{STOP_LOSS:.4f} | Entry_RAS:{self.entry_ras:.4f}")
                                    else:
                                        print("Premium too low — skipping")

                        # ══════════════════════════════════════════════
                        # TRAIL SL / LIVE RAS CHECK  (in-trade)
                        # ══════════════════════════════════════════════
                        elif self.entry_data and self.TIME_1 < now.time() < self.TIME_2:
                            if self.live_ltp["CE"] and self.live_ltp["PE"]:
                                current_premium = self.live_ltp["CE"] + self.live_ltp["PE"]
                                pnl             = self.entry_data["entry"] - current_premium
                                Exit_Type       = None

                                self.entry_data["current"] = current_premium

                                # Update trailing SL
                                if pnl > self.entry_data["max_pnl"]:
                                    self.entry_data["max_pnl"] = pnl
                                    self.entry_data["tsl"]     = pnl + self.entry_data["sl"]

                                # ── Build 1-sec greeks snapshot for RAS ──────
                                spot = self.get_live_spot()
                                if spot is not None:
                                    atm_strike = self.entry_data["atm"]
                                    expiry_dt  = self.expiry + timedelta(hours=15, minutes=30)
                                    tte_live   = (expiry_dt - now).total_seconds() / (365 * 24 * 60 * 60)

                                    if tte_live > 0:
                                        live_snap = self.build_greeks_snapshot(
                                            spot, atm_strike, tte_live,
                                            self.live_ltp["CE"], self.live_ltp["PE"]
                                        )
                                        self.live_greeks_history.append(live_snap)
                                        # Bound the history to norm window
                                        max_len = self.RAS_1MIN_NORM * 60
                                        if len(self.live_greeks_history) > max_len:
                                            self.live_greeks_history.pop(0)

                                        # Recompute RAS every tick
                                        self.live_ras = self.compute_ras(
                                            self.live_greeks_history,
                                            lookback_bars=self.RAS_1MIN_LOOKBACK,
                                            norm_window_bars=self.RAS_1MIN_NORM
                                        )
                                        print(f"[RAS Live] {self.live_ras:.4f} "
                                              f"| {self.ras_regime_label(self.live_ras)}")

                                self.save_entry_data_to_csv(self.entry_data)

                                # ── Partial profit booking ────────────────────
                                # if ((pnl > 15 and self.entry_data["cut"] == 0) or
                                #         (pnl > 30 and self.entry_data["cut"] == 1)):
                                if (pnl > 15 and self.entry_data["cut"] == 0):
                                    book_prem=current_premium
                                    # book_prem = self.square_off_straddle(self.TOKENS["CE"], self.TOKENS["PE"], self.PROFIT_CUT_QTY)   # Partial Booking
                                    partial_pnl = (self.entry_data["entry"] - book_prem) * self.PROFIT_CUT_QTY
                                    self.entry_data["pnl"]   += partial_pnl
                                    self.entry_data["Qty_B"] += self.PROFIT_CUT_QTY
                                    self.entry_data["cut"]   += 1
                                    self.save_entry_data_to_csv(self.entry_data)
                                    logger.info(
                                        f"[PARTIAL] Time:{now.strftime('%H:%M:%S')} | "
                                        f"ExitPrem:{book_prem:.2f} | PnL:₹{partial_pnl:.2f}"
                                    )

                                # ── Layer 2: RAS Kill Switch ──────────────────
                                if (self.live_ras > self.RAS_KILL_SWITCH and not self.ras_kill_switch_fired):
                                    Q = self.QTY - self.entry_data["Qty_B"]
                                    exit_prem=current_premium
                                    # exit_prem   = self.square_off_straddle(self.TOKENS["CE"], self.TOKENS["PE"], Q)   # Exit Position
                                    final_pnl   = (((self.entry_data["entry"] - exit_prem) * Q) +
                                                   self.entry_data["pnl"]) / self.QTY
                                    Total       = final_pnl * self.QTY
                                    Exit_Type   = "RAS_KILL"
                                    self.ras_kill_switch_fired = True

                                    self.deactivate_ws(self.entry_data["atm"])
                                    Ti = now.strftime("%H:%M:%S")
                                    logger.info(
                                        f"[RAS KILL] Time:{Ti} | RAS:{self.live_ras:.4f} | "
                                        f"ExitPrem:{exit_prem:.2f} | PnL:{final_pnl:.2f} | "
                                        f"Total:₹{Total:.2f}"
                                    )
                                    print(f"[RAS KILL] RAS={self.live_ras:.4f} "
                                          f"({self.ras_regime_label(self.live_ras)}) "
                                          f"@ {Ti} | PnL=₹{Total:.2f}")

                                    e = self.entry_data["entry"]
                                    self.write_to_csv([
                                        self.entry_data["time"].strftime("%Y-%m-%d"),
                                        self.entry_data["atm"],
                                        self.entry_data["time"].strftime("%H:%M:%S"),
                                        f"{e:.2f}",
                                        self.entry_data["sl"],
                                        Ti,
                                        f"{exit_prem:.2f}",
                                        Exit_Type,
                                        self.entry_data["max_pnl"],
                                        self.entry_data["cut"],
                                        f"{final_pnl:.2f}",
                                        self.QTY,
                                        f"{Total:.2f}",
                                        f"{self.entry_ras:.4f}",          # Entry_RAS
                                        f"{self.live_ras:.4f}",           # Exit_RAS
                                        "YES",                            # RAS_KillSwitch
                                    ])
                                    self.entry_data          = None
                                    self.live_greeks_history = []
                                    self.clear_position_state()
                                    self.TOKENS.clear()
                                    self.sl_exit_time = now
                                    time.sleep(1)
                                    continue

                                # ── Trailing SL exit ──────────────────────────
                                if pnl <= self.entry_data["tsl"]:
                                    Q         = self.QTY - self.entry_data["Qty_B"]
                                    exit_prem=current_premium
                                    # exit_prem = self.square_off_straddle(self.TOKENS["CE"], self.TOKENS["PE"], Q)   # Exit Position
                                    final_pnl = (((self.entry_data["entry"] - exit_prem) * Q) +
                                                 self.entry_data["pnl"]) / self.QTY
                                    Total     = final_pnl * self.QTY
                                    Exit_Type = "TSL"

                                    self.deactivate_ws(self.entry_data["atm"])
                                    Ti = now.strftime("%H:%M:%S")
                                    logger.info(
                                        f"[SL HIT] Time:{Ti} | ExitPrem:{exit_prem:.2f} | "
                                        f"PnL:{final_pnl:.2f} | Total:₹{Total:.2f} | "
                                        f"RAS:{self.live_ras:.4f}"
                                    )
                                    print(f"[SL HIT] PnL:₹{final_pnl:.2f} | "
                                          f"RAS={self.live_ras:.4f}")

                                    e = self.entry_data["entry"]
                                    self.write_to_csv([
                                        self.entry_data["time"].strftime("%Y-%m-%d"),
                                        self.entry_data["atm"],
                                        self.entry_data["time"].strftime("%H:%M:%S"),
                                        f"{e:.2f}",
                                        self.entry_data["sl"],
                                        Ti,
                                        f"{exit_prem:.2f}",
                                        Exit_Type,
                                        self.entry_data["max_pnl"],
                                        self.entry_data["cut"],
                                        f"{final_pnl:.2f}",
                                        self.QTY,
                                        f"{Total:.2f}",
                                        f"{self.entry_ras:.4f}",          
                                        f"{self.live_ras:.4f}",           
                                        "NO",                            
                                    ])
                                    self.entry_data          = None
                                    self.live_greeks_history = []
                                    self.clear_position_state()
                                    self.TOKENS.clear()
                                    self.sl_exit_time = now

                        # ══════════════════════════════════════════════
                        # TIME-BASED EXIT
                        # ══════════════════════════════════════════════
                        elif self.entry_data and now.time() >= self.TIME_2 and now.second == 0:
                            if self.live_ltp["CE"] and self.live_ltp["PE"]:
                                Q         = self.QTY - self.entry_data["Qty_B"]
                                exit_prem=self.live_ltp["CE"] + self.live_ltp["PE"]
                                # exit_prem = self.square_off_straddle(self.TOKENS["CE"], self.TOKENS["PE"], Q)  # Exit Position
                                final_pnl = (((self.entry_data["entry"] - exit_prem) * Q) +
                                             self.entry_data["pnl"]) / self.QTY
                                Total     = final_pnl * self.QTY
                                Exit_Type = "EOD"

                                self.deactivate_ws(self.entry_data["atm"])
                                Ti = now.strftime("%H:%M:%S")
                                logger.info(
                                    f"[TIME EXIT] Time:{Ti} | ExitPrem:{exit_prem:.2f} | "
                                    f"PnL:{final_pnl:.2f} | Total:₹{Total:.2f} | "
                                    f"RAS:{self.live_ras:.4f}"
                                )
                                e = self.entry_data["entry"]
                                self.write_to_csv([
                                    self.entry_data["time"].strftime("%Y-%m-%d"),
                                    self.entry_data["atm"],
                                    self.entry_data["time"].strftime("%H:%M:%S"),
                                    f"{e:.2f}",
                                    self.entry_data["sl"],
                                    Ti,
                                    f"{exit_prem:.2f}",
                                    Exit_Type,
                                    self.entry_data["max_pnl"],
                                    self.entry_data["cut"],
                                    f"{final_pnl:.2f}",
                                    self.QTY,
                                    f"{Total:.2f}",
                                    f"{self.entry_ras:.4f}",              
                                    f"{self.live_ras:.4f}",               
                                    "NO",                                 
                                ])
                                self.entry_data          = None
                                self.live_greeks_history = []
                                self.clear_position_state()
                                self.TOKENS.clear()
                                self.sl_exit_time = None

                            print("[INFO] Market close time reached. Exiting strategy.")
                            break

                        time.sleep(1)

                    except Exception as e:
                        logger.error(f"Main loop error:", exc_info=e)
                        print(f"Main loop error: {str(e)}")
                        time.sleep(60)

            except Exception as e:
                logging.error(f"Error:", exc_info=e)
                deployed_strategy.status = "STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e