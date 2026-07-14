from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from zerodha import Zerodha
import csv, time, math, os
import pandas as pd
from datetime import datetime, timedelta
from scipy.stats import norm
from scipy.optimize import brentq
from math import log, sqrt, exp

import logging
from ..common.logging_config import create_logger

import warnings
warnings.filterwarnings("ignore")

logger = create_logger(__name__, 'Vega_Live.log')

class StrategyVegaLive(BaseStrategy):
    def __init__(self, params):
        super().__init__()

        try:
            self.kite = Zerodha(user_id=params["api_key"], password=params["api_secret"], twofa=params["session_token"])
            self.kite.login()
            logger.info("Zerodha initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Zerodha: ", exc_info = e)
            raise e
        
        start_time = str(params["start_hour"]) + ":" + str(params["start_minute"])
        end_time = str(params["end_hour"]) + ":" + str(params["end_minute"])

        self.TIME_1 = datetime.strptime(start_time, "%H:%M").time()
        self.TIME_2 = datetime.strptime(end_time, "%H:%M").time()
        
        self.expiry = params["expiry"]
        expiry_date = datetime.strptime(self.expiry, "%Y-%m-%d")
        self.expiry_dt = expiry_date.replace(hour=15, minute=30, second=0)

        self.csv_file = params["csv_file"]
        
        # Strategy Constants
        self.risk_free_rate = 0.068
        self.dividend_yield = 0.012
        self.STRIKE_STEP = 50
        self.N_WINGS = 9
        self.YEAR_DAYS = 365.0
        
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.token_cache = {}
        self.initial_cache = {}
        self.instruments_df = None


    # --- Helper Functions (Moved inside class) ---
    def pdf(self, x):
        return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

    def cdf(self, x):
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

    def d1(self, up, sp, t_, r, v, d):
        return (math.log(up / sp) + (r - d + 0.5 * v ** 2) * t_) / (v * math.sqrt(t_))

    def d2(self, up, sp, t_, r, v, d):
        return self.d1(up, sp, t_, r, v, d) - v * math.sqrt(t_)

    def option_vega(self, up, sp, t_, r, v, d):
        # per 1% (0.01) change in vol
        return 0.01 * up * math.exp(-d * t_) * math.sqrt(t_) * self.pdf(self.d1(up, sp, t_, r, v, d))

    def option_vomma(self, up, sp, t_, r, v, d):
        d1v = self.d1(up, sp, t_, r, v, d)
        d2v = self.d2(up, sp, t_, r, v, d)
        return self.option_vega(up, sp, t_, r, v, d) * d1v * d2v / v

    def black_scholes_price(self, S, K, T, r, q, sigma, option_type):
        _d1 = (log(S / K) + (r - q + sigma ** 2 / 2) * T) / (sigma * sqrt(T))
        _d2 = _d1 - sigma * sqrt(T)
        if option_type == 'call':
            return S * exp(-q * T) * norm.cdf(_d1) - K * exp(-r * T) * norm.cdf(_d2)
        elif option_type == 'put':
            return K * exp(-r * T) * norm.cdf(-_d2) - S * exp(-q * T) * norm.cdf(-_d1)
        raise ValueError("option_type must be 'call' or 'put'")

    def calculate_iv(self, option_price, S, K, T, r, q, right):
        if option_price is None or option_price <= 0 or T <= 0:
            return None
        f = lambda sig: self.black_scholes_price(S, K, T, r, q, sig, right) - option_price
        a, b = 0.005, 5.0           
        try:
            if f(a) * f(b) > 0:      
                return None
            return brentq(f, a, b)
        except (ValueError, ZeroDivisionError):
            return None

    def fetch_token(self, strike, right):
        key = (strike, right)
        if key in self.token_cache:
            return self.token_cache[key]
        
        df = self.instruments_df[(self.instruments_df.exchange == 'NFO') &
                                 (self.instruments_df.name == 'NIFTY') &
                                 (self.instruments_df.instrument_type == right) &
                                 (self.instruments_df.strike == strike) &
                                 (self.instruments_df.expiry.astype(str) == str(self.expiry))]
                                 
        tok = int(df.iloc[0]['instrument_token']) if not df.empty else None
        self.token_cache[key] = tok
        return tok

    def get_day_candle(self, token, upto):
        rows = self.kite.historical_data(token, self.today, self.today, "day")
        return rows[-1] if rows else None

    def get_initial_greeks(self, token, strike, right, opt_open, S0, T0):
        if token in self.initial_cache:
            return self.initial_cache[token]
        side = 'call' if right == 'CE' else 'put'
        iv0 = self.calculate_iv(opt_open, S0, strike, T0, self.risk_free_rate, self.dividend_yield, side)
        if iv0 is None:
            self.initial_cache[token] = None
            return None
        g = (self.option_vega(S0, strike, T0, self.risk_free_rate, iv0, self.dividend_yield),
             self.option_vomma(S0, strike, T0, self.risk_free_rate, iv0, self.dividend_yield))
        self.initial_cache[token] = g
        return g

    
    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                # 1. Fetch Instruments (One-time setup per run)
                instruments_raw = self.kite.instruments()
                self.instruments_df = pd.DataFrame(instruments_raw)
                
                _idx = self.instruments_df[(self.instruments_df.exchange == 'NSE') &
                                           (self.instruments_df.name == 'NIFTY 50') &
                                           (self.instruments_df.segment == 'INDICES')]
                underlying_inst_id = int(_idx.iloc[0]['instrument_token'])

                _nifty_day = self.kite.historical_data(underlying_inst_id, self.today, self.today, "day")
                S0 = _nifty_day[-1]['open'] if _nifty_day else 0

                _session_open_dt = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
                T0 = (self.expiry_dt - _session_open_dt) / timedelta(days=self.YEAR_DAYS)

                # Initialize CSV Headers if file doesn't exist
                first_write = not os.path.exists(self.csv_file)
                if first_write:
                    with open(self.csv_file, 'w', newline='') as fh:
                        csv.writer(fh).writerow(['Timestamp', 'CE_Vega', 'PE_Vega', 'CE_Vomma', 'PE_Vomma'])

                last_minute = None

                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()

                        if now.time() >= self.TIME_2:
                            print("End of the day ")
                            break

                        in_session = self.TIME_1 <= now.time() < self.TIME_2
                        new_minute = (now.minute != last_minute)

                        if in_session and new_minute:
                            last_minute = now.minute
                            
                            T_live = max((self.expiry_dt - now) / timedelta(days=self.YEAR_DAYS), 1e-6)

                            # Fetch Spot Underlying
                            u = self.get_day_candle(underlying_inst_id, now)
                            if u is None:
                                continue
                            
                            S_live = u['close']
                            atm = round(S_live / self.STRIKE_STEP) * self.STRIKE_STEP

                            ce_strikes = [atm + self.STRIKE_STEP * i for i in range(1, self.N_WINGS + 1)]
                            pe_strikes = [atm - self.STRIKE_STEP * i for i in range(1, self.N_WINGS + 1)]

                            vega_sum = vega_sum_pe = vomma_sum = vomma_sum_pe = 0.0

                            # Calculate for CE WING
                            for strike in ce_strikes:
                                tok = self.fetch_token(strike, 'CE')
                                if tok is None: continue
                                c = self.get_day_candle(tok, now)
                                if c is None: continue
                                
                                premium, opt_open = c['close'], c['open']
                                if premium <= 0 or opt_open <= 0: continue

                                init = self.get_initial_greeks(tok, strike, 'CE', opt_open, S0, T0)
                                if init is None: continue

                                iv = self.calculate_iv(premium, S_live, strike, T_live, self.risk_free_rate, self.dividend_yield, 'call')
                                if iv is None: continue

                                vega_live  = self.option_vega(S_live, strike, T_live, self.risk_free_rate, iv, self.dividend_yield)
                                vomma_live = self.option_vomma(S_live, strike, T_live, self.risk_free_rate, iv, self.dividend_yield)
                                vega_sum  += vega_live  - init[0]
                                vomma_sum += vomma_live - init[1]

                            # Calculate for PE WING
                            for strike in pe_strikes:
                                tok = self.fetch_token(strike, 'PE')
                                if tok is None: continue
                                c = self.get_day_candle(tok, now)
                                if c is None: continue
                                
                                premium, opt_open = c['close'], c['open']
                                if premium <= 0 or opt_open <= 0: continue

                                init = self.get_initial_greeks(tok, strike, 'PE', opt_open, S0, T0)
                                if init is None: continue

                                iv = self.calculate_iv(premium, S_live, strike, T_live, self.risk_free_rate, self.dividend_yield, 'put')
                                if iv is None: continue

                                vega_live  = self.option_vega(S_live, strike, T_live, self.risk_free_rate, iv, self.dividend_yield)
                                vomma_live = self.option_vomma(S_live, strike, T_live, self.risk_free_rate, iv, self.dividend_yield)
                                vega_sum_pe  += vega_live  - init[0]
                                vomma_sum_pe += vomma_live - init[1]

                            # Logging and appending to CSV
                            ts = now.strftime('%Y-%m-%d %H:%M:%S')
                            with open(self.csv_file, 'a', newline='') as fh:
                                csv.writer(fh).writerow([ts, vega_sum, vega_sum_pe, vomma_sum, vomma_sum_pe])

                            print(f"Appended Vega Data at {ts} - CE: {vega_sum:.4f}, PE: {vega_sum_pe:.4f}")
                            logging.info(f"Appended Vega Data at {ts} - CE: {vega_sum:.4f}, PE: {vega_sum_pe:.4f}")

                        else:
                            time.sleep(0.5)

                    except Exception as e:
                        logger.error(f"Fatal error in main loop:", exc_info = e)
                        print(f"Main loop error: {str(e)}")
                        time.sleep(1) # Sleep to prevent tight infinite loop on error
                    
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e