from sqlmodel import Session
from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine
from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
from datetime import date, datetime, timedelta, time as t
import time as time_
import csv
import logging
import traceback
from tenacity import retry, stop_after_attempt, wait_exponential
import os
import warnings
warnings.filterwarnings("ignore")
from ..common.logging_config import create_logger
logger = create_logger(__name__, 'NayanOptionBuying.log')


class StrategyNayanOptionBuying(BaseStrategy):
    def __init__(self, params):
        super().__init__()
        
        self.POSITION_CSV = "open_position_buying.csv"
        self.TRADES_CSV = params["csv_file"]
        try:
            self.breeze = BreezeConnect(api_key=params["api_key"])
            self.breeze.generate_session(
                api_secret=params["api_secret"],
                session_token=params["session_token"]
            )
            logger.info("BreezeConnect initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize BreezeConnect: ", exc_info = e)
            raise e

        start_time = str(params["start_hour"]) + ":" + str(params["start_minute"])
        end_time = str(params["end_hour"]) + ":" + str(params["end_minute"])    
        self.TIME_1 = datetime.strptime(start_time, "%H:%M").time()
        self.TIME_2 = datetime.strptime(end_time, "%H:%M").time()
        self.MAX_ENTRY_TIME = datetime.strptime("15:00", "%H:%M").time()
        self.OPTION_EXPIRY = params["expiry"]
        self.FUT_EXPIRY = params["fut_expiry"]
        self.QTY = params["qty"]
        self.api_secret=params["api_secret"]
        self.session_token=params["session_token"]
        self.atm_strike = None
        self.open_position = None
        self.adding_pos = True
        self.spot_price = None
        self.call_data = {}
        self.put_data = {}
        self._fut_df: pd.DataFrame = pd.DataFrame()
        self._last_trading_day: str = ""
        self.cost = 80.0
        self.EMA_SHORT = 5
        self.EMA_LONG = 20
        self.ADX_PERIOD = 14
        self.ADX_LOOKBACK = 3
        self.MAX_TRADES = params["max_position"]
        self.SL_PCT = 0.10
        self.daily_trade_num = 0
        self.prev_day_fetched = False
        self.one_tick = None
        self._last_live_log_ts: float = 0.0  
        self._signal_minute_done: str = ""    
        self.breeze.ws_connect()
        self.breeze.ws_disconnect()
        self.breeze.on_ticks = self.on_ticks
        self.TRADE_HEADERS = ["Date", "Trade_Num", "Strike", "Entry_Time", "Entry_Premium",
                 "Initial_SL", "Peak_Price", "Final_Trail_SL",
                 "Exit_Time", "Exit_Premium", "PnL", "PnL_Rs", "Exit_Type"]
        
    def on_ticks(self,ticks):
        try:
            if 'strike_price' in ticks and ticks.get('right') in ['Call', 'Put']:
                data = str(ticks['strike_price']) + '_' + ticks['right']
                if ticks['right'] == 'Call' and data in self.call_data:
                    self.call_data[data] = ticks['last']
                elif ticks['right'] == 'Put' and data in self.put_data:
                    self.put_data[data] = ticks['last']
    
            elif ticks.get('last') is not None and 'strike_price' not in ticks:
                try:
                    self.spot_price = float(ticks['last'])
                except (TypeError, ValueError):
                    pass
        except Exception as e:
            logger.error(f"Error in on ticks ", exc_info = e) 

    def reconnect_ws(self):
        try: 
            logger.info("Reconnecting WebSocket...")
            try:
                self.breeze.ws_disconnect()
            except Exception:
                pass
            time_.sleep(2)
            self.breeze.ws_connect()
            self.breeze.on_ticks = self.on_ticks
            self.initiate_ws("others", "0")
            if self.open_position is not None:
                self.initiate_ws("call", int(self.open_position["strike"]))
            logger.info("WebSocket reconnected")
            time_.sleep(2)
        except Exception as e:
            logger.error(f"Error in reconnect ws ", exc_info = e)
            
    def get_cmp_with_retry(self,ce_or_pe, strike, retries=5) -> float | None:
        try:
            for attempt in range(1, retries + 1):
                cmp = self.get_current_market_price(ce_or_pe, strike)
                if cmp is not None and cmp != '' and float(cmp) > 0:
                    return float(cmp)
                logger.warning(f"CMP attempt {attempt}/{retries} failed — reconnecting WS")
                self.reconnect_ws()
            logger.error(f"CMP failed after {retries} attempts — skipping trade")
            return None
        except Exception as e:
            logger.error(f"Error in get cmp with retry ", exc_info = e)
            
    def initiate_ws(self, CE_or_PE, strike_price):
        try:
            expiry1 = datetime.strptime(self.OPTION_EXPIRY, '%Y-%m-%d').strftime('%d-%b-%Y')
    
            if CE_or_PE != 'others':
                leg = self.breeze.subscribe_feeds(
                    exchange_code="NFO",
                    stock_code="NIFTY",
                    product_type="options",
                    expiry_date=expiry1,
                    right=CE_or_PE,
                    strike_price=str(strike_price),
                    get_exchange_quotes=True,
                    get_market_depth=False
                )
                CE_or_PE_title = CE_or_PE.title()
                if CE_or_PE_title == 'Call':
                    self.call_data[f'{strike_price}_{CE_or_PE_title}'] = ''
                elif CE_or_PE_title == 'Put':
                    self.put_data[f'{strike_price}_{CE_or_PE_title}'] = ''
                print(leg)
    
            elif CE_or_PE == 'others':
                leg = self.breeze.subscribe_feeds(
                    exchange_code="NSE",
                    stock_code="NIFTY",
                    product_type="cash",
                    get_exchange_quotes=True,
                    get_market_depth=False
                )
                print(leg)
        except Exception as e:
            logger.error(f"initiate websocket Error: {str(e)}")
            print(f"initiate websocket error: {str(e)}")


    def deactivate_ws(self, CE_or_PE, strike_price):
        try:
            expiry1 = datetime.strptime(self.OPTION_EXPIRY, '%Y-%m-%d').strftime('%d-%b-%Y')
            leg = self.breeze.unsubscribe_feeds(
                exchange_code="NFO",
                stock_code="NIFTY",
                product_type="options",
                expiry_date=expiry1,
                right=CE_or_PE,
                strike_price=str(strike_price),
                get_exchange_quotes=True,
                get_market_depth=False
            )
            data = str(strike_price) + '_' + CE_or_PE.title()
            if data in self.call_data:
                self.call_data.pop(data)
            elif data in self.put_data:
                self.put_data.pop(data)
            else:
                print('Problem with', data)
            print(leg)
        except Exception as e:
            logger.error(f"Deactivate websocket Error: {str(e)}")


    def get_current_market_price(self, CE_or_PE, strike_price):
        try:
            data = str(strike_price) + '_' + CE_or_PE.title()
            if data in self.call_data and self.call_data[data] != '':
                return self.call_data[data]
            elif data in self.put_data and self.put_data[data] != '':
                return self.put_data[data]
            return None
        except Exception as e:
            logger.error(f" get current market price {str(e)}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def nifty_spot(self):
        try:
            if self.spot_price is not None:
                return float(self.spot_price)
            raise ValueError("No success response from API")
        except Exception as e:
            logging.error(f"Error fetching Nifty spot: {str(e)}")
            raise


    def update_and_compute(self, today: str) -> pd.DataFrame:
        try:
            now = datetime.now()
            if not self._last_trading_day or self._last_trading_day.split("_")[0] != today:
                self.prev_day_fetched = False
                for i in range(1, 8):
                    d = datetime.strptime(today, "%Y-%m-%d") - timedelta(days=i)
                    if d.weekday() >= 5:
                        continue
                    try:
                        resp = self.breeze.get_historical_data_v2(
                            interval="1minute",
                            from_date=f"{d.strftime('%Y-%m-%d')}T09:15:00.000Z",
                            to_date=f"{d.strftime('%Y-%m-%d')}T15:25:00.000Z",
                            stock_code="NIFTY", exchange_code="NFO",
                            product_type="futures",
                            expiry_date=f"{self.FUT_EXPIRY}T07:00:00.000Z",
                            right="others", strike_price="0"
                        )
                        if resp and resp.get("Success"):
                            prev_df = pd.DataFrame(resp["Success"])
                            prev_df["datetime"] = pd.to_datetime(prev_df["datetime"])
                            prev_df = prev_df.set_index("datetime").sort_index()
                            for col in ["open", "high", "low", "close", "volume"]:
                                prev_df[col] = pd.to_numeric(prev_df[col], errors="coerce")
                            self._last_trading_day = f"{today}_{d.strftime('%Y-%m-%d')}"
                            self._fut_df = prev_df
                            logger.info(
                                f"Last trading day fetched: {d.strftime('%Y-%m-%d')} | {len(prev_df)} rows"
                            )
                            self.prev_day_fetched = True
                            break
                    except Exception as ex:
                        logger.warning(f"prev-day fetch failed for {d.strftime('%Y-%m-%d')}: {ex}")
                        continue
        
                if not self.prev_day_fetched:
                    logger.error("update_and_compute: could not fetch ANY previous trading day in last 7 days")
            try:
                resp = self.breeze.get_historical_data_v2(
                    interval="1minute",
                    from_date=f"{today}T09:15:00.000Z",
                    to_date=now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    stock_code="NIFTY", exchange_code="NFO",
                    product_type="futures",
                    expiry_date=f"{self.FUT_EXPIRY}T07:00:00.000Z",
                    right="others", strike_price="0"
                )
        
                if not resp or not resp.get("Success"):
                    logger.warning("update_and_compute: no today data")
                    return self._fut_df
        
                today_df = pd.DataFrame(resp["Success"])
                today_df["datetime"] = pd.to_datetime(today_df["datetime"])
                today_df = today_df.set_index("datetime").sort_index()
                for col in ["open", "high", "low", "close", "volume"]:
                    today_df[col] = pd.to_numeric(today_df[col], errors="coerce")
        
                combined = pd.concat([self._fut_df, today_df])
                combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        
                c, h, l = combined["close"], combined["high"], combined["low"]
                combined["ema_short"] = ta.ema(c, length=self.EMA_SHORT)
                combined["ema_long"] = ta.ema(c, length=self.EMA_LONG)
                adx_df = ta.adx(h, l, c, length=self.ADX_PERIOD)
                combined["adx"] = adx_df[f"ADX_{self.ADX_PERIOD}"] if adx_df is not None else np.nan
                combined["vol_sma_5"]  = combined["volume"].rolling(5).mean()
                combined["vol_sma_15"] = combined["volume"].rolling(15).mean()
                combined["log_ret"] = np.log(combined["close"] / combined["close"].shift(1))
                combined["rv"]      = combined["log_ret"].rolling(10).std() * np.sqrt(375 * 252)
                combined.drop(columns=["log_ret"], inplace=True)
                self._fut_df = combined
                return self._fut_df
        
            except Exception as e:
                logger.error(f"update_and_compute error: {e}")
                return self._fut_df
        except Exception as e:
            logger.error(f"Error in update and compute", exc_info = e)


    def check_entry_signal(self, df: pd.DataFrame) -> bool:
        try:    
            df_valid = df.dropna(subset=["ema_short", "ema_long", "adx"])
            if len(df_valid) < self.ADX_LOOKBACK + 1:
                return False
        
            cur = df_valid.iloc[-1]
            adx_vals = df_valid["adx"].values[-self.ADX_LOOKBACK:]
        
            return (
                float(cur["close"]) > float(cur["ema_short"]) > float(cur["ema_long"])
                and all(adx_vals[i] > adx_vals[i - 1] for i in range(1, self.ADX_LOOKBACK))
            )
        except Exception as e:
            logger.warning(f" check_entry_signal {e}")
    
    def execute_entry(self) -> bool:
        try:
            if self.spot_price is None:
                logger.warning("execute_entry: spot_price is None")
                return False
        
            self.atm_strike = int(round(float(self.spot_price) / 50) * 50)
            self.initiate_ws("call", self.atm_strike)
            entry_premium = self.get_cmp_with_retry("call", self.atm_strike)
            if entry_premium is None:
                logger.warning(f"execute_entry: bad premium {entry_premium}")
                self.deactivate_ws("call", self.atm_strike)
                return False
        
            now = datetime.now()
            initial_sl = round(entry_premium * (1 - self.SL_PCT), 2)
            self.daily_trade_num += 1
        
            open_position = {
                "date": now.strftime("%Y-%m-%d"),
                "trade_num": self.daily_trade_num,
                "strike": self.atm_strike,
                "entry_time": now.strftime("%H:%M:%S"),
                "entry_premium": entry_premium,
                "initial_sl": initial_sl,
                "trail_sl": initial_sl,
                "peak_price": entry_premium,
            }
            self.open_position = open_position
            self.open_trade(self.open_position)
            logger.info(
                f"ENTRY | strike={self.atm_strike} | premium={entry_premium:.2f} | "
                f"sl={initial_sl:.2f} | trade#{self.daily_trade_num}"
            )
            return True
        except Exception as e:
            logger.warning(f" execute entry {e}")

    def check_and_exit(self) -> bool:
        try:
            if self.open_position is None:
                return False
            strike = int(self.open_position["strike"])
            entry_premium = float(self.open_position["entry_premium"])
            trail_sl = float(self.open_position["trail_sl"])
            peak_price = float(self.open_position["peak_price"])
            initial_sl = float(self.open_position["initial_sl"])
            cmp = self.get_cmp_with_retry("call", strike)
            if cmp is None:
                return False
            if cmp > peak_price:
                peak_price = cmp
                trail_sl = round(peak_price - (entry_premium * self.SL_PCT), 2)
                self.open_position["peak_price"] = peak_price
                self.open_position["trail_sl"] = trail_sl
                pd.DataFrame([self.open_position]).to_csv(self.POSITION_CSV, index=False)
                logger.info(f"NEW PEAK {peak_price:.2f} | trail_sl -> {trail_sl:.2f}")
        
            now_time = datetime.now().time()
            exit_type = None
            if now_time >= self.TIME_2:
                exit_type = "EOD"
            elif cmp <= trail_sl:
                exit_type = "Trailing_SL"
        
            if exit_type is None:
                now_ts = time_.time()
                if now_ts - self._last_live_log_ts >= 30:
                    logger.info(f"LIVE | cmp={cmp:.2f} | peak={peak_price:.2f} | sl={trail_sl:.2f}")
                    self._last_live_log_ts = now_ts
                return False
            now_str = datetime.now().strftime("%H:%M:%S")
            logger.info(
                f"EXIT [{exit_type}] | strike={strike} | entry={entry_premium:.2f} | "
                f"exit={cmp:.2f}"
            )
            self.close_trade(self.open_position, cmp, now_str, exit_type)
            self.deactivate_ws("call", strike)
            time_.sleep(60)
            self.open_position = None
            return True
        except Exception as e:
            logger.warning(f" check and exit {e}")

    def get_daily_trade_count(self,today: str) -> int:
        try:
            if not os.path.exists(self.TRADES_CSV):
                return 0
            df = pd.read_csv(self.TRADES_CSV, dtype=str)
            return len(df[df["Date"] == today])
        except Exception as e:
            logger.warning(f" get daily trade count {e}")  
            return 0

    def open_trade(self,pos: dict):
        try:
            pd.DataFrame([pos]).to_csv(self.POSITION_CSV, index=False)
        except Exception as e:
            logger.warning(f" open trade {e}") 

    def close_trade(self, pos: dict, exit_premium: float, exit_time: str, exit_type: str):
        try:
            if not os.path.exists(self.TRADES_CSV):
                pd.DataFrame(columns=self.TRADE_HEADERS).to_csv(self.TRADES_CSV, index=False)
        
            pnl_points = round(float(exit_premium) - float(pos["entry_premium"]), 2)
            pnl_rs     = round((pnl_points * self.QTY) - self.cost, 2)
        
            logger.info(f"PnL=Rs.{pnl_rs:.2f}")
            row = {
                "Date"           : pos["date"],
                "Trade_Num"      : pos["trade_num"],
                "Strike"         : pos["strike"],
                "Entry_Time"     : pos["entry_time"],
                "Entry_Premium"  : pos["entry_premium"],
                "Initial_SL"     : pos["initial_sl"],
                "Peak_Price"     : pos["peak_price"],
                "Final_Trail_SL" : pos["trail_sl"],
                "Exit_Time"      : exit_time,
                "Exit_Premium"   : exit_premium,
                "PnL_Points"     : pnl_points,
                "PnL_Rs"         : pnl_rs,
                "Exit_Type"      : exit_type,
            }
            pd.DataFrame([row]).to_csv(self.TRADES_CSV, mode="a", header=False, index=False)
            logger.info(f"CLOSED | {exit_type} | PnL=Rs.{pnl_rs}")
        
            if os.path.exists(self.POSITION_CSV):
                os.remove(self.POSITION_CSV)
        except Exception as e:
            logger.warning(f" close trade {e}") 

    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                logger.info("START: Option Buying Engine")
                self.breeze.ws_connect()
                self.breeze.on_ticks = self.on_ticks
                self.initiate_ws("others", "0")
                today = datetime.now().strftime("%Y-%m-%d")
                self.daily_trade_num = self.get_daily_trade_count(today)
                self.open_position = None            
                if os.path.exists(self.POSITION_CSV):
                    try:
                        df = pd.read_csv(self.POSITION_CSV, dtype={"date": str, "entry_time": str})
                        if not df.empty:
                            self.open_position = df.iloc[0].to_dict()
                            try:
                                self.daily_trade_num += 1 
                            except Exception:
                                self.daily_trade_num = 1
                            self.initiate_ws("call", int(self.open_position["strike"]))
                            time_.sleep(2)
                            logger.info(
                                f"RESUME: open position strike={self.open_position['strike']} | "
                                f"trade_num restored = {self.daily_trade_num}"
                            )
                    except Exception as e:
                        logger.error(f"crash recovery error: {e}")
            
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        if self.open_position is not None:
                            self.check_and_exit()
                            time_.sleep(0.5)
                            continue
                            
                        now = datetime.now()
                        current_time = now.time()
                    
                        if not (self.TIME_1 <= current_time <= self.TIME_2):
                            time_.sleep(5)
                            continue            
                        now_minute = now.strftime("%H:%M")
            
                        if now.second >= 5 and now_minute != self._signal_minute_done:
                            self._signal_minute_done = now_minute
            
                            df = self.update_and_compute(today)
            
                            if df.empty:
                                logger.warning("empty df, skipping signal check")
                                time_.sleep(1)
                                continue
            
                            if self.daily_trade_num < self.MAX_TRADES and self.check_entry_signal(df):
                                if current_time >= self.MAX_ENTRY_TIME:               # ← add this guard
                                    logger.info(f"NO ENTRY after 15:00 | {now}")
                                    continue
                                last_row = df.iloc[-1]
                                logger.info(
                                    f"SIGNAL | c={last_row['close']:.2f} "
                                    f"ema5={last_row['ema_short']:.2f} "
                                    f"ema20={last_row['ema_long']:.2f} "
                                    f"adx={last_row['adx']:.2f}"
                                )
                                success = self.execute_entry()
                                if not success:
                                    logger.warning("entry failed — will retry on next signal")
                            else:
                                logger.info(f"NO SIGNAL @ {now_minute} | trades today={self.daily_trade_num}")
            
                        time_.sleep(0.5)
            
                    except KeyboardInterrupt:
                        logger.info("SHUTDOWN: KeyboardInterrupt")
                        if self.open_position is not None:
                            pd.DataFrame([self.open_position]).to_csv(self.POSITION_CSV, index=False)
                        break
            
                    except Exception as e:
                        logger.error(f"main loop error: {e}\n{traceback.format_exc()}")
            
                        time_.sleep(2)


            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e