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
logger = create_logger(__name__, 'NayanCreditSpreads.log')

class StrategyNayanCreditSpreads(BaseStrategy):
    def __init__(self, params):
        super().__init__()
        
        self.POSITION_CSV = "open_position_credit_spread.csv"
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
        self.cost = 160.0
        self.EMA_SHORT = 5
        self.EMA_LONG = 15
        self.ADX_PERIOD = 14
        self.ADX_LOOKBACK = 3
        self.MAX_TRADES = params["max_position"]
        self.daily_trade_num = 0
        self.prev_day_fetched = False
        self.one_tick = None
        self._last_live_log_ts: float = 0.0  
        self._signal_minute_done: str = ""    
        self.breeze.ws_connect()
        self.breeze.ws_disconnect()
        self.breeze.on_ticks = self.on_ticks
        self.TRADE_HEADERS = [
        "Date", "Trade_Num", "Strategy", "Side", "Right",
        "Sell_Strike", "Buy_Strike",
        "Entry_Time", "Sell_Entry", "Buy_Entry", "Entry_Net",
        "SL_Level",
        "Exit_Time", "Sell_Exit", "Buy_Exit", "Exit_Net",
        "PnL", "PnL_Rs", "Exit_Type"
        ]
    # self.breeze.ws_connect()


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
            
    
    
    # self.breeze.on_ticks = self.on_ticks

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
                df_pos = pd.read_csv(self.POSITION_CSV, dtype=str)
                self.open_position = df_pos.iloc[0].to_dict()
                right = self.open_position.get("right", "call")
                self.initiate_ws(right, int(self.open_position["sell_strike"]))
                self.initiate_ws(right, int(self.open_position["buy_strike"]))
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
                            interval="5minute",
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
                    interval="5minute",
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

    def get_h6_avg(self,df: pd.DataFrame, now: datetime) -> float | None:
        try:
            hourly = df.resample("1h", offset="15min").agg({
                "open":   "first",
                "high":   "max",
                "low":    "min",
                "close":  "last",
                "volume": "sum"
            }).dropna(how="all")
            
            current_bar_start = now.replace(second=0, microsecond=0)
            current_bar_start = current_bar_start - timedelta(
                minutes=(current_bar_start.minute - 15) % 60
            )
            hourly = hourly[hourly.index < current_bar_start].dropna()
            # print("hourly",hourly)
    
            if len(hourly) < 6:
                logger.warning(f"h6_avg: only {len(hourly)} completed hourly candles")
                return None
    
            return float(hourly["close"].iloc[-6:].mean())
    
        except Exception as e:
            logger.warning(f"h6_avg error: {e}")
            return None
                
    def check_entry_signal(self,df: pd.DataFrame, now: datetime) -> str | None:
        try:
            df_valid = df.dropna(subset=["ema_short", "ema_long", "adx", "vol_sma_5", "vol_sma_15", "rv"])
            if len(df_valid) < self.ADX_LOOKBACK + 1:
                return None
        
            cur      = df_valid.iloc[-1]
            adx_vals = df_valid["adx"].values[-self.ADX_LOOKBACK:]
            close  = float(cur["close"])
            ema_s  = float(cur["ema_short"])
            ema_l  = float(cur["ema_long"])
            logger.info(f" close={close:.2f} ema5={ema_s:.2f} ema15={ema_l:.2f} ")
            h6_avg = self.get_h6_avg(df, now)
            logger.info(f" h6_avg = {h6_avg}")
            if h6_avg is None:
                logger.warning("h6_avg: not enough hourly candles")
                return None

            if float(cur["rv"]) > 0.20:
                logger.info(f"rv={cur['rv']:.3f} ")
                return None
            if not all(adx_vals[i] > adx_vals[i - 1] for i in range(1, self.ADX_LOOKBACK)):
                logger.info(f"adx=[{adx_vals[0]:.1f},{adx_vals[1]:.1f},{adx_vals[2]:.1f}] ")
                return None
            if float(cur["vol_sma_5"]) <= float(cur["vol_sma_15"]):
                logger.info(f"vol5={cur['vol_sma_5']:.0f} vol15={cur['vol_sma_15']:.0f}")
                return None


        
            # ── direction ─────────────────────────────────────────────
            if close > ema_s > ema_l and close > h6_avg:
                logger.info(
                    f"[BULL] close={close:.2f} ema5={ema_s:.2f} ema15={ema_l:.2f} "
                    f"h6_avg={h6_avg:.2f} rv={cur['rv']:.3f} "
                    f"adx=[{adx_vals[0]:.1f},{adx_vals[1]:.1f},{adx_vals[2]:.1f}] "
                    f"vol5={cur['vol_sma_5']:.0f} vol15={cur['vol_sma_15']:.0f}"
                )
                return "bull"
        
            if close < ema_s < ema_l and close < h6_avg:
                logger.info(
                    f"[BEAR] close={close:.2f} ema5={ema_s:.2f} ema15={ema_l:.2f} "
                    f"h6_avg={h6_avg:.2f} rv={cur['rv']:.3f} "
                    f"adx=[{adx_vals[0]:.1f},{adx_vals[1]:.1f},{adx_vals[2]:.1f}] "
                    f"vol5={cur['vol_sma_5']:.0f} vol15={cur['vol_sma_15']:.0f}"
                )
                return "bear"
        
            return None
        except Exception as e:
            logger.warning(f" check_entry_signal {e}")
    
    def execute_entry(self,side: str) -> bool:
        try:
            if self.spot_price is None:
                logger.warning("execute_entry: spot_price is None")
                return False
        
            self.atm_strike = int(round(float(self.spot_price) / 50) * 50)
        
            # ── strike selection ──────────────────────────────────────
            if side == "bull":
                sell_strike = self.atm_strike - 50
                buy_strike  = self.atm_strike - 250
                right       = "put"
                strategy    = "Bull_Put_Spread"
            else:
                sell_strike = self.atm_strike + 50
                buy_strike  = self.atm_strike + 250
                right       = "call"
                strategy    = "Bear_Call_Spread"
        
            # ── subscribe both legs ───────────────────────────────────
            self.initiate_ws(right, sell_strike)
            self.initiate_ws(right, buy_strike)
            time_.sleep(1)  
        
            sell_premium = self.get_cmp_with_retry(right, sell_strike)
            buy_premium  = self.get_cmp_with_retry(right, buy_strike)
        
            if sell_premium is None or buy_premium is None:
                logger.warning(f"execute_entry: could not fetch premiums — sell={sell_premium}, buy={buy_premium}")
                self.deactivate_ws(right, sell_strike)
                self.deactivate_ws(right, buy_strike)
                return False
        
            if sell_premium < 10:
                logger.warning(f"execute_entry: sell leg premium too low ({sell_premium:.2f} < 10), skipping")
                self.deactivate_ws(right, sell_strike)
                self.deactivate_ws(right, buy_strike)
                return False
        
            entry_net = round(sell_premium - buy_premium, 2)
            if entry_net <= 0:
                logger.warning(f"execute_entry: net credit <= 0 ({entry_net:.2f}), skipping")
                self.deactivate_ws(right, sell_strike)
                self.deactivate_ws(right, buy_strike)
                return False
        
            # ── SL level: spread widens to 2×credit − 3 ──────────────
            sl_level = round(2 * entry_net - 3, 2)
        
            now = datetime.now()
            self.daily_trade_num += 1
        
            open_position = {
                "date":          now.strftime("%Y-%m-%d"),
                "trade_num":     self.daily_trade_num,
                "strategy":      strategy,
                "side":          side,
                "right":         right,
                "sell_strike":   sell_strike,
                "buy_strike":    buy_strike,
                "entry_time":    now.strftime("%H:%M:%S"),
                "sell_premium":  sell_premium,
                "buy_premium":   buy_premium,
                "entry_net":     entry_net,
                "sl_level":      sl_level,
                "peak_price":    entry_net,   # for spread: peak = tightest spread seen
            }
            self.open_position = open_position
            self.open_trade(self.open_position)
            logger.info(
                f"ENTRY | {strategy} | sell={sell_strike}@{sell_premium:.2f} "
                f"buy={buy_strike}@{buy_premium:.2f} | "
                f"net_credit={entry_net:.2f} | sl_level={sl_level:.2f} | trade#{self.daily_trade_num}"
            )
            return True
        except Exception as e:
            logger.warning(f" execute entry {e}")

    
    def check_and_exit(self) -> bool:
        try:
            if self.open_position is None:
                return False
        
            right       = self.open_position["right"]
            sell_strike = int(self.open_position["sell_strike"])
            buy_strike  = int(self.open_position["buy_strike"])
            entry_net   = float(self.open_position["entry_net"])
            sl_level    = float(self.open_position["sl_level"])
        
            sell_cmp = self.get_cmp_with_retry(right, sell_strike)
            buy_cmp  = self.get_cmp_with_retry(right, buy_strike)
        
            if sell_cmp is None or buy_cmp is None:
                return False
        
            current_net = round(sell_cmp - buy_cmp, 2)
            pnl_points  = round(entry_net - current_net, 2)   # credit shrinking = profit
        
            now_time   = datetime.now().time()
            exit_type  = None
        
            if now_time >= self.TIME_2:
                exit_type = "EOD"
            elif current_net >= sl_level:
                exit_type = "SL"
        
            if exit_type is None:
                now_ts = time_.time()
                if now_ts - self._last_live_log_ts >= 30:
                    logger.info(
                        f"LIVE | side = {right} | sell strike = {sell_strike} @ {sell_cmp} | buy strike = {buy_strike} @ {buy_cmp}"
                        f"net={current_net:.2f} | entry_net={entry_net:.2f}" 
                        f"pnl_pts={pnl_points:.2f} | sl={sl_level:.2f}"
                    )
                    self._last_live_log_ts = now_ts
                return False
        
            now_str = datetime.now().strftime("%H:%M:%S")
            logger.info(
                f"EXIT [{exit_type}] | net={current_net:.2f} | pnl_pts={pnl_points:.2f}"
            )
            self.close_trade(self.open_position, sell_cmp, buy_cmp, current_net, now_str, exit_type)
            self.deactivate_ws(right, sell_strike)
            self.deactivate_ws(right, buy_strike)
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
    
    def close_trade(self, pos: dict, sell_exit: float, buy_exit: float,
                    exit_net: float, exit_time: str, exit_type: str):
        try:
            if not os.path.exists(self.TRADES_CSV):
                pd.DataFrame(columns=self.TRADE_HEADERS).to_csv(self.TRADES_CSV, index=False)
        
            pnl_points = round(float(pos["entry_net"]) - exit_net, 2)
            pnl_rs     = round((pnl_points * self.QTY) - self.cost, 2)
        
            row = {
                "Date":        pos["date"],
                "Trade_Num":   pos["trade_num"],
                "Strategy":    pos["strategy"],
                "Side":        pos["side"],
                "Right":       pos["right"],
                "Sell_Strike": pos["sell_strike"],
                "Buy_Strike":  pos["buy_strike"],
                "Entry_Time":  pos["entry_time"],
                "Sell_Entry":  pos["sell_premium"],
                "Buy_Entry":   pos["buy_premium"],
                "Entry_Net":   pos["entry_net"],
                "SL_Level":    pos["sl_level"],
                "Exit_Time":   exit_time,
                "Sell_Exit":   sell_exit,
                "Buy_Exit":    buy_exit,
                "Exit_Net":    exit_net,
                "PnL_Points":  pnl_points,
                "PnL_Rs":      pnl_rs,
                "Exit_Type":   exit_type,
            }
            pd.DataFrame([row]).to_csv(self.TRADES_CSV, mode="a", header=False, index=False)
            logger.info(f"CLOSED | {exit_type} | PnL=Rs.{pnl_rs:.2f}")
        
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
                logger.info("START: Credit Spread Engine")
                self.breeze.ws_connect()
                self.breeze.on_ticks = self.on_ticks
                self.initiate_ws("others", "0")
            
                today = datetime.now().strftime("%Y-%m-%d")
                self.daily_trade_num = self.get_daily_trade_count(today)
                self.open_position = None
                if os.path.exists(self.POSITION_CSV):
                    try:
                        df_pos = pd.read_csv(self.POSITION_CSV, dtype=str)
                        if not df_pos.empty:
                            self.open_position = df_pos.iloc[0].to_dict()
                            self.daily_trade_num += 1 
                            right = self.open_position.get("right", "call")
                            self.initiate_ws(right, int(self.open_position["sell_strike"]))
                            self.initiate_ws(right, int(self.open_position["buy_strike"]))
                            time_.sleep(2)
                            logger.info(f"RESUME: {self.open_position['strategy']} | trade#{self.open_position['trade_num']}")
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
                        candle_minute = now.minute - (now.minute % 5)
                        candle_ts     = now.strftime(f"%H:{candle_minute:02d}")
            
                        if now.second >= 10 and candle_ts != self._signal_minute_done:
                            self._signal_minute_done = candle_ts
            
                            df = self.update_and_compute(today)
                            if df.empty:
                                logger.warning("empty df, skipping signal check")
                                time_.sleep(1)
                                continue
                            print(f"Time:{now}")
                            side = self.check_entry_signal(df, now)
                            if self.daily_trade_num < self.MAX_TRADES and side is not None:
                                if current_time >= self.MAX_ENTRY_TIME:               # ← add this guard
                                    logger.info(f"NO ENTRY after 15:00 | {now}")
                                    continue
                                last_row = df.iloc[-1]
                                logger.info(
                                    f"SIGNAL {side.upper()} | "
                                    f"close={last_row['close']:.2f} "
                                    f"ema5={last_row['ema_short']:.2f} "
                                    f"ema15={last_row['ema_long']:.2f} "
                                    f"adx={last_row['adx']:.2f} "
                                    f"rv={last_row['rv']:.3f}"
                                )
                                success = self.execute_entry(side)
                                if not success:
                                    logger.warning("entry failed — will retry on next signal")
                            else:
                                logger.info(f"NO SIGNAL @ {now} | trades today={self.daily_trade_num}")
            
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