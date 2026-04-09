from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
import math
from datetime import datetime, date, timedelta, time as dt
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

logger = create_logger(__name__, 'trading_option_buying.log')

class StrategyOptionBuying(BaseStrategy):
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
            logger.error(f"Failed to initialize BreezeConnect: ", exc_info = e)
            raise e
        
        self.breeze.ws_connect()
        self.breeze.on_ticks=self.on_ticks

        start_time = str(params["start_hour"]) + ":" + str(params["start_minute"])
        end_time = str(params["end_hour"]) + ":" + str(params["end_minute"])

        self.TIME_1 = datetime.strptime(start_time, "%H:%M").time()
        self.TIME_2 = datetime.strptime(end_time, "%H:%M").time()
        self.EXPIRY = params["expiry"]
        self.FUT_EXPIRY=params["fut_expiry"]
        self.QTY = params["qty"]
        self.csv_file = params["csv_file"]
        self.temp_file="open_option_bying_position.csv"
        self.max_trades=params["max_position"]
        self.entry_data = None
        self.sl_exit_time = None 
        self.stop_loss=20
        
        # Global Variables
        self.tick_data = {}
        self.latest_ltp = None
        self.lock = threading.Lock()
        self.first_tick_received = threading.Event()
        self.tick_data_lock = threading.Lock()
        self.expiry = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry.strftime('%d-%b-%Y') 

        # Greeks Parameters
        self.RATE_LIMIT_DELAY = 3
        self.RISK_FREE_RATE = 0.07
        self.DIVIDEND=1.4
        self.W_DELTA = 0.5  # Weight for Delta
        self.W_THETA = 0.3  # Weight for Theta
        self.W_VEGA = 0.2   # Weight for Vega

    # <--------------------- For Fetching Live Option tick by tick data using Web socket ------------------->
    # Helper function to normalize the right for dictionary keys
    def get_key_from_params(self,strike, right):
        normalized_right = right.upper().replace('CALL', 'CE').replace('PUT', 'PE')
        return f"{strike}_{normalized_right}"
    
    def on_ticks(self,ticks):
        # Check if the tick is a data update (not just a subscription confirmation message)
        if 'strike_price' in ticks and 'right' in ticks:
            
            # Determine the key for the global dictionary
            strike = ticks['strike_price']
            right = ticks['right']
            key = self.get_key_from_params(strike, right)
            
            current_ltp = ticks.get('last')
            
            # Use lock when reading/writing to shared dictionary
            with self.tick_data_lock: 
                self.tick_data[key] = ticks
                self.latest_ltp = current_ltp # Store the most recent LTP globally

            # Print the LIVE, tick-by-tick LTP data
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')}] LIVE TICK for {key}: LTP = {current_ltp}")
            
            # Signal the main thread that the first tick has arrived
            if not self.first_tick_received.is_set():
                self.first_tick_received.set()
                
        else:
            # Handle subscription success/error messages
            print(f"Subscription update/message: {ticks}")

    def initiate_ws(self,strike_price, right):
        # Reset the event before subscribing to ensure we wait for the first tick
        self.first_tick_received.clear() 
        
        leg = self.breeze.subscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry1,
                                right=right,
                                strike_price=str(strike_price),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        
        key = self.get_key_from_params(strike_price, right)
        
        with self.tick_data_lock:
            # Initialize the dictionary entry with None to ensure the key exists quickly
            self.tick_data[key] = None 
        
        print(leg)
        print(f"Subscribed to {key}.")

    def deactivate_ws(self,strike_price, right):     
        # Reset the event before subscribing to ensure we wait for the first tick
        self.first_tick_received.clear() 
        
        leg = self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry1,
                                right=right,
                                strike_price=str(strike_price),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        
        key = self.get_key_from_params(strike_price, right)
        
        with self.tick_data_lock:
            if key in self.tick_data:
                self.tick_data.pop(key) 
        print(leg)
        print(f"Unsubscribed to {key}.")
    # ---------------------------------------------------------------------
        

    # # Retry decorator
    # def retry_on_exception(self,exception):
    #     return isinstance(exception, (requests.exceptions.RequestException, Exception))
    

    # @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    # def initiate_ws( self,atm,right):
    #     self.latest_ltp
    #     atm_str=str(atm)
    #     try:
    #         if right=='call':    
    #             leg1=self.breeze.subscribe_feeds(exchange_code= "NFO", 
    #                     stock_code="NIFTY", 
    #                     expiry_date=self.expiry1, 
    #                     strike_price=atm_str, 
    #                     right=right, 
    #                     product_type="options", 
    #                     get_market_depth=False ,
    #                     get_exchange_quotes=True,
    #                     )
    #             # print(leg1)

    #         elif right=='put':
    #             leg2=self.breeze.subscribe_feeds(exchange_code= "NFO", 
    #                     stock_code="NIFTY", 
    #                     expiry_date=self.expiry1, 
    #                     strike_price=atm_str, 
    #                     right=right, 
    #                     product_type="options", 
    #                     get_market_depth=False ,
    #                     get_exchange_quotes=True,
    #                     )
    #             # print(leg2)
    #         logger.info(f"Subscribed to {right} {atm}")
    #         time.sleep(2)
    #     except Exception as e:
    #         logger.error(f"Failed to initiate WebSocket for {right} {atm}: ", exc_info = e)
    #         raise
        

    # # Disconnecting websocket 
    # @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    # def deactivate_ws(self, atm,right):
    #     self.latest_ltp
    #     atm_str=str(atm)
    #     try:
    #         if right=='call':    
    #             self.breeze.unsubscribe_feeds(exchange_code="NFO",
    #                                 stock_code="NIFTY", 
    #                                 expiry_date=self.expiry1, 
    #                                 strike_price=atm_str, 
    #                                 right=right, 
    #                                 product_type="options", 
    #                                 get_market_depth=False ,
    #                                 get_exchange_quotes=True,
    #                                 )


    #         elif right=='put':
    #             self.breeze.unsubscribe_feeds(exchange_code="NFO",
    #                                 stock_code="NIFTY", 
    #                                 expiry_date=self.expiry1, 
    #                                 strike_price=atm_str, 
    #                                 right=right, 
    #                                 product_type="options", 
    #                                 get_market_depth=False ,
    #                                 get_exchange_quotes=True,
    #                                 )
    #         logger.info(f"Unsubscribed from {right} {atm}")
    #     except Exception as e:
    #         logger.error(f"Failed to deactivate WebSocket for {right} {atm}:",  exc_info = e)
    #         raise
    
    
    # <------------------- For fetching Option data tick by tick ------------------->

    def generate_master_index(self,from_date, to_date):
        # Round from_date and to_date to minute
        from_date = from_date.replace(microsecond=0)
        to_date = to_date.replace(microsecond=0)
        # Create all 1-minute timestamps between range
        return pd.date_range(start=from_date, end=to_date, freq="1s")

    def get_option_df_complete(self,expiry,strike, right, from_date, to_date):
        """
        Fetch full 1-second data for an option between from_date and to_date,
        bypassing Breeze's 1000-candle limit by looping.
        """
        # interval = "1second"
        max_points = 1000  # Breeze limit
        step_seconds = 1000  # 1000 * 1 sec = 1000 seconds (~16.6 minutes)

        current_start = from_date
        all_data = []

        while current_start < to_date:
            current_end = min(current_start + timedelta(seconds=step_seconds), to_date)

            try:
                data = self.breeze.get_historical_data_v2(
                    interval="1second",
                    from_date=current_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    to_date=current_end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    stock_code="NIFTY",
                    exchange_code="NFO",
                    product_type="options",
                    expiry_date=expiry,
                    right=right,
                    strike_price=strike
                )

                if "Success" in data and data["Success"]:
                    all_data.extend(data["Success"])
                    # print(f"Fetched {len(data['Success'])} candles from {current_start} to {current_end}")
                # else:
                    # print(f"No data between {current_start} and {current_end}")

            except Exception as e:
                print(f"Error fetching data between {current_start} and {current_end}: {e}")

            # Move window forward
            current_start = current_end
            time.sleep(0.2)  # Slight delay to avoid rate limit

        # Combine all segments
        if not all_data:
            raise ValueError(f"No data received for strike {strike} {right}")

        df = pd.DataFrame(all_data)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)

        df = df[~df.index.duplicated(keep='last')]
        # . Reindex to master timeline
        master_index = self.generate_master_index(from_date, to_date)
        df = df.reindex(master_index)

        # Optional: forward fill OHLC values for missing bars
        df = df.ffill()

        print(f"Final dataframe has {len(df)} rows from {df.index.min()} to {df.index.max()}")
        return df
    # <--------------------- Calculatig Last N Candle Low ------------------->

    def option_initial_sl_from_low(self,option_df, enter_ts, lookback_seconds= 15 * 60):
        # lookback window in seconds before enter_ts
        start = enter_ts - timedelta(seconds=lookback_seconds)
        if start < option_df.index.min():
            start = option_df.index.min()
        segment = option_df.loc[start:enter_ts]
        if segment.empty:
            return None
        low = segment['close'].min()
        return low
    # <--------------------- Rounding off ------------------->
    def round_to_nearest_50(self,x):
        return int(round(x / 50.0)) * 50
    
    # <--------------------- Getting Nifty Option  ------------------->
    def get_5_min_historical_optional(self,expiry,strike, right, from_date, to_date):
        return self.breeze.get_historical_data_v2(
                    interval="5minute",
                    from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    stock_code="NIFTY",
                    exchange_code="NFO",
                    product_type="options",
                    expiry_date=expiry,
                    right=right,
                    strike_price=strike
                )
    
    # <--------------------- Getting Nifty Data  ------------------->

    def get_5_min_historical(self,from_date, to_date, exchange="NSE", interval="5minute"):
        return self.breeze.get_historical_data_v2(
            interval=interval,
            from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            stock_code="NIFTY",
            exchange_code="NSE",
            product_type="cash"
        )
    # <--------------------- Checking Entry Condition ------------------->
    def calculate_atm(self,ct):
        try:
            N=5
            x=dt(15, 19)
            print(f"ct:{ct}")
            date=datetime.now()
            print(f"date:{date}")
            time.sleep(2)
            nifty_data = self.get_5_min_historical((date-timedelta(days=4)).replace(hour=9, minute=0), date)
            df_nifty = pd.DataFrame(nifty_data["Success"])
            # print(df_nifty)
            if df_nifty.empty:
                raise ValueError("NIFTY data not available")

            df_nifty['datetime'] = pd.to_datetime(df_nifty['datetime'])
            df_nifty.set_index('datetime', inplace=True)
            df_nifty["log_ret"] = np.log(df_nifty['close'] / df_nifty['close'].shift(1))
            df_nifty["RV"] = df_nifty["log_ret"].rolling(window=10).std()* np.sqrt(252*390)
            time.sleep(2)
            # df_nifty["EMA_34"]=ta.sma(df_nifty["close"], length=34)
            # df_nifty["EMA_13"]=ta.sma(df_nifty["close"], length=13)

            #<--------------- change in entry condition-------------->
            prev_N=df_nifty[-6:-1].tail(N)
            last_N = df_nifty[df_nifty.index <=ct].tail(3)
            last=last_N.iloc[-1]['close']
            strike=self.round_to_nearest_50(last)
            highest = prev_N["high"].max()
            lowest = prev_N["low"].min()
            RV=last_N.iloc[-3]['RV'] < last_N.iloc[-2]['RV']<last_N.iloc[-1]['RV']
            # MA_34=last_N.iloc[-1]['EMA_34']
            # MA_13=last_N.iloc[-1]['EMA_13']
            print(f"[DEBUG] RV: {last_N.iloc[-3]['RV']:.4f} < {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
            # print(f"last: {last} || highest:{highest} || lowest :{lowest} || MA_13: {MA_13} || MA_34 : {MA_34}")
            print(f"last: {last} || highest:{highest} || lowest :{lowest}")
            logger.info(f"last: {last} || highest:{highest} || lowest :{lowest}")
            logger.info(f"RV: {last_N.iloc[-3]['RV']:.4f} < {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f} ")
            RV=last_N.iloc[-3]['RV'] < last_N.iloc[-2]['RV']<last_N.iloc[-1]['RV']
            C1=last>highest
            C2=last<lowest
            M=None


            # Fetching Option
            if C1:
                side="call"
                nifty_data_side = self.get_5_min_historical_optional(self.expiry,strike,side,(date-timedelta(days=4)).replace(hour=9, minute=0), date)
                df_nifty_side = pd.DataFrame(nifty_data_side["Success"])
                # print(df_nifty)
                if df_nifty_side.empty:
                    raise ValueError("NIFTY data not available")
                df_nifty_side['datetime'] = pd.to_datetime(df_nifty_side['datetime'])
                df_nifty_side.set_index('datetime', inplace=True)
                df_nifty_side["EMA_34"]=ta.sma(df_nifty_side["close"], length=34)
                df_nifty_side["EMA_13"]=ta.sma(df_nifty_side["close"], length=13)
                M=df_nifty_side.iloc[-1]["EMA_13"]>df_nifty_side.iloc[-1]["EMA_34"]
                logger.info(f"EMA_13_call : {df_nifty_side.iloc[-1]['EMA_13']} | EMA_34_call : {df_nifty_side.iloc[-1]['EMA_34']}")
                time.sleep(2)

            
            if C2:
                side="put"
                nifty_data_side = self.get_5_min_historical_optional(self.expiry,strike,side,(date-timedelta(days=4)).replace(hour=9, minute=0), date)
                df_nifty_side = pd.DataFrame(nifty_data_side["Success"])
                # print(df_nifty)
                if df_nifty_side.empty:
                    raise ValueError("NIFTY data not available")
                df_nifty_side['datetime'] = pd.to_datetime(df_nifty_side['datetime'])
                df_nifty_side.set_index('datetime', inplace=True)
                df_nifty_side["EMA_34"]=ta.sma(df_nifty_side["close"], length=34)
                df_nifty_side["EMA_13"]=ta.sma(df_nifty_side["close"], length=13)
                M=df_nifty_side.iloc[-1]["EMA_13"]>df_nifty_side.iloc[-1]["EMA_34"]
                logger.info(f"EMA_13_put : {df_nifty_side.iloc[-1]['EMA_13']} | EMA_34_put : {df_nifty_side.iloc[-1]['EMA_34']}")
                time.sleep(2)


            print(f"BO: {C1} | BD: {C2} | RV: {RV} | M: {M} ")
            logger.info(f"BO: {C1} | BD: {C2} | RV: {RV} | M: {M}")

            
            if len(prev_N) < N:
                return None,None
            
            if (date.time()<x) and (last > highest) and RV and M:
                print(f"[DEBUG] RV: {last_N.iloc[-3]['RV']:.4f} < {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
                print(f"Breakout last: {last}> highest:{highest}")
                logger.info(f"BO condition matched")
                return last, "BO"
            
            elif (date.time()<x) and (last < lowest) and RV and M:
                print(f"[DEBUG] RV: {last_N.iloc[-3]['RV']:.4f} < {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
                print(f"Breakdown last: {last}< lowest:{lowest}")
                logger.info(f"BD condition matched")
                return last,"BD"

            else:
                return None,None
            
        except Exception as e:
            logger.info(f"Condition matching Failed")
            return None,None
        
    
    # <---------------------------------- For Checking if Entry is Already exit  ------------------------------>
    # For saving position
    def save_entry_data_to_csv(self,entry_data):
        try:
            with open(self.temp_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(entry_data.keys())
                writer.writerow(entry_data.values())
        except Exception as e:
            logger.error(f"Failed to Save Entry Data ",exc_info = e)
            raise
        

    # LOading pos if existed
    def load_entry_data_from_csv(self):
        try:
            if not os.path.exists(self.temp_file):
                return None
            with open(self.temp_file, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not rows:
                    return None
                row = rows[0]
                row["trade_rem"]=int(row["trade_rem"])
                row["time"] = datetime.strptime(row["time"], "%Y-%m-%d %H:%M:%S.%f")
                row["atm"] = int(row["atm"])
                row["right"] = row["right"]
                row["type"] = row["type"]
                row["qty"] = int(row["qty"])
                row["qty1"] = int(row["qty1"])
                row["qty2"] = int(row["qty2"])
                row["qty3"] = int(row["qty3"])
                row["entry"] = float(row["entry"])
                row["entry1"] = float(row["entry1"])
                row["entry2"] = float(row["entry2"])
                row["entry3"] = float(row["entry3"])
                row["current"] = float(row["current"])
                row["cut"] = int(row["cut"])
                row["Qty_B"] = int(row["Qty_B"])
                row["max_pnl"] = float(row["max_pnl"])
                row["low"] = float(row["low"])
                row["sl"] = float(row["sl"])
                row["tsl"] = float(row["tsl"])
                row["pnl"] = float(row["pnl"])
                return row
        except Exception as e:
            logger.error(f"Failed to Load Entry Data ",exc_info = e)
            raise

    # Deleting position
    def clear_position_state(self):
        try:
            if os.path.exists(self.temp_file):
                os.remove(self.temp_file)
        except Exception as e:
            logger.error(f"Failed to Delete Entry Data ",exc_info = e)
            raise
    
    # <---------------------------------- Creating CSV ------------------------------>
    
    # Creating csv
    def write_to_csv(self, data):
        headers = ['Date', 'Strike', 'Type','Sl','Entry Time','Entry premium', 
                'Exit Time', 'Exit premium', 'Max_PNL','Exit Type','PnL', 'Quantity', 'Total']
        try:
            try:
                with open(self.csv_file, 'x', newline='') as file: #change krna hai output file ko csv file mai
                    writer = csv.writer(file)
                    writer.writerow(headers)
                    print("Created new CSV file")
            except FileExistsError:
                pass
            with open(self.csv_file, 'a', newline='') as file: #change krna hai output file ko csv file mai
                writer = csv.writer(file)
                writer.writerow(data)
                print(f"Trade data written to CSV: {data}")
        except Exception as e:
            logger.error(f"Error writing to CSV: ", exc_info = e)
            print(f"Error writing to CSV: {str(e)}")

    # <---------------------------------- Main loop ------------------------------>

    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                print("[START] Waiting for signal...")
                # <------------- Fetchinh Previous Entry If any --------------->
                self.entry_data = self.load_entry_data_from_csv()
                if self.entry_data:
                    self.max_trades= self.entry_data['trade_rem']   
                    atm_strike=self.entry_data['atm']
                    side=self.entry_data['right']
                    print(f"[RESUME] Loaded open position from CSV {side} {atm_strike}")
                    logger.info("[RESUME] Loaded open position from CSV")
                    self.initiate_ws(atm_strike, side)
                    time.sleep(1)
                
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()
                        current_time = now.time()    
                        current_time=datetime.now().replace(microsecond=0)
                        if self.TIME_1 <= current_time.time() < self.TIME_2 and self.max_trades>0 and self.entry_data is None and (now.time().minute % 5==0) and now.time().second == 0:
                            print(f"\n current_time: {current_time} ")
                            print("Checking Entry Condition")
                            logger.info(f"[Checking Entry Condition] at time: {current_time} ")
                            atm_price_raw,type= self.calculate_atm(current_time)

                            print(f"Entry condition Checking done Result atm_price_raw : {atm_price_raw} | type: {type}")
                            logger.info(f"Entry condition Checking done Result atm_price_raw : {atm_price_raw} | type: {type}")
                            if atm_price_raw is not None and type is not None:
                                atm_price = float(atm_price_raw)
                                atm_strike = self.round_to_nearest_50(atm_price)
                                side=None

                                df=pd.DataFrame()
                                if type=="BO":
                                    atm_strike=atm_strike
                                    df = self.get_option_df_complete(self.expiry, atm_strike, "call", current_time-timedelta(minutes=18), current_time)
                                    df['ATR'] = ta.atr(df['high'], df['low'], df['close'],length=14)
                                    side="call"

                                elif type=="BD":
                                    atm_strike=atm_strike
                                    df = self.get_option_df_complete(self.expiry, atm_strike, "put", current_time-timedelta(minutes=18), current_time)
                                    df['ATR'] = ta.atr(df['high'], df['low'], df['close'],length=14)
                                    side="put"
                
                                sl_lookback_seconds = 15* 60
                                low = self.option_initial_sl_from_low(df, current_time-timedelta(seconds=10), lookback_seconds=sl_lookback_seconds)
                            
                                print(f"Subscribing to NIFTY {side} Strike {atm_strike}...")
                                self.initiate_ws(atm_strike, side)
                                print("--- Waiting for live ticks  ---")
                                time.sleep(2)
                                
                                print(f"{side} {atm_strike} Ltp: {self.latest_ltp}")
                                exit_premium = None
                                exit_time_actual = None
                                exit_time = datetime.combine(now.date(), self.TIME_2)
                                expiry_dt = self.expiry + timedelta(hours=15, minutes=30)
                                tte = (expiry_dt - now).total_seconds() / (365 * 24 * 60 * 60)
                                entry_time_actual = current_time
                                entry_prem = self.latest_ltp
                            
                                print(f"ENTRY at {entry_time_actual.time()} | Entry_Prem: {entry_prem:.2f} ")
                               
                                print("<---++++++-->")
                                print(tte)
                                print("<---++++++-->")
                                atr_value = df.loc[current_time]['ATR']
                                
                                # # # Annualize (sqrt(252) ~ trading days)
                                # volatility = self.calculate_iv(entry_prem, atm_price, atm_strike, tte, self.RISK_FREE_RATE, self.DIVIDEND, side)

                                # # <<_change_>> 
                            
                                # # max_profit = 0
                                # print(f"ATR: {atr_value}, Volatility_: {volatility}")
                                
                                sl = self.stop_loss
                                if (sl>entry_prem):
                                    sl=math.floor(entry_prem)
                                stoploss=-(sl)
                                print(f"SL : {stoploss} | Low: {low} ")
                                current_time=datetime.now()
                                print(f"t1:{current_time}")
                                current_time=datetime.strptime(str(current_time), "%Y-%m-%d %H:%M:%S.%f")
                                print(f"t2:{current_time}")
                                print(f"curr: {current_time}")
                                self.max_trades=self.max_trades-1
                                self.entry_data = {
                                    "trade_rem":self.max_trades,
                                    "time": current_time,
                                    "atm": atm_strike,
                                    "right":side,
                                    "type":type,
                                    "qty":self.QTY,
                                    "qty1":self.QTY,
                                    "qty2":0,
                                    "qty3":0,
                                    "entry": entry_prem,
                                    "entry1": entry_prem,
                                    "entry2": 0,
                                    "entry3": 0,
                                    "current":0,
                                    "cut":0,
                                    "Qty_B":0,
                                    "max_pnl": 0,
                                    "low":low,
                                    "sl":stoploss,
                                    "tsl": stoploss,
                                    "pnl":0
                                }

                                self.save_entry_data_to_csv(self.entry_data)
                                self.sl_exit_time = None  #  Reset SL flag
                                print(f"[ENTRY] Time: {current_time} ATM: {atm_strike} Entry Premium: {entry_prem}")
                                logger.info(f"[ENTRY] Time: {current_time} ATM: {atm_strike} Entry Premium: {entry_prem}")
                        

                        # TRAIL STOP LOSS CHECK
                        elif self.entry_data and self.TIME_1 < now.time() < self.TIME_2:
                            current_prem = self.latest_ltp
                            self.entry_data["current"]=current_prem
                            entry_prem = self.entry_data["entry"]
                            Q = self.entry_data["qty"]-self.entry_data["Qty_B"]
                            pnl = (current_prem - entry_prem)
                            Qty_PB=int(Q/2)
                            
                            if (pnl> self.entry_data['max_pnl']):
                                self.entry_data['max_pnl']=pnl
                                # self.entry_data['tsl']= self.entry_data['max_pnl'] + self.entry_data['sl']
                            
                            # TRAILING SL = Peak - 10 pts (only after partial booking)
                            if self.entry_data["cut"] == 1 and (abs(self.entry_data['sl'])>10):
                                trailing_sl_points = self.entry_data['max_pnl'] - 10     # peak - 10
                                self.entry_data['tsl'] = trailing_sl_points   
                            
                            # # Partial booking condition 
                            if (pnl>=20) and self.entry_data["cut"]==0 and (Qty_PB>=65):
                                self.entry_data['pnl']=pnl*Qty_PB
                                self.entry_data["Qty_B"]=Qty_PB   
                                self.entry_data["cut"]=self.entry_data["cut"]+1
                                print(f"[Partial Booking Done] Attempt:{self.entry_data['cut']}")
                                Quantity_C=self.entry_data["qty"]-self.entry_data["Qty_B"]
                                print(f"PNL:{self.entry_data['pnl']} | REM Qt:{Quantity_C}")
                                logger.info(f"[PARTIAL BOOKING] PNL:{self.entry_data['pnl']} | REM Qt:{Quantity_C}")
                            
                            self.save_entry_data_to_csv(self.entry_data)


                            # Exit on Initial Sl
                            if (pnl<=self.entry_data['tsl'] and self.entry_data['cut']==0):
                                current_prem=self.latest_ltp
                                # Q=self.entry_data["qty"]
                                Q = self.entry_data["qty"]-self.entry_data["Qty_B"]

                                pnl=(current_prem-self.entry_data["entry"])
                                Total=Q*pnl+self.entry_data["pnl"] 

                                pts = Total / self.entry_data["qty"] 
                                exit_premium=current_prem
                                exit_time_actual=current_time
                                exit_type="Initial Sl"
                                # use values stored in entry_data for logging
                                self.deactivate_ws(self.entry_data["atm"], self.entry_data["right"])
                                t=now.strftime("%H:%M:%S")
                                logger.info(f"[Initial SL HIT] Time: {t} || Exit Prem: {exit_premium:.2f} || PnL: {pnl:.2f} || Total: ₹{Total}")
                                self.write_to_csv([self.entry_data["time"].strftime("%Y-%m-%d"),
                                    self.entry_data["atm"],
                                    self.entry_data["type"],
                                    self.entry_data["sl"],
                                    self.entry_data["time"].strftime("%H:%M:%S"),
                                    self.entry_data["entry"],
                                    t,
                                    f"{exit_premium:.2f}",
                                    self.entry_data["max_pnl"],
                                    exit_type,
                                    f"{pts:.2f}",
                                    self.entry_data["qty"],
                                    f"{Total:.2f}"])
                                
                                self.entry_data = None
                                self.clear_position_state()
                                self.sl_exit_time = now  #  Mark time of SL exit
                            
                            # Exit on trailing SL or low
                            if (self.entry_data and pnl<=self.entry_data['tsl'] and self.entry_data['cut']==1):
                                current_prem=self.latest_ltp
                                Q = self.entry_data["qty"]-self.entry_data["Qty_B"]
                                pnl=(current_prem-self.entry_data["entry"])
                                Total=Q*pnl+self.entry_data["pnl"] 

                                pts = Total / self.entry_data["qty"] 
                                exit_premium=current_prem
                                exit_time_actual=current_time
                                exit_type="Trailing Sl"
                                # use values stored in entry_data for logging
                                self.deactivate_ws(self.entry_data["atm"], self.entry_data["right"])
                                t=now.strftime("%H:%M:%S")
                                logger.info(f"[Trailing SL HIT] Time: {t} || Exit Prem: {exit_premium:.2f} || PnL: {pnl:.2f} || Total: ₹{Total}")
                                self.write_to_csv([self.entry_data["time"].strftime("%Y-%m-%d"),
                                    self.entry_data["atm"],
                                    self.entry_data["type"],
                                    self.entry_data["sl"],
                                    self.entry_data["time"].strftime("%H:%M:%S"),
                                    self.entry_data["entry"],
                                    t,
                                    f"{exit_premium:.2f}",
                                    self.entry_data["max_pnl"],
                                    exit_type,
                                    f"{pts:.2f}",
                                    self.entry_data["qty"],
                                    f"{Total:.2f}"])
                                
                                self.entry_data = None
                                self.clear_position_state()
                                self.sl_exit_time = now  #  Mark time of SL exit

                        # End-of-day exit
                        elif self.entry_data and now.time() >= self.TIME_2 and now.second == 0:
                            current_prem=self.latest_ltp
                            Q = self.entry_data["qty"]-self.entry_data["Qty_B"]
                            pnl=(current_prem-self.entry_data["entry"])
                            Total=(Q*pnl)+self.entry_data["pnl"] 
                            pts = Total / self.entry_data["qty"] 
                            exit_premium=current_prem
                            exit_time_actual=current_time
                            exit_type = "EOD"
                            t=now.strftime("%H:%M:%S")
                            logger.info(f"[EOD] Time: {t} || Exit Prem: {exit_premium:.2f} || PnL: {pnl:.2f} || Total: ₹{Total}")
                            self.write_to_csv([self.entry_data["time"].strftime("%Y-%m-%d"),
                                    self.entry_data["atm"],
                                    self.entry_data["type"],
                                    self.entry_data["sl"],
                                    self.entry_data["time"].strftime("%H:%M:%S"),
                                    self.entry_data["entry"],
                                    t,
                                    f"{exit_premium:.2f}",
                                    self.entry_data["max_pnl"],
                                    exit_type,
                                    f"{pts:.2f}",
                                    self.entry_data["qty"],
                                    f"{Total:.2f}"])
                            
                            self.deactivate_ws(self.entry_data["atm"], self.entry_data["right"])
                            self.entry_data = None
                            self.clear_position_state()
                            self.sl_exit_time = None
                            print(f"[INFO] Market close time reached. Exiting strategy. at {exit_time_actual} PNL : {Total}")
                            
                            break
                        time.sleep(1)

                    except Exception as e:
                        logger.error(f"Main loop error: ", exc_info = e)
                        print(f"Main loop error: {str(e)}")
                        time.sleep(60)

            
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e