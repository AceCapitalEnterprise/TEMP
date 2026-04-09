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

logger = create_logger(__name__, 'trading_option_buying_index.log')

class StrategyOptionBuyingIndex(BaseStrategy):
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
        
        self.breeze.on_ticks = self.on_ticks
        self.breeze.ws_connect()

        start_time = str(params["start_hour"]) + ":" + str(params["start_minute"])
        end_time = str(params["end_hour"]) + ":" + str(params["end_minute"])

        self.TIME_1 = datetime.strptime(start_time, "%H:%M").time()
        self.TIME_2 = datetime.strptime(end_time, "%H:%M").time()
        self.EXPIRY = params["expiry"]
        self.FUT_EXPIRY=params["fut_expiry"]
        self.QTY = params["qty"]
        self.csv_file = params["csv_file"]
        self.temp_file="open_option_bying_position_index.csv"
        self.max_trades=params["max_position"]
        self.entry_data = None
        self.sl_exit_time = None 
        self.stop_loss=20
        
        # Global Variables
        self.latest_ltp = None
        self.tick_data = {}
        self.ws_connected = False
        self.lock = threading.Lock()
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
    def connect_websocket(self):
        try:
            if not self.ws_connected:
                self.breeze.ws_connect()
                self.ws_connected = True
                print("WebSocket connected successfully.")
        except Exception as e:
            self.ws_connected = False
            print(f"WebSocket connection failed:", exc_info = e)
            raise

    def on_ticks(self,ticks):
        try:
            key = f"{ticks['strike_price']}_{ticks['right']}"
            with self.lock:
                self.tick_data[key] = ticks
            print(f"Received tick for {key}")
            current_ltp = ticks.get("last")
            self.latest_ltp = current_ltp
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')}] {key} LTP: {current_ltp}")
        except Exception as e:
            print(f"Error in on_ticks: ", exc_info = e)

    def initiate_ws(self,ce_or_pe, strike_price):
        print(f"Expiry : {self.expiry1}")
        try:
            data =self.breeze.subscribe_feeds(
                exchange_code="NFO",
                stock_code="NIFTY",
                product_type="options",
                expiry_date=str(self.expiry1),
                right=str(ce_or_pe),
                strike_price=str(strike_price),
                get_exchange_quotes=True,
                get_market_depth=False
            )
            time.sleep(2)
            ce_or_pe = ce_or_pe.title()
            with self.lock:
                self.tick_data[f'{strike_price}_{ce_or_pe}'] = ''
            print(f"Subscribed to {ce_or_pe} {strike_price}")
            print(data)
        except Exception as e:
            print(f"Failed to initiate WebSocket for {ce_or_pe} {strike_price}: ", exc_info = e)
            raise
    
    def deactivate_ws(self,ce_or_pe, strike_price):
        try:
            self.breeze.unsubscribe_feeds(
                exchange_code="NFO",
                stock_code="NIFTY",
                product_type="options",
                expiry_date=self.expiry1,
                right=ce_or_pe,
                strike_price=str(strike_price),
                get_exchange_quotes=True,
                get_market_depth=False
            )
            ce_or_pe = ce_or_pe.title()
            with self.lock:
                if f'{strike_price}_{ce_or_pe}' in self.tick_data:
                    self.tick_data.pop(f'{strike_price}_{ce_or_pe}')
            print(f"Unsubscribed from {ce_or_pe} {strike_price}")
        except Exception as e:
            print(f"Failed to deactivate WebSocket for {ce_or_pe} {strike_price}:",  exc_info = e)
            raise
    
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
    # <--------------------- Fetching live ltp------------------->
    def get_live_ltp(self,strike, right):
        data=self.breeze.get_quotes(stock_code="NIFTY",
                    exchange_code="NFO",
                    expiry_date=str(self.expiry),
                    product_type="options",
                    right=right,
                    strike_price=strike)
        p=data['Success'][0]['ltp']
        print(f"Value: {p}")
        return p

    
    # <--------------------- Buying and Squaring off  Order ------------------->

    # Buying

    def place_order(self,right, strike, action, quantity):
        try:
            order_detail = self.breeze.place_order(
                stock_code="NIFTY",
                exchange_code="NFO",
                product="options",
                action=action,
                order_type="market",
                quantity=quantity,
                price="",
                validity="day",
                disclosed_quantity="0",
                expiry_date=f'{self.expiry1}T06:00:00.000Z',
                right=right,
                strike_price=strike
            )
            time.sleep(5)
            order_id = order_detail['Success']['order_id']
            trade_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
            execution_price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
            print(f"Order placed: {action} {quantity} {right} at strike {strike} for {execution_price}")
            return execution_price

        except Exception as e:
            logger.error(f"Error placing order for {right} {strike}: {e}")
            print(f"[ERROR] | Error placing {action} order for {right} {strike}: {str(e)}")
            

    # Square off

    def square_off(self,right, strike, action, quantity):
        try:
            order_detail = self.breeze.square_off(
                stock_code="NIFTY",
                exchange_code="NFO",
                product="options",
                action=action,
                order_type="market",
                quantity=quantity,
                price="",
                validity="day",
                disclosed_quantity="0",
                expiry_date=f'{self.expiry1}T06:00:00.000Z',
                right=right,
                strike_price=strike
                )
            time.sleep(5)
            order_id = order_detail['Success']['order_id']
            trade_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
            execution_price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
            print(f"Order placed: {action} {quantity} {right} at strike {strike} for {execution_price}")
            return execution_price
        except Exception as e:
            logger.error(f"Error placing order for {right} {strike}: {e}")
            print(f"[ERROR] | Error placing {action} order for {right} {strike}: {str(e)}")
            

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
            time.sleep(1)

            #<--------------- change in entry condition-------------->
            prev_N=df_nifty[-6:-1].tail(N)
            last_N = df_nifty[df_nifty.index <=ct].tail(3)
            last=last_N.iloc[-1]['close']
            highest = prev_N["high"].max()
            lowest = prev_N["low"].min()
            RV=last_N.iloc[-3]['RV'] < last_N.iloc[-2]['RV']<last_N.iloc[-1]['RV']
            print(f"[DEBUG] RV: {last_N.iloc[-3]['RV']:.4f} < {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
            # print(f"last: {last} || highest:{highest} || lowest :{lowest} || MA_13: {MA_13} || MA_34 : {MA_34}")
            print(f"last: {last} || highest:{highest} || lowest :{lowest}")
            logger.info(f"last: {last} || highest:{highest} || lowest :{lowest}")
            logger.info(f"RV: {last_N.iloc[-3]['RV']:.4f} < {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f} ")
            RV=last_N.iloc[-3]['RV'] < last_N.iloc[-2]['RV']<last_N.iloc[-1]['RV']
            C1=last>highest
            C2=last<lowest
            # M=None


            print(f"BO: {C1} | BD: {C2} | RV: {RV}  ")
            logger.info(f"BO: {C1} | BD: {C2} | RV: {RV} ")

            
            if len(prev_N) < N:
                return None,None
            
            if (date.time()<x) and (last > highest) and RV:
                print(f"[DEBUG] RV: {last_N.iloc[-3]['RV']:.4f} < {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
                print(f"Breakout last: {last}> highest:{highest}")
                logger.info(f"BO condition matched")
                return last+100, "BO"
            
            elif (date.time()<x) and (last < lowest) and RV:
                print(f"[DEBUG] RV: {last_N.iloc[-3]['RV']:.4f} < {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
                print(f"Breakdown last: {last}< lowest:{lowest}")
                logger.info(f"BD condition matched")
                return last-100,"BD"

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
                logger.info("[START] Waiting for signal...")
                # <------------- Fetchinh Previous Entry If any --------------->
                self.entry_data = self.load_entry_data_from_csv()
                # logger.info(f"Checking Open Position response:{self.entry_data}")
                if self.entry_data:
                    self.max_trades= self.entry_data['trade_rem']   
                    atm_strike=self.entry_data['atm']
                    side=self.entry_data['right']
                    print(f"[RESUME] Loaded open position from CSV {side} {atm_strike}")
                    logger.info("[RESUME] Loaded open position from CSV")
                    self.initiate_ws(side,atm_strike)
                    time.sleep(1)
                
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()
                        current_time = now.time()    
                        current_time=datetime.now().replace(microsecond=0)
                        # logger.info("Entered inside while Loop")
                        if self.TIME_1 < current_time.time() < self.TIME_2 and self.max_trades>0 and self.entry_data is None and (now.time().minute % 1==0) and now.time().second == 0:
                            print(f"\n current_time: {current_time} ")
                            print("Checking Entry Condition")
                            logger.info(f"[Checking Entry Condition] at time: {current_time} ")
                            # atm_price_raw,type= self.calculate_atm(current_time)
                            atm_price_raw,type= 23400,"BO"

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
                                p=self.get_live_ltp(atm_strike,side)
                                if p<20 and side=="put":
                                    atm_strike=atm_strike+100
                                
                                if p<20 and side=="call":
                                    atm_strike=atm_strike-100
                            
                                print(f"Subscribing to NIFTY {side} Strike {atm_strike}...")
                                self.initiate_ws(side,atm_strike)
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
                                # entry_prem= self.place_order( side, atm_strike, "buy", self.QTY) #Placing Buy
                            
                                print(f"ENTRY at {entry_time_actual.time()} | Entry_Prem: {entry_prem:.2f} ")
                               
                                print("<---++++++-->")
                                print(tte)
                                print("<---++++++-->")
                                atr_value = df.loc[current_time]['ATR']
                                
                                # # # Annualize (sqrt(252) ~ trading days)
                                # volatility = self.calculate_iv(entry_prem, atm_price, atm_strike, tte, self.RISK_FREE_RATE, self.DIVIDEND, side)

                                # <<_change_>> 
                            
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
                        
                            time.sleep(1)

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
                                self.entry_data['tsl']= self.entry_data['max_pnl'] + self.entry_data['sl']
                            
                            # # TRAILING SL = Peak - 10 pts (only after partial booking)
                            # if self.entry_data["cut"] == 1 and (abs(self.entry_data['sl'])>10):
                            #     trailing_sl_points = self.entry_data['max_pnl'] - 10     # peak - 10
                            #     self.entry_data['tsl'] = trailing_sl_points   
                            
                            # # Partial booking condition 
                            if pnl>=20 and self.entry_data["cut"]==0 and (Qty_PB>=65):
                                # current_prem= self.square_off(side, atm_strike, "sell",Qty_PB) #Placing square off
                                pnl=current_prem-self.entry_data["entry"]
                                self.entry_data['pnl']=pnl*Qty_PB
                                self.entry_data["Qty_B"]=Qty_PB   
                                self.entry_data["cut"]=self.entry_data["cut"]+1
                                print(f"[Partial Booking Done] Attempt:{self.entry_data['cut']}")
                                Quantity_C=self.entry_data["qty"]-self.entry_data["Qty_B"]
                                print(f"PNL:{self.entry_data['pnl']} | REM Qt:{Quantity_C}")
                                logger.info(f"[PARTIAL BOOKING] PNL:{self.entry_data['pnl']} | REM Qt:{Quantity_C}")
                            
                            self.save_entry_data_to_csv(self.entry_data)


                            # # Exit on Initial Sl
                            # if (pnl<=self.entry_data['tsl'] and self.entry_data['cut']==0):
                            #     current_prem=self.latest_ltp
                            #     # Q=self.entry_data["qty"]
                            #     Q = self.entry_data["qty"]-self.entry_data["Qty_B"]

                            #     pnl=(current_prem-self.entry_data["entry"])
                            #     Total=Q*pnl+self.entry_data["pnl"] 

                            #     pts = Total / self.entry_data["qty"] 
                            #     exit_premium=current_prem
                            #     exit_time_actual=current_time
                            #     exit_type="Initial Sl"
                            #     # use values stored in entry_data for logging
                            #     self.deactivate_ws(self.entry_data["atm"], self.entry_data["right"])
                            #     t=now.strftime("%H:%M:%S")
                            #     logger.info(f"[Initial SL HIT] Time: {t} || Exit Prem: {exit_premium:.2f} || PnL: {pnl:.2f} || Total: ₹{Total}")
                            #     self.write_to_csv([self.entry_data["time"].strftime("%Y-%m-%d"),
                            #         self.entry_data["atm"],
                            #         self.entry_data["type"],
                            #         self.entry_data["sl"],
                            #         self.entry_data["time"].strftime("%H:%M:%S"),
                            #         self.entry_data["entry"],
                            #         t,
                            #         f"{exit_premium:.2f}",
                            #         self.entry_data["max_pnl"],
                            #         exit_type,
                            #         f"{pts:.2f}",
                            #         self.entry_data["qty"],
                            #         f"{Total:.2f}"])
                                
                            #     self.entry_data = None
                            #     self.clear_position_state()
                            #     self.sl_exit_time = now  #  Mark time of SL exit
                            
                            # Exit on trailing SL or low
                            # if (self.entry_data and pnl<=self.entry_data['tsl'] and self.entry_data['cut']==1):
                            
                            if (self.entry_data and pnl<=self.entry_data['tsl'] ):
                                current_prem=self.latest_ltp
                                Q = self.entry_data["qty"]-self.entry_data["Qty_B"]
                                # current_prem= self.square_off(side, atm_strike, "sell",Q) #Placing square off
                                pnl=(current_prem-self.entry_data["entry"])
                                Total=Q*pnl+self.entry_data["pnl"] 

                                pts = Total / self.entry_data["qty"] 
                                exit_premium=current_prem
                                exit_time_actual=current_time
                                exit_type="Trailing Sl"
                                # use values stored in entry_data for logging
                                self.deactivate_ws( self.entry_data["right"],self.entry_data["atm"])
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
                            # current_prem= self.square_off(side, atm_strike, "sell",Q) #Placing square off
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
                            
                            self.deactivate_ws( self.entry_data["right"],self.entry_data["atm"])
                            self.entry_data = None
                            self.clear_position_state()
                            self.sl_exit_time = None
                            print(f"[INFO] Market close time reached. Exiting strategy. at {exit_time_actual} PNL : {Total}")
                            break                        

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