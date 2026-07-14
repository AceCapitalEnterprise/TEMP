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
from math import log, sqrt, exp
import logging
import signal
from ..common.logging_config import create_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = create_logger(__name__, 'trading_option_buying_New.log')

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
        self.temp_file="open_option_bying_position_2_RV.csv"
        self.max_trades=params["max_position"]
        self.entry_data = None
        self.sl_exit_time = None 
        self.stop_loss=10
        
        # Global Variables
        self.latest_ltp = None
        self.tick_data = {}
        self.ws_connected = False
        self.lock = threading.Lock()
        self.expiry = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry.strftime('%d-%b-%Y') 
        self.expiry2= self.expiry.strftime('%Y-%m-%d')

    #Retry decorator
    def retry_on_exception(self,exception):
        return isinstance(exception, (requests.exceptions.RequestException, Exception))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def connect_websocket(self):
        try:
            if not self.ws_connected:
                self.breeze.ws_connect()
                self.ws_connected = True
                logger.info("WebSocket connected successfully.")
        except Exception as e:
            self.ws_connected = False
            logger.error(f"WebSocket connection failed:", exc_info = e)
            raise
        
    def on_ticks(self,ticks):
        try:
            key = f"{ticks['strike_price']}_{ticks['right']}"
            self.latest_ltp= None
            with self.lock:
                self.tick_data[key] = ticks
                current_ltp = ticks.get("last")
                self.latest_ltp = current_ltp
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')}] {key} LTP: {current_ltp}")
            logger.debug(f"Received tick for {key}")
        except Exception as e:
            logger.error(f"Error in on_ticks: ", exc_info = e)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def initiate_ws(self,ce_or_pe, strike_price):
        exp=str(self.expiry1)
        right=str(ce_or_pe)
        strike=str(strike_price)
        exchange_code="NFO"
        stock_code="NIFTY"
        product_type="options"
        logger.info(f"Expiry : {exp} type{type(exp)}")
        logger.info(f"Right : {right} type{type(right)}")
        logger.info(f"Strike : {strike} type{type(strike)}")
        logger.info(f"exchange_code : {exchange_code} type{type(exchange_code)}")
        logger.info(f"stock_code : {stock_code} type{type(stock_code)}")
        logger.info(f"product_type : {product_type} type{type(product_type)}")
        try:
            self.breeze.subscribe_feeds(
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
                self.tick_data[f'{strike_price}_{ce_or_pe}'] = ''
            logger.info(f"Subscribed to {ce_or_pe} {strike_price}")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Failed to initiate WebSocket for {ce_or_pe} {strike_price}: ", exc_info = e)
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
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
            logger.info(f"Unsubscribed from {ce_or_pe} {strike_price}")
        except Exception as e:
            logger.error(f"Failed to deactivate WebSocket for {ce_or_pe} {strike_price}:",  exc_info = e)
            raise
    
    def round_to_nearest_50(self,x):
        return int(round(x / 50.0)) * 50
    
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
    
    def get_5_min_historical(self,from_date, to_date, exchange="NSE", interval="5minute"):
        return self.breeze.get_historical_data_v2(
            interval=interval,
            from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            stock_code="NIFTY",
            exchange_code="NSE",
            product_type="cash"
        )
    
    def get_1_min_historical(self,from_date, to_date, exchange="NSE", interval="1minute"):
        return self.breeze.get_historical_data_v2(
            interval=interval,
            from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            stock_code="NIFTY",
            exchange_code="NSE",
            product_type="cash"
        )

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
            if df_nifty.empty:
                raise ValueError("NIFTY data not available")

            df_nifty['datetime'] = pd.to_datetime(df_nifty['datetime'])
            df_nifty.set_index('datetime', inplace=True)
            df_nifty["log_ret"] = np.log(df_nifty['close'] / df_nifty['close'].shift(1))
            df_nifty["RV"] = df_nifty["log_ret"].rolling(window=10).std()* np.sqrt(252*390)
            time.sleep(1)

            # Fetching 1 min data 
            nifty_data_1 = self.get_1_min_historical((date-timedelta(days=1)).replace(hour=9, minute=0), date)
            df_nifty_1 = pd.DataFrame(nifty_data_1["Success"])
            if df_nifty_1.empty:
                raise ValueError("NIFTY data not available")

            df_nifty_1['datetime'] = pd.to_datetime(df_nifty_1['datetime'])
            df_nifty_1.set_index('datetime', inplace=True)
            time.sleep(1)

            prev_N=df_nifty[-6:-1].tail(N)
            last_N = df_nifty[df_nifty.index <=ct].tail(3)
            last=last_N.iloc[-1]['close']
            highest = prev_N["high"].max()
            lowest = prev_N["low"].min()

            last_N_1 = df_nifty_1[df_nifty_1.index <=ct].tail(3)
            Cond_1_BO=last_N_1.iloc[-2]['close']<last_N_1.iloc[-1]['open']
            Cond_1_BD=last_N_1.iloc[-2]['close']>last_N_1.iloc[-1]['open']
            logger.info(f"[BO] LP:{last_N_1.iloc[-2]['close']} < LC: {last_N_1.iloc[-1]['open']} :: Result : {Cond_1_BO}")
            logger.info(f"[BD] LP:{last_N_1.iloc[-2]['close']} > LC: {last_N_1.iloc[-1]['open']} :: Result : {Cond_1_BD}")

            RV= last_N.iloc[-2]['RV']<last_N.iloc[-1]['RV']
            
            print(f"[DEBUG] RV:  {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
            print(f"last: {last} || highest:{highest} || lowest :{lowest}")
            logger.info(f"last: {last} || highest:{highest} || lowest :{lowest}")
            logger.info(f"RV: {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f} ")
            
            C1=last>highest
            C2=last<lowest

            print(f"BO: {C1} | BD: {C2} | RV: {RV}  ")
            logger.info(f"BO: {C1} | BD: {C2} | RV: {RV} ")

            if len(prev_N) < N:
                return None,None
            
            if (date.time()<x) and (last > highest) and RV and Cond_1_BO:
                print(f"[DEBUG] RV:  {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
                print(f"Breakout last: {last}> highest:{highest}")
                logger.info(f"BO condition matched")
                return last, "BO"
            
            elif (date.time()<x) and (last < lowest) and RV and Cond_1_BD:
                print(f"[DEBUG] RV:  {last_N.iloc[-2]['RV']:.4f}<{last_N.iloc[-1]['RV']:.4f}  ")
                print(f"Breakdown last: {last}< lowest:{lowest}")
                logger.info(f"BD condition matched")
                return last,"BD"

            else:
                return None,None
            
        except Exception as e:
            logger.info(f"Condition matching Failed")
            return None,None
        
    def save_entry_data_to_csv(self,entry_data):
        try:
            with open(self.temp_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(entry_data.keys())
                writer.writerow(entry_data.values())
        except Exception as e:
            logger.error(f"Failed to Save Entry Data ",exc_info = e)
            raise
        
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
                row["trade_rem"] = int(row["trade_rem"])
                row["time"] = datetime.strptime(row["time"], "%Y-%m-%d %H:%M:%S.%f")
                row["atm"] = int(row["atm"])
                row["right"] = row["right"]
                row["type"] = row["type"]
                row["qty"] = int(row["qty"])
                row["entry"] = float(row["entry"])
                row["current"] = float(row["current"])
                row["cut"] = int(row["cut"])
                row["Qty_B"] = int(row["Qty_B"])
                row["max_pnl"] = float(row["max_pnl"])
                row["sl"] = float(row["sl"])
                row["tsl"] = float(row["tsl"])
                row["pnl"] = float(row["pnl"])
                return row
        except Exception as e:
            logger.error(f"Failed to Load Entry Data ", exc_info=e)
            raise

    def clear_position_state(self):
        try:
            if os.path.exists(self.temp_file):
                os.remove(self.temp_file)
        except Exception as e:
            logger.error(f"Failed to Delete Entry Data ",exc_info = e)
            raise
    
    def write_to_csv(self, data):
        headers = ['Date', 'Strike', 'Type','Sl','Entry Time','Entry premium', 
            'Exit Time', 'Exit premium', 'Max_PNL','Exit Type','PnL', 'Quantity', 'Total']
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
            logger.error(f"Error writing to CSV: ", exc_info = e)
            print(f"Error writing to CSV: {str(e)}")

    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                print("[START] Waiting for signal...")
                logger.info("[START] Waiting for signal...")
                
                self.entry_data = self.load_entry_data_from_csv()
                if self.entry_data:
                    self.max_trades= self.entry_data['trade_rem']   
                    atm_strike=self.entry_data['atm']
                    side=self.entry_data['right']
                    print(f"[RESUME] Loaded open position from CSV {side} {atm_strike}")
                    logger.info("[RESUME] Loaded open position from CSV")
                    self.initiate_ws(side,atm_strike)
                    time.sleep(1)
                
                self.connect_websocket()
                
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()
                        current_time=datetime.now().replace(microsecond=0)
                        
                        # ENTRY CHECK
                        if self.TIME_1 < current_time.time() < self.TIME_2 and self.max_trades>0 and self.entry_data is None and (now.time().minute % 5==0) and now.time().second == 0:
                            print(f"\n current_time: {current_time} ")
                            print("Checking Entry Condition")
                            logger.info(f"[Checking Entry Condition] at time: {current_time} ")
                            atm_price_raw,type= self.calculate_atm(current_time)

                            if atm_price_raw is not None and type is not None:
                                atm_price = float(atm_price_raw)
                                atm_strike = self.round_to_nearest_50(atm_price)
                                side=None

                                if type=="BO":
                                    atm_strike=atm_strike
                                    side="call"
                                elif type=="BD":
                                    atm_strike=atm_strike
                                    side="put"
                
                            
                                print(f"Subscribing to NIFTY {side} Strike {atm_strike}...")
                                self.initiate_ws(side,str(atm_strike))
                                print("--- Waiting for live ticks  ---")
                                time.sleep(2)
                                
                                print(f"{side} {atm_strike} Ltp: {self.latest_ltp}")
                                entry_time_actual = current_time
                                entry_prem = self.latest_ltp
                                # entry_prem= self.place_order( side, atm_strike, "buy", self.QTY) #Placing Buy
                            
                                print(f"ENTRY at {entry_time_actual.time()} | Entry_Prem: {entry_prem:.2f} ")
                                     
                                sl = self.stop_loss
                                if (sl>entry_prem):
                                    sl=math.floor(entry_prem)
                                stoploss=-(sl)
                                
                                current_time=datetime.now()
                                current_time=datetime.strptime(str(current_time), "%Y-%m-%d %H:%M:%S.%f")
                                
                                self.max_trades=self.max_trades-1
                                
                                # Cleaned up entry_data dictionary
                                self.entry_data = {
                                    "trade_rem": self.max_trades,
                                    "time": current_time,
                                    "atm": atm_strike,
                                    "right": side,
                                    "type": type,
                                    "qty": self.QTY,
                                    "entry": entry_prem,
                                    "current": 0,
                                    "cut": 0,   # PB_C equivalent
                                    "Qty_B": 0, # Booked Quantity
                                    "max_pnl": 0,
                                    "sl": stoploss,
                                    "tsl": stoploss,
                                    "pnl": 0    # PNL_C equivalent
                                }

                                self.save_entry_data_to_csv(self.entry_data)
                                self.sl_exit_time = None
                                print(f"[ENTRY] Time: {current_time} ATM: {atm_strike} Entry Premium: {entry_prem}")
                                logger.info(f"[ENTRY] Time: {current_time} ATM: {atm_strike} Entry Premium: {entry_prem}")
                        
                            time.sleep(1)

                        # THREE TIER EXIT LOGIC CHECK
                        elif self.entry_data and self.TIME_1 < now.time() < self.TIME_2:
                            current_prem = self.latest_ltp
                            if current_prem is None:
                                continue # Wait for next valid tick
                                
                            self.entry_data["current"] = current_prem
                            entry_prem = self.entry_data["entry"]
                            pnl = current_prem - entry_prem

                            # Always update the running peak
                            if pnl > self.entry_data['max_pnl']:
                                self.entry_data['max_pnl'] = pnl
                                
                            max_profit = self.entry_data['max_pnl']
                            Quantity_C = self.entry_data["qty"] - self.entry_data["Qty_B"]
                            PB_C = self.entry_data["cut"]
                            
                            exit_triggered = False
                            exit_type = ""

                            # ══════════════════════════════════════════════════════════════
                            # TIER 1  |  max_profit < 5  →  Hard stop at max_profit − 10
                            # ══════════════════════════════════════════════════════════════
                            if max_profit < 5:
                                if pnl <= (max_profit - 10):
                                    exit_triggered = True
                                    exit_type = "ISL"
                            
                            # ══════════════════════════════════════════════════════════════
                            # TIER 2  |  5 ≤ max_profit < 10
                            # ══════════════════════════════════════════════════════════════
                            elif max_profit < 10:
                                step = max(0, int(max_profit % 10) - 5)
                                complete_exit_level = 3.0 + (step * 0.5)

                                if pnl <= complete_exit_level:
                                    exit_triggered = True
                                    exit_type = "CE_T2"

                            # ══════════════════════════════════════════════════════════════
                            # TIER 3  |  max_profit ≥ 10
                            # ══════════════════════════════════════════════════════════════
                            else:
                                partial_exit_level  = max_profit - min(5.0,  max_profit / 4.0)
                                complete_exit_level = max_profit - min(10.0, max_profit / 2.0)

                                # Partial booking fires once
                                if PB_C == 0 and pnl <= partial_exit_level:
                                    PARTIAL_QTY = self.entry_data["qty"] // 2
                                    if PARTIAL_QTY > 0:
                                        # self.square_off(self.entry_data["right"], self.entry_data["atm"], "sell", PARTIAL_QTY)
                                        self.entry_data["pnl"] += pnl * PARTIAL_QTY
                                        self.entry_data["Qty_B"] += PARTIAL_QTY
                                        self.entry_data["cut"] = 1
                                        PB_C = 1
                                        Quantity_C = self.entry_data["qty"] - self.entry_data["Qty_B"]
                                        print(f"  [PARTIAL] @ {now.time()} pnl/unit={pnl:.2f}  level={partial_exit_level:.2f}  rem_qty={Quantity_C}")
                                        logger.info(f"[PARTIAL BOOKING] pnl/unit={pnl:.2f} | REM Qt:{Quantity_C}")

                                # Complete exit
                                if pnl <= complete_exit_level:
                                    exit_triggered = True
                                    exit_type = "TSL_FULL" if PB_C == 0 else "TSL_AFTER_PARTIAL"

                            # ── Handle Full Exit Trigger ─────────────────────────
                            if exit_triggered:
                                # self.square_off(self.entry_data["right"], self.entry_data["atm"], "sell", Quantity_C)
                                exit_premium = current_prem
                                exit_time_actual = now
                                Total = (Quantity_C * pnl) + self.entry_data["pnl"]
                                pts = Total / self.entry_data["qty"]
                                t = now.strftime("%H:%M:%S")

                                print(f"  EXIT ({exit_type}) @ {t} | price={exit_premium:.2f}  PnL=₹{Total:.2f}")
                                logger.info(f"[{exit_type} HIT] Time: {t} || Exit Prem: {exit_premium:.2f} || PnL: {pnl:.2f} || Total: ₹{Total}")
                                
                                self.write_to_csv([
                                    self.entry_data["time"].strftime("%Y-%m-%d"),
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
                                    f"{Total:.2f}"
                                ])
                                
                                self.deactivate_ws(self.entry_data["right"], self.entry_data["atm"])
                                self.entry_data = None
                                self.clear_position_state()
                                self.sl_exit_time = now
                            else:
                                self.save_entry_data_to_csv(self.entry_data)

                        # End-of-day exit
                        elif self.entry_data and now.time() >= self.TIME_2 and now.second == 0:
                            current_prem = self.latest_ltp
                            if current_prem is not None:
                                pnl = current_prem - self.entry_data["entry"]
                                Quantity_C = self.entry_data["qty"] - self.entry_data["Qty_B"]
                                
                                # self.square_off(self.entry_data["right"], self.entry_data["atm"], "sell", Quantity_C)
                                Total = (Quantity_C * pnl) + self.entry_data["pnl"]
                                pts = Total / self.entry_data["qty"]
                                exit_premium = current_prem
                                exit_time_actual = current_time
                                exit_type = "EOD"
                                t = now.strftime("%H:%M:%S")
                                
                                print(f"  EOD EXIT @ {t} | price={exit_premium:.2f}  PnL=₹{Total:.2f}")
                                logger.info(f"[EOD] Time: {t} || Exit Prem: {exit_premium:.2f} || PnL: {pnl:.2f} || Total: ₹{Total}")
                                
                                self.write_to_csv([
                                    self.entry_data["time"].strftime("%Y-%m-%d"),
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
                                    f"{Total:.2f}"
                                ])
                                
                                self.deactivate_ws(self.entry_data["right"], self.entry_data["atm"])
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