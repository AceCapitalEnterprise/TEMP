from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from py5paisa import FivePaisaClient
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

logger = create_logger(__name__, 'trading_straddle_strategy_10_lag_PaperTrade.log')

class StrategyStraddleTenLagPaperFivePaisa(BaseStrategy):
    def __init__(self, params):
        super().__init__()

        #Initializing 
        try:
            self.cred = {
            "APP_NAME": "5P50989130",
            "APP_SOURCE": "24320",
            "USER_ID": "0dRTMBbKRFd",
            "PASSWORD": "8GbywRjxMgB",
            "USER_KEY": "fBgBM44FAlFp8BWSi1oSFA0UV4ZdHWDS",
            "ENCRYPTION_KEY": "WY3cdcuzZWLfYhn0tQKVT06ypL1BtKzf"
            }

            self.client = FivePaisaClient(cred=self.cred)
            self.client.get_oauth_session(params["session_token"])
            logger.info("Five Paisa initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Five Paisa: ", exc_info = e)
            raise e
        
        # Variables
        start_time = str(params["start_hour"]) + ":" + str(params["start_minute"])
        end_time = str(params["end_hour"]) + ":" + str(params["end_minute"])
        self.TIME_1 = datetime.strptime(start_time, "%H:%M").time()
        self.TIME_2 = datetime.strptime(end_time, "%H:%M").time()
        self.EXPIRY = params["expiry"]
        self.FUT_EXPIRY=params["fut_expiry"]
        self.QTY = params["qty"]
        self.csv_file = params["csv_file"]
        self.temp_file="open_position_10_Lag_paper.csv"
        self.PROFIT_CUT_QTY=150
        self.ROUND_OFF=50
        self.max_trades=params["max_position"]
        # self.output_file="straddle_result.csv"

        self.expiry = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry.strftime('%Y-%m-%d') 
        self.expiry2 = self.expiry.strftime('%d-%b-%Y') 

        self.live_ltp = {"CE": None, "PE": None}
        self.TOKENS = {}
        self.entry_data = None
        self.lock = threading.Lock()
        self.sl_exit_time = None 
        self.keep_receiving = True

        # Greeks Parameters
        self.RATE_LIMIT_DELAY = 3
        self.RISK_FREE_RATE = 0.07
        self.DIVIDEND=1.4
        self.W_DELTA = 0.5  # Weight for Delta
        self.W_THETA = 0.3  # Weight for Theta
        self.W_VEGA = 0.2   # Weight for Vega
    
# <--------------------- Placing order ------------------->

    def place_order_bulk(self,side):
        try:
            print(f"Placing {side} Order")
            order_type = "S" if side == "sell" else "B"
            suffix = "SELL" if side == "sell" else "BUY"
            now = datetime.now()
            Rp=None
            Rc=None
            CE_order=self.client.place_order(OrderType=order_type,Exchange='N',ExchangeType='D', ScripCode = self.TOKENS["CE"], Qty=self.QTY, Price=0)
            print(f"CE order details : {CE_order}")
            PE_order=self.client.place_order(OrderType=order_type,Exchange='N',ExchangeType='D', ScripCode = self.TOKENS["PE"], Qty=self.QTY, Price=0)
            print(f"PE order details : {PE_order}")
            time.sleep(2)
            orders = self.client.order_book()
            for order in orders:
                if order['BrokerOrderId'] == CE_order['BrokerOrderID'] and order['ScripCode']==CE_order['ScripCode']:
                    Rc= order['AveragePrice']
                elif order['BrokerOrderId'] == PE_order['BrokerOrderID'] and order['ScripCode']==PE_order['ScripCode']:
                    Rp= order['AveragePrice']
            print(f"[ORDER] {side.upper()} orders placed for CE & PE")
            resp=float(Rp+Rc)
            return resp, Rc, Rp
        except Exception as e:
            logger.error(f"Error placing order for {order_type}: ", exc_info = e)
            raise
    
# <--------------------- Squaring Off order ------------------->
    def square_off(self,Q):
        print("Placing Buy Order")
        Rp=None
        Rc=None
        positions = self.client.positions()
        for pos in positions:
            if pos['NetQty'] != 0 and pos['ScripCode']==self.TOKENS["CE"]:
                CE_order=self.client.place_order(OrderType='B',Exchange='N',ExchangeType='D', ScripCode = self.TOKENS["CE"], Qty=Q, Price=0)
                print(f"CE order details : {CE_order}")

            if pos['NetQty'] != 0 and pos['ScripCode']== self.TOKENS["PE"]:   
                PE_order=self.client.place_order(OrderType='B',Exchange='N',ExchangeType='D', ScripCode = self.TOKENS["PE"], Qty=Q, Price=0)
                print(f"PE order details : {PE_order}")
        time.sleep(2)
        orders = self.client.order_book()
        for order in orders:
            if order['BrokerOrderId'] == CE_order['BrokerOrderID'] and order['ScripCode']==CE_order['ScripCode']:
                Rc= order['AveragePrice']
            elif order['BrokerOrderId'] == PE_order['BrokerOrderID'] and order['ScripCode']==PE_order['ScripCode']:
                Rp= order['AveragePrice']
        print(f"[ORDER] Buy orders placed for CE & PE")
        resp=float(Rc+Rp)
        return resp
   


# <---------------------------------- Data For Greeks calculation ------------------------------>


    # Black-Scholes Greeks Calculation

    def black_scholes_call(self,S, K, T, r, q, sigma):
        d1 = (log(S / K) + (r - q + sigma**2 / 2) * T) / (sigma * sqrt(T))
        d2 = d1 - sigma * sqrt(T)
        call_price = S * exp(-q * T) * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)
        return call_price

    def black_scholes_put(self,S, K, T, r, q, sigma):
        d1 = (log(S / K) + (r - q + sigma**2 / 2) * T) / (sigma * sqrt(T))
        d2 = d1 - sigma * sqrt(T)
        put_price = K * exp(-r * T) * norm.cdf(-d2) - S * exp(-q * T) * norm.cdf(-d1)
        return put_price

    def calculate_iv(self,option_price, S, K, T, r, q, option_type):
        option_func=None
        option_func = lambda sigma: self.black_scholes_call(S, K, T, r, q, sigma) - option_price 
        a = 0.01  
        b = 2.0   
    
        iv = brentq(option_func, 0.01, 2)  
        return iv
    
    def black_scholes_greeks(self,spot, strike, time_to_expiry, volatility, risk_free_rate, dividend,option_type="call"):
        """Calculate Delta, Theta, and Vega using Black-Scholes model."""
        if time_to_expiry < 0 or volatility <= 0:
            return 0, 0, 0
        
        d1 = (np.log(spot / strike) + (risk_free_rate -dividend + 0.5 * volatility**2) * time_to_expiry) / (volatility * np.sqrt(time_to_expiry))
        d2 = d1 - volatility * np.sqrt(time_to_expiry)
        nd1=math.exp(-(d1**2)/ 2) / (math.sqrt(2 * math.pi))
        nd2=0.5 * (1 + math.erf(d2/ math.sqrt(2)))
    
        if option_type == "call":
            delta = nd1
            theta = -((spot * volatility * nd1 / (2 * math.sqrt(time_to_expiry)) -  risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * nd2))/365
        else:  # put
            delta = nd1 - 1
            theta = -(spot * nd1 * volatility / (2 * math.sqrt(time_to_expiry)) + 
                    risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * (1-nd2)) / 365
        
        vega = (spot * nd1 * math.sqrt(time_to_expiry) )/ 100  ## Vega per 1% IV change
        
        return delta, theta, vega

    # Calculating Greeks

    def calculate_greeks_sl(self,ent_time,exi_time, nifty, strike, time_to_expiry, volatility, atr,option_type):
        delta, theta, vega = self.black_scholes_greeks(nifty, strike, time_to_expiry, volatility, self.RISK_FREE_RATE, self.DIVIDEND,option_type)
        print(f"Delta : {delta}  ||  Theta :  {theta}  ||  Vega:  {vega}")
        # Time factor: remaining minutes / total trading minutes (365 minutes from 9:15 to 15:20)
        total_minutes = 369
        minutes_left = (exi_time.hour * 60 + exi_time.minute) - (ent_time.hour * 60 + ent_time.minute)
        time_factor = minutes_left / total_minutes if minutes_left > 0 else 0.01
            
        # Use ATR as a proxy for SpotMove and VolFactor
        spot_move = atr
        vol_factor = atr / nifty
            
        # Greeks-based SL offset
        sl_offset = (self.W_DELTA * abs(delta) * spot_move + 
                    self.W_THETA * abs(theta) * time_factor + 
                    self.W_VEGA * vega * vol_factor)
        # sl=min(max((sl_offset*6),20),45)
        sl=min((sl_offset*2.5),25)
        # return (sl)*75
        return (sl)

# <---------------------------------- Data For Greeks calculation ------------------------------>

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
                row["entry"] = float(row["entry"])
                row["current"] = float(row["current"])
                row["cut"] = int(row["cut"])
                row["Qty_B"] = int(row["Qty_B"])
                row["max_pnl"] = float(row["max_pnl"])
                row["sl"] = float(row["sl"])
                row["tsl"] = float(row["tsl"])
                row["ce_token"] = int(row["ce_token"])
                row["pe_token"] = int(row["pe_token"])
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

# <---------------------------------- For Checking if Entry is Already exit ------------------------------>

    # --- Utility Functions ---
    def round_to_nearest_50(self,x):
        return int(round(x / 50.0)) * 50
    
    # Creating csv
    def write_to_csv(self, data):
        headers = ['Date', 'Strike', 'Entry Time', 'Entry premium', 
                'Exit Time', 'Exit premium', 'PnL', 'Quantity', 'Total']
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

    # Checking entry Condition
    def check_atr_adx(self):
        try:
            now = datetime.now()
            x=t(14, 10)
            hist = self.client.historical_data("N", "C", 999920000, "1m", (now - timedelta(days=4)).date(), now.date())
            df = pd.DataFrame(hist)
            df["datetime"] = pd.to_datetime(df["Datetime"])
            df.set_index("datetime", inplace=True)
            df["ADX"] = ta.adx(df["High"], df["Low"], df["Close"])["ADX_14"]
            df["ATR"] = ta.atr(df["High"], df["Low"], df["Close"])
            df["log_ret"] = np.log(df["Close"] / df["Close"].shift(1))
            df["RV"] = df["log_ret"].rolling(window=10).std()* np.sqrt(252*390)
            df.dropna(inplace=True)
        
            if len(df) < 20:
                return None
            last = df.iloc[-10]
            prev10 = df.iloc[-20]
            atr_down = prev10["ATR"] > last["ATR"]
            adx_down = prev10["ADX"] > last["ADX"]
            rv_down= prev10["RV"] > last["RV"]
            print(f"[DEBUG] ATR: {prev10['ATR']:.2f} > {last['ATR']:.2f} => {atr_down}")
            print(f"[DEBUG] ADX: {prev10['ADX']:.2f} > {last['ADX']:.2f} => {adx_down}")
            print(f"[DEBUG] RV: {prev10['RV']:.2f} > {last['RV']:.2f} => {rv_down}")
                      
            if (now.time()<x) and rv_down and atr_down and adx_down:
                logger.info("Condition Matched")
                return last["Close"]
            else:
                return None
        except Exception as e:
            logger.error(f"Error to calculate Atm: ", exc_info = e)
            print(f"[ERROR] ATR/ADX: {e}")
            return None

    
    # Checking GAP condition 
    def check_GAP(self):
        try:
            now = datetime.now()
            hist = self.client.historical_data("N", "C", 999920000, "1d", (now - timedelta(days=10)).date(), now.date())
            df = pd.DataFrame(hist)
            df["datetime"] = pd.to_datetime(df["Datetime"])
            df.set_index("datetime", inplace=True)
            if len(df) < 3:
                return None
            today = df.iloc[-1]['Open']
            previous = df.iloc[-2]['Close']
            gap= ((today-previous)/previous)*100
            GAP_day=gap<=1.25
            print(f"[DEBUG] GAP:{gap:.2f} || Straddle Day => {GAP_day}")
            return GAP_day
        except Exception as e:
            print(f"[ERROR] GAP checking: {e}")
            return None 
    
    # Checking Closes ATM Premium Matching function 

    def check_nearest_ltp(self,atm,Cc,Cu,Cd,Pc,Pu,Pd):
        req_list = [
        {"Exch":"N","ExchType":"D","ScripCode":Cc},
        {"Exch":"N","ExchType":"D","ScripCode":Cu},
        {"Exch":"N","ExchType":"D","ScripCode":Cd},
        {"Exch":"N","ExchType":"D","ScripCode":Pc},
        {"Exch":"N","ExchType":"D","ScripCode":Pu},
        {"Exch":"N","ExchType":"D","ScripCode":Pd}
        ]

        ltps =self.client.fetch_market_feed_scrip(req_list)
        # print(ltps)
        cc = ltps["Data"][0]["LastRate"]
        cu= ltps["Data"][1]["LastRate"]
        cd= ltps["Data"][2]["LastRate"]
        pc= ltps["Data"][3]["LastRate"]
        pu= ltps["Data"][4]["LastRate"]
        pd= ltps["Data"][5]["LastRate"]

        print(f"Call: | Call Curr: {Cc}-{cc} | Call Up: {Cu} -{cu}| Put Down: {Cd}-{cd}")
        print(f"Put:  | Put Curr: {Pc}-{pc} | Put Up: {Pu}-{pu} | Put Down: {Pd}-{pd}")
        m1=abs(cc-pc)
        m2=abs(cd-pd)
        m3=abs(cu-pu)
        m=min(m1,m2,m3)
        print(f" U:{m2} | D:{m3} | C:{m1} | M:{m}")
        if m==m1 and m<=(0.2*max(cc,pc)):
            return Cc,Pc,atm
        elif m==m2 and m<=(0.2*max(cd,pd)):
            return Cd,Pd,atm-50
        elif m==m3 and m<=(0.2*max(cu,pu)):
            return Cu,Pu,atm+50
        else:
            print("Failed to do premium Matching")
            return None,None,None
        
# <---------------------------------- For Checking if Entry is Already exit ------------------------------>
    def on_message(self,ws, message):
        try:
            if not self.keep_receiving:
                return
            data = json.loads(message)
            if isinstance(data, list):
                ticks = data
            elif isinstance(data, dict) and "MarketFeedData" in data:
                ticks = data["MarketFeedData"]
            else:
                print(f"[WARNING] Unexpected data format: {data}")
                return

            for tick in ticks:
                token = tick.get("Token")           
                ltp = tick.get("LastRate")
                open_ = tick.get("OpenRate")
                high = tick.get("High")
                low = tick.get("Low")
                close = tick.get("PClose")
                volume = tick.get("TotalQty")

                with self.lock:
                    if token == self.TOKENS.get("CE"):
                        self.live_ltp["CE"] = ltp
                    elif token == self.TOKENS.get("PE"):
                        self.live_ltp["PE"] = ltp

                print(f"[TICK] Token: {token} | LTP: {ltp} | O: {open_} H: {high} L: {low} C: {close} V: {volume}")

        except Exception as e:
            logger.error(f"Failed to calculat Live ltp: ", exc_info = e)
            print(f"[ERROR] in on_message: {e}")

    def start_feed(self):
        try:
            self.keep_receiving=True
            req = self.client.Request_Feed("mf", "s", [
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["CE"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["PE"]}
            ])
            self.client.connect(req)
            threading.Thread(target=self.client.receive_data, args=(self.on_message,), daemon=True).start()
            print(f"[INFO] Streaming Started for CE: {self.TOKENS['CE']}, PE: {self.TOKENS['PE']}")
        except Exception as e:
            logger.error(f"Failed to start FEED: ", exc_info = e)
            print(f"[ERROR] in start_feed: {e}")

    def stop_feed(self):
        try:
            self.keep_receiving=False
            req = self.client.Request_Feed("mf", "u", [  
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["CE"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["PE"]}
            ])
            self.client.connect(req)
            print(f"[INFO] Streaming Stopped for CE: {self.TOKENS['CE']}, PE: {self.TOKENS['PE']}")
        except Exception as e:
            logger.error(f"Failed to stop FEED: ", exc_info = e)
            print(f"[ERROR] in stop_feed: {e}")

# <---------------------------------- For Checking if Entry is Already exit ------------------------------> 
     
    def get_scrip_code(self, strike, opt_type):
        try:
            strike = str(strike)
            df = self.client.query_scrips("N", "D", "NIFTY", strike, opt_type, self.expiry1)
            if not df.empty:
                code = int(df["ScripCode"].iloc[0])
                print(f"[DEBUG] ScripCode for {strike} {opt_type}: {code}")
                return code
            return None
        except Exception as e:
            logger.error(f"Failed to Fetch Scrip Code ", exc_info = e)
        
# <------------Main loop------>
    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )

            try:
                print("[START] Waiting for signal...")
                GAP=self.check_GAP()
                # <------------- Fetchinh Previous Entry If any --------------->
                self.entry_data = self.load_entry_data_from_csv()
                if self.entry_data:
                    self.TOKENS["CE"] = self.entry_data["ce_token"]
                    self.TOKENS["PE"] = self.entry_data["pe_token"]
                    self.max_trades=self.entry_data["trade_rem"]
                    print(f"[RESUME] Loaded open position from CSV. CE: {self.TOKENS['CE']}, PE: {self.TOKENS['PE']}")
                    logger.info("[RESUME] Loaded open position from CSV")

                    self.start_feed()

                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()
                        # ENTRY CONDITION
                        if self.TIME_1 < now.time() < self.TIME_2 and GAP and self.max_trades>0 and self.entry_data is None and now.second == 0:
                            #  Skip entry for 60 seconds after SL exit
                            if self.sl_exit_time and (now - self.sl_exit_time).seconds < 60:
                                continue
                            print(f"Checking ATR/ADX conditions for Time {now.time()}")
                            spot = self.check_atr_adx()
                            if spot is not None:
                                atm = self.round_to_nearest_50(spot)
                                print(f"spot: {spot} || atm : {atm}")
                                print("Fetching tokens...")
                                Cc = self.get_scrip_code(atm, "CE")                    # ATM CE
                                time.sleep(0.2)
                                Cu = self.get_scrip_code(atm + self.ROUND_OFF, "CE")   # One strike above
                                time.sleep(0.2)
                                Cd = self.get_scrip_code(atm - self.ROUND_OFF, "CE")   # One strike below
                                time.sleep(0.2)
                                Pc = self.get_scrip_code(atm, "PE")                    # ATM PE
                                time.sleep(0.2)
                                Pu = self.get_scrip_code(atm + self.ROUND_OFF, "PE")   # One strike above
                                time.sleep(0.2)
                                Pd = self.get_scrip_code(atm - self.ROUND_OFF, "PE")   # One strike below

                                print(f"Call: | Call Curr: {Cc} | Call Up: {Cu} | Put Down: {Cd}")
                                print(f"Put:  | Put Curr: {Pc} | Put Up: {Pu} | Put Down: {Pd}")
                                C,P,atm =self.check_nearest_ltp(atm,Cc,Cu,Cd,Pc,Pu,Pd)
                                self.TOKENS["CE"] = C
                                self.TOKENS["PE"] = P

                                if not self.TOKENS["CE"] or not self.TOKENS["PE"]:
                                    print("[ABORT] Invalid ScripCode. Skipping.")
                                    continue

                                self.start_feed()
                                time.sleep(1)

                                if self.live_ltp["CE"] and self.live_ltp["PE"]:
                                    entry_time = now
                                    # entry_premium ,ce_entry,pe_entry = self.place_order_bulk("sell") # Selling both PE AND CE
                                    ce_entry=self.live_ltp["CE"]
                                    pe_entry=self.live_ltp["PE"]
                                    entry_premium=ce_entry+pe_entry
                                    print(f"Ent CE: {ce_entry} | Ent PE: {pe_entry} | Ent Tot: {entry_premium}")
                                    exit_time = datetime.combine(now.date(), self.TIME_2)
                                    expiry_dt = self.expiry + timedelta(hours=15, minutes=30)
                                    tte = (expiry_dt - now).total_seconds() / (365 * 24 * 60 * 60)
                                    print("<---++++++-->")
                                    print(f"Time to expiry {tte}")
                                    print("<---++++++-->")
                                    # --- Fetch historical candles and calculate ATR  ---
                                    start_hist = now - timedelta(minutes=30)
                                    end_hist = now
                                    hist_ce = self.client.historical_data('N', 'D', self.TOKENS['CE'], '1m', start_hist.date(), end_hist.date())
                                    hist_pe = self.client.historical_data('N', 'D', self.TOKENS['PE'], '1m', start_hist.date(), end_hist.date())
                                    df_ce = pd.DataFrame(hist_ce)
                                    df_pe = pd.DataFrame(hist_pe)
                                    df_ce['datetime'] = pd.to_datetime(df_ce['Datetime'])
                                    df_pe['datetime'] = pd.to_datetime(df_pe['Datetime'])
                                    df_ce.set_index('datetime', inplace=True)
                                    df_pe.set_index('datetime', inplace=True)
                                    df_ce['ATR'] = ta.atr(df_ce['High'], df_ce['Low'], df_ce['Close'], length=14)
                                    df_pe['ATR'] = ta.atr(df_pe['High'], df_pe['Low'], df_pe['Close'], length=14)
                                    atr_ce = df_ce.iloc[-1]['ATR']
                                    atr_pe = df_pe.iloc[-1]['ATR']
                                    # vol_ce = atr_ce / spot
                                    # vol_pe = atr_pe / spot
                                    vol_ce = self.calculate_iv(ce_entry, spot, atm, tte, self.RISK_FREE_RATE, self.DIVIDEND, 'call')
                                    vol_pe = self.calculate_iv(pe_entry, spot, atm, tte, self.RISK_FREE_RATE, self.DIVIDEND, 'put')
                                    print(f"ATR_CE: {atr_ce}, Volatility_CE: {vol_ce}")
                                    print(f"ATR_PE: {atr_pe}, Volatility_PE: {vol_pe}")
                                    sl_ce = self.calculate_greeks_sl(entry_time, exit_time, spot, atm, tte, vol_ce, atr_ce, "call")
                                    sl_pe = self.calculate_greeks_sl(entry_time, exit_time, spot, atm, tte, vol_pe, atr_pe, "put")
                                    STOP_LOSS = -((sl_ce + sl_pe) / 2)
                                    print("--------------||------------")
                                    print(f"stop_loss : {STOP_LOSS}")
                                    print("--------------||------------")
                                    self.max_trades=self.max_trades-1 # Adjusting maxium trades
                                    # self.place_order_bulk("sell") # Selling both PE AND CE
                                    self.entry_data = {
                                        "trade_rem":self.max_trades,
                                        "time": entry_time,
                                        "atm": atm,
                                        "entry": entry_premium,
                                        "current":0,
                                        "cut":0,
                                        "Qty_B":0,
                                        "max_pnl": 0,
                                        "sl":STOP_LOSS,
                                        "tsl": STOP_LOSS,
                                        "ce_token": self.TOKENS["CE"],
                                        "pe_token": self.TOKENS["PE"],
                                        "pnl":0
                                    }
                                    self.save_entry_data_to_csv(self.entry_data)
                                    self.sl_exit_time = None  #  Reset SL flag
                                    print(f"[ENTRY] Time: {now.time()} ATM: {atm} Entry Premium: {self.entry_data['entry']:.2f}")
                                    logger.info(f"[ENTRY] Time: {now.time()} || ATM: {atm} || Entry Premium: {self.entry_data['entry']:.2f} || Quantity: {self.QTY} || Stop Loss: {self.entry_data['tsl']}")


                                
                        # TRAIL STOP LOSS CHECK
                        elif self.entry_data and self.TIME_1 < now.time() < self.TIME_2:
                            if self.live_ltp["CE"] and self.live_ltp["PE"]:
                                current_premium = self.live_ltp["CE"] + self.live_ltp["PE"]
                                pnl = (self.entry_data["entry"] - current_premium)
                                self.entry_data["current"]=current_premium
                                if pnl > self.entry_data["max_pnl"]:
                                    self.entry_data["max_pnl"] = pnl
                                    self.entry_data["tsl"] = pnl + self.entry_data["sl"]
                                
                                self.save_entry_data_to_csv(self.entry_data)
                                
                                if (pnl>15 and self.entry_data["cut"]==0) or (pnl>30 and self.entry_data["cut"]==1):
                                    # current_premium=self.square_off(self.PROFIT_CUT_QTY) # squaring off
                                    current_premium = self.live_ltp["CE"] + self.live_ltp["PE"]
                                    pnl = (self.entry_data["entry"] - current_premium)*self.PROFIT_CUT_QTY
                                    self.entry_data["pnl"]=pnl+self.entry_data["pnl"]
                                    self.entry_data["Qty_B"]=self.entry_data["Qty_B"]+self.PROFIT_CUT_QTY
                                    self.entry_data["cut"]=self.entry_data["cut"]+1
                                    self.save_entry_data_to_csv(self.entry_data)
                                    t=now.strftime("%H:%M:%S")
                                    logger.info(f"[PARTIAL BOOKING] Time: {t} || Exit Prem: {current_premium:.2f} || PnL: ₹{pnl:.2f}")

                                if pnl <= self.entry_data["tsl"]:
                                    Q=self.QTY-self.entry_data["Qty_B"]
                                    # current_premium=self.square_off(Q) # squaring off 
                                    current_premium = self.live_ltp["CE"] + self.live_ltp["PE"]
                                    pnl = (((self.entry_data["entry"] - current_premium)*Q)+self.entry_data["pnl"])/self.QTY
                                    Total=(pnl) * self.QTY
                                    self.stop_feed()
                                    print("--------------||------------")
                                    print(f"[SL HIT] PnL: ₹{pnl:.2f}")
                                    print("--------------||------------")
                                    t=now.strftime("%H:%M:%S") #extra for log
                                    e=self.entry_data["entry"]
                                    logger.info(f"[SL HIT] Time: {t} || Exit Prem: {current_premium:.2f} || PnL: {pnl:.2f} || Total: ₹{Total}")
                                    self.write_to_csv([self.entry_data["time"].strftime("%Y-%m-%d"), self.entry_data["atm"], self.entry_data["time"].strftime("%H:%M:%S"),f"{e:.2f}", now.strftime("%H:%M:%S"),f"{current_premium:.2f}",f"{pnl:.2f}",self.QTY, f"{Total:.2f}"])
                                    self.entry_data = None
                                    self.clear_position_state()
                                    self.TOKENS.clear()
                                    self.sl_exit_time = now  #  Mark time of SL exit

                        # TIME-BASED EXIT
                        elif self.entry_data and now.time() >= self.TIME_2 and now.second == 0:
                            if self.live_ltp["CE"] and self.live_ltp["PE"]:
                                Q=self.QTY-self.entry_data["Qty_B"]
                                # current_premium=self.square_off(Q) # squaring off
                                current_premium = self.live_ltp["CE"] + self.live_ltp["PE"]
                                pnl = (((self.entry_data["entry"] - current_premium)*Q)+self.entry_data["pnl"])/self.QTY
                                Total = (pnl) * self.QTY
                                self.stop_feed()
                                print(f"[TIME EXIT] PnL: ₹{pnl:.2f}")
                                t=now.strftime("%H:%M:%S") #extra for log
                                e=self.entry_data["entry"]
                                logger.info(f"[TIME EXIT] Time: {t} || Exit Prem: {current_premium:.2f} || PnL: {pnl:.2f} || Total: ₹{Total}")
                                self.write_to_csv([self.entry_data["time"].strftime("%Y-%m-%d"), self.entry_data["atm"], self.entry_data["time"].strftime("%H:%M:%S"),f"{e:.2f}", now.strftime("%H:%M:%S"),f"{current_premium:.2f}",f"{pnl:.2f}",self.QTY, f"{Total:.2f}"])
                                self.entry_data = None
                                self.clear_position_state()
                                self.TOKENS.clear()
                                self.sl_exit_time = None
                            print("[INFO] Market close time reached. Exiting strategy.")
                            break

                        time.sleep(1)                   

       
                    except Exception as e:
                        logger.error(f"Main loop error: ", exc_info = e)
                        print(f"Main loop error: {str(e)}")
                        time.sleep(60)

            
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                self.stop_feed() # change for checking if feed get stop 
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e



