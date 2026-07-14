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

logger = create_logger(__name__, 'trading_straddle_strategy_Old_PaperTrade.log')

class StrategyStraddleOldPaperFivePaisa(BaseStrategy):
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
        self.temp_file="open_position_xx.csv"
        self.PROFIT_CUT_QTY=150
        self.ROUND_OFF=50
        self.max_trades=params["max_position"]
        self.greeks="greek.csv"

        self.expiry = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry.strftime('%Y-%m-%d') 
        self.expiry2 = self.expiry.strftime('%d-%b-%Y') 

        self.live_ltp = {"CE": None, "PE": None,"CE_UP": None, "PE_UP": None, "CE_DOWN": None, "PE_DOWN": None}
        self.TOKENS = {}
        self.entry_data = None
        self.lock = threading.Lock()
        self.sl_exit_time = None 
        self.keep_receiving = True
        self.spot_fetched=False

        # Greeks Parameters
        self.RATE_LIMIT_DELAY = 3
        self.RISK_FREE_RATE = 0.07
        self.DIVIDEND=1.4
        self.W_DELTA = 0.5  # Weight for Delta
        self.W_THETA = 0.3  # Weight for Theta
        self.W_VEGA = 0.2   # Weight for Vega


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
            gamma= delta/(spot*volatility*math.sqrt(time_to_expiry))

        else:  # put
            delta = nd1 - 1
            theta = -(spot * nd1 * volatility / (2 * math.sqrt(time_to_expiry)) + 
                    risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * (1-nd2)) / 365
            gamma= delta/(spot*volatility*math.sqrt(time_to_expiry))

        vega = (spot * nd1 * math.sqrt(time_to_expiry) )/ 100  ## Vega per 1% IV change
        
        return delta, theta, vega,gamma

    # Calculating Greeks

    def calculate_greeks_sl(self,ent_time,exi_time, nifty, strike, time_to_expiry, volatility, atr,option_type):
        delta, theta, vega,gamma = self.black_scholes_greeks(nifty, strike, time_to_expiry, volatility, self.RISK_FREE_RATE, self.DIVIDEND,option_type)
        print(f"Delta : {delta}  ||  Theta :  {theta}  ||  Vega:  {vega } || Gamma : {gamma}")
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
    
    # Calculating Greeks

    def calculate_greeks_sl2(self,ent_time,exi_time, nifty, strike, time_to_expiry, volatility, atr,option_type):
        delta, theta, vega,gamma = self.black_scholes_greeks(nifty, strike, time_to_expiry, volatility, self.RISK_FREE_RATE, self.DIVIDEND,option_type)
        print(f"Delta : {delta}  ||  Theta :  {theta}  ||  Vega:  {vega} || Gamma : {gamma}")
        return delta,theta,vega,gamma

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
                row["spot"] = float(row["spot"])
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

    def log_greeks(self, filename ,data):
        headers = ['Time', 'Strike', 'Delta_CE','Theta_CE','Vega_CE','Gamma_CE',
                   'Delta_PE','Theta_PE','Vega_PE','Gamma_PE']
        try:
            try:
                with open(filename, 'x', newline='') as file: #change krna hai output file ko csv file mai
                    writer = csv.writer(file)
                    writer.writerow(headers)
                    print("Created new CSV file")
            except FileExistsError:
                pass
            with open(filename, 'a', newline='') as file: #change krna hai output file ko csv file mai
                writer = csv.writer(file)
                writer.writerow(data)
                print(f"Trade data written to CSV: {data}")
        except Exception as e:
            logger.error(f"Error writing to CSV: ", exc_info = e)
            print(f"Error writing to CSV: {str(e)}")
      


        
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
                    elif token == self.TOKENS.get("CE_UP"):
                        self.live_ltp["CE_UP"] = ltp
                    elif token == self.TOKENS.get("PE_UP"):
                        self.live_ltp["PE_UP"] = ltp
                    elif token == self.TOKENS.get("CE_DOWN"):
                        self.live_ltp["CE_DOWN"] = ltp
                    elif token == self.TOKENS.get("PE_DOWN"):
                        self.live_ltp["PE_DOWN"] = ltp

                print(f"[TICK] Token: {token} | LTP: {ltp} | O: {open_} H: {high} L: {low} C: {close} V: {volume}")

        except Exception as e:
            logger.error(f"Failed to calculat Live ltp: ", exc_info = e)
            print(f"[ERROR] in on_message: {e}")

    def start_feed(self):
        try:
            self.keep_receiving=True
            req = self.client.Request_Feed("mf", "s", [
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["CE"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["PE"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["CE_UP"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["PE_UP"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["CE_DOWN"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["PE_DOWN"]}
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
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["PE"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["CE_UP"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["PE_UP"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["CE_DOWN"]},
                {"Exch": "N", "ExchType": "D", "ScripCode": self.TOKENS["PE_DOWN"]}
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
                logger.info("[START] Waiting for signal...")
               
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()
                        # ENTRY CONDITION
                        if self.TIME_1 < now.time() < self.TIME_2  and now.second % 5==0:
                            spot=self.max_trades
                            atm=self.round_to_nearest_50(spot)
                            if spot is not None and not self.spot_fetched:
                                # atm = self.round_to_nearest_50(spot)
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
                                logger.info(f"Call: | Call Curr: {Cc} | Call Up: {Cu} | Put Down: {Cd}")
                                logger.info(f"Call: | Call Curr: {Pc} | Call Up: {Pu} | Put Down: {Pd}")
                                
                                self.TOKENS["CE"] = Cc
                                self.TOKENS["PE"] = Pc
                                self.TOKENS["CE_UP"] = Cu
                                self.TOKENS["PE_UP"] = Pu
                                self.TOKENS["CE_DOWN"] = Cd
                                self.TOKENS["PE_DOWN"] = Pd
                                

                                if not self.TOKENS["CE"] or not self.TOKENS["PE"] or not self.TOKENS["CE_UP"] or not self.TOKENS["PE_UP"] or not self.TOKENS["CE_DOWN"] or not self.TOKENS["PE_DOWN"]:
                                    print("[ABORT] Invalid ScripCode. Skipping.")
                                    continue
                                self.spot_fetched=True
                                self.start_feed()
                                time.sleep(1)

                            if self.spot_fetched and self.live_ltp["CE"] and self.live_ltp["PE"] and self.live_ltp["CE_UP"] and self.live_ltp["PE_UP"] and self.live_ltp["CE_DOWN"] and self.live_ltp["PE_DOWN"]:
                                ce_entry=self.live_ltp["CE"]
                                pe_entry=self.live_ltp["PE"]
                                ce_entry_up=self.live_ltp["CE_UP"]
                                pe_entry_up=self.live_ltp["PE_UP"]
                                ce_entry_down=self.live_ltp["CE_DOWN"]
                                pe_entry_down=self.live_ltp["PE_DOWN"]
                                
                                entry_time=now
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

                                hist_ce_up = self.client.historical_data('N', 'D', self.TOKENS['CE_UP'], '1m', start_hist.date(), end_hist.date())
                                hist_pe_up = self.client.historical_data('N', 'D', self.TOKENS['PE_UP'], '1m', start_hist.date(), end_hist.date())
                                df_ce_up = pd.DataFrame(hist_ce_up)
                                df_pe_up = pd.DataFrame(hist_pe_up)
                                df_ce_up['datetime'] = pd.to_datetime(df_ce_up['Datetime'])
                                df_pe_up['datetime'] = pd.to_datetime(df_pe_up['Datetime'])
                                df_ce_up.set_index('datetime', inplace=True)
                                df_pe_up.set_index('datetime', inplace=True)
                                df_ce_up['ATR'] = ta.atr(df_ce_up['High'], df_ce_up['Low'], df_ce_up['Close'], length=14)
                                df_pe_up['ATR'] = ta.atr(df_pe_up['High'], df_pe_up['Low'], df_pe_up['Close'], length=14)

                                hist_ce_down = self.client.historical_data('N', 'D', self.TOKENS['CE_DOWN'], '1m', start_hist.date(), end_hist.date())
                                hist_pe_down = self.client.historical_data('N', 'D', self.TOKENS['PE_DOWN'], '1m', start_hist.date(), end_hist.date())
                                df_ce_down = pd.DataFrame(hist_ce_down)
                                df_pe_down = pd.DataFrame(hist_pe_down)
                                df_ce_down['datetime'] = pd.to_datetime(df_ce_down['Datetime'])
                                df_pe_down['datetime'] = pd.to_datetime(df_pe_down['Datetime'])
                                df_ce_down.set_index('datetime', inplace=True)
                                df_pe_down.set_index('datetime', inplace=True)
                                df_ce_down['ATR'] = ta.atr(df_ce_down['High'], df_ce_down['Low'], df_ce_down['Close'], length=14)
                                df_pe_down['ATR'] = ta.atr(df_pe_down['High'], df_pe_down['Low'], df_pe_down['Close'], length=14)
                                atr_ce = df_ce.iloc[-1]['ATR']
                                atr_pe = df_pe.iloc[-1]['ATR']
                                atr_ce_up = df_ce_up.iloc[-1]['ATR']
                                atr_pe_up = df_pe_up.iloc[-1]['ATR']
                                atr_ce_down = df_ce_down.iloc[-1]['ATR']
                                atr_pe_down = df_pe_down.iloc[-1]['ATR']

                                print(f"spot:{spot}")
                                vol_ce = self.calculate_iv(ce_entry, spot, atm, tte, self.RISK_FREE_RATE, self.DIVIDEND, 'call')
                                vol_pe = self.calculate_iv(pe_entry, spot, atm, tte, self.RISK_FREE_RATE, self.DIVIDEND, 'put')
                                vol_ce_up = self.calculate_iv(ce_entry_up, spot, atm+50, tte, self.RISK_FREE_RATE, self.DIVIDEND, 'call')
                                vol_pe_up = self.calculate_iv(pe_entry_up, spot, atm+50, tte, self.RISK_FREE_RATE, self.DIVIDEND, 'put')
                                vol_ce_down = self.calculate_iv(ce_entry_down, spot, atm-50, tte, self.RISK_FREE_RATE, self.DIVIDEND, 'call')
                                vol_pe_down = self.calculate_iv(pe_entry_down, spot, atm-50, tte, self.RISK_FREE_RATE, self.DIVIDEND, 'put')
                                
                                delta_ce,theta_ce,vega_ce,gamma_ce= self.calculate_greeks_sl2(entry_time, exit_time, spot, atm, tte, vol_ce, atr_ce, "call")
                                delta_pe,theta_pe,vega_pe,gamma_pe = self.calculate_greeks_sl2(entry_time, exit_time, spot, atm, tte, vol_pe, atr_pe, "put")
                                delta_ce_up,theta_ce_up,vega_ce_up,gamma_ce_up= self.calculate_greeks_sl2(entry_time, exit_time, spot+50, atm+50, tte, vol_ce_up, atr_ce_up, "call")
                                delta_pe_up,theta_pe_up,vega_pe_up,gamma_pe_up = self.calculate_greeks_sl2(entry_time, exit_time, spot+50, atm+50, tte, vol_pe_up, atr_pe_up, "put")
                                delta_ce_down,theta_ce_down,vega_ce_down,gamma_ce_down= self.calculate_greeks_sl2(entry_time, exit_time, spot-50, atm-50, tte, vol_ce_down, atr_ce_down, "call")
                                delta_pe_down,theta_pe_down,vega_pe_down,gamma_pe_down = self.calculate_greeks_sl2(entry_time, exit_time, spot-50, atm-50, tte, vol_pe_down, atr_pe_down, "put")
                                self.log_greeks("atm.csv",[entry_time, atm, f"{delta_ce:.4f}", f"{theta_ce:.4f}", f"{vega_ce:.4f}", f"{gamma_ce:.4f}",f"{delta_pe:.4f}", f"{theta_pe:.4f}", f"{vega_pe:.4f}", f"{gamma_pe:.4f}"])
                                self.log_greeks("atm.csv_up.csv",[entry_time, atm+50, f"{delta_ce_up:.4f}", f"{theta_ce_up:.4f}", f"{vega_ce_up:.4f}", f"{gamma_ce_up:.4f}",f"{delta_pe_up:.4f}", f"{theta_pe_up:.4f}", f"{vega_pe_up:.4f}", f"{gamma_pe_up:.4f}"])
                                self.log_greeks("atm.csv_down.csv",[entry_time, atm-50, f"{delta_ce_down:.4f}", f"{theta_ce_down:.4f}", f"{vega_ce_down:.4f}", f"{gamma_ce_down:.4f}",f"{delta_pe_down:.4f}", f"{theta_pe_down:.4f}", f"{vega_pe_down:.4f}", f"{gamma_pe_down:.4f}"])
                                logger.info(f"Position added at {now.time()}")
                                    
                        elif now.time() >= self.TIME_2 and now.second == 0:   
                            self.TOKENS.clear()
                            self.sl_exit_time = None
                            self.spot_fetched=False
                            self.stop_feed()
                            print("[INFO] Market close time reached. Exiting strategy.")
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