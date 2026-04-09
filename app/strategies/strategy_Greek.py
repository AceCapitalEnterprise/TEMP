from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect 
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

logger = create_logger(__name__, 'Greek_Calculation.log')

class StrategyGreek(BaseStrategy):
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
        

        start_time = str(params["start_hour"]) + ":" + str(params["start_minute"])
        end_time = str(params["end_hour"]) + ":" + str(params["end_minute"])
        self.TIME_1 = datetime.strptime(start_time, "%H:%M").time()
        self.TIME_2 = datetime.strptime(end_time, "%H:%M").time()
        self.symbol= params["symbol"]
        self.interval= params["interval"]
        self.strike=int(params["strike"])
        self.EXPIRY = params["expiry"]
        self.expiry = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry.strftime('%d-%b-%Y') 
        self.csv_file = params["csv_file"]
        self.temp_file = f"Greeks_Calculation_{self.symbol}_{self.strike}.csv"
        self.start_date = datetime.strptime(
            params["first_day_of_month"], "%Y-%m-%d"
        ).date()

        self.end_date = datetime.strptime(
            params["last_day_of_month"], "%Y-%m-%d"
        ).date()

        # Greeks Parameters
        self.RATE_LIMIT_DELAY = 3
        self.RISK_FREE_RATE = 0.07
        self.DIVIDEND=0.012
        self.W_DELTA = 0.5  # Weight for Delta
        self.W_THETA = 0.3  # Weight for Theta
        self.W_VEGA = 0.2   # Weight for Vega

    # fetching symbol
    def fetch_stock_code(self,symbol):
        symbol=str(symbol)
        data=self.breeze.get_names(exchange_code = 'NSE',stock_code = symbol)
        code= data["isec_stock_code"]
        print(f"Code:{code}")
        return code
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
        try:
            option_func=None 
            if( option_type=="call"):
                option_func = lambda sigma: self.black_scholes_call(S, K, T, r, q, sigma) - option_price 
            elif( option_type=="put"):
                option_func = lambda sigma: self.black_scholes_put(S, K, T, r, q, sigma) - option_price 
            a = 0.01 
            b = 2.0 
            iv = brentq(option_func, 0.01, 2) 
            return iv 
        except Exception as e:
            return None
    
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
            vomma=0.01 * spot * math.sqrt(time_to_expiry) * d1* nd1
        else:  # put
            delta = nd1 - 1
            theta = -(spot * nd1 * volatility / (2 * math.sqrt(time_to_expiry)) + 
                    risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * (1-nd2)) / 365
            gamma= delta/(spot*volatility*math.sqrt(time_to_expiry)) 
            vomma=0.01 * spot * math.sqrt(time_to_expiry) * d1* nd1
        
        vega = (spot * nd1 * math.sqrt(time_to_expiry) )/ 100  ## Vega per 1% IV change
        
        return delta, theta, vega, gamma, vomma

    # Calculating Greeks 
    def calculate_greeks(self,nifty, strike, time_to_expiry, volatility, option_type): 
        delta, theta, vega ,gamma,vomma= self.black_scholes_greeks(nifty, strike, time_to_expiry, volatility, self.RISK_FREE_RATE, self.DIVIDEND,option_type) 
        print(f"Delta : {delta} || Theta : {theta} || Vega: {vega}|| Gamma: {gamma} || Vomma: {vomma}")
        return delta , theta , vega , gamma ,vomma
    
    # Creating Csv 
    def log_greeks(self,data): 
        headers = ['Time', 'Strike', 'Delta_CE','Theta_CE','Vega_CE','Gamma_CE', 'Vomma_CE','GT_CE','IV_CE', 'Delta_PE','Theta_PE','Vega_PE','Gamma_PE','Vomma_PE','GT_PE','IV_PE','GT_Combine'] 
        try: 
            try:
                with open(self.temp_file, 'x', newline='') as file: #change krna hai output file ko csv file mai 
                    writer = csv.writer(file) 
                    writer.writerow(headers) 
                    print("Created new CSV file") 
            except FileExistsError: pass 
            with open(self.temp_file, 'a', newline='') as file: #change krna hai output file ko csv file mai 
                writer = csv.writer(file) 
                writer.writerow(data) 
                print(f"Trade data written to CSV: {data}") 
        except Exception as e: 
            print(f"Error writing to CSV: {str(e)}") 
    
    # Fetching Option data 

    def generate_master_index(self,from_date, to_date,interval): 
        # Round from_date and to_date to minute 
        from_date = from_date.replace(second=0,microsecond=0) 
        to_date = to_date.replace(second=0,microsecond=0) 
        frequency= None
        if interval=="1minute" or interval=="5minute" or interval=="30minute":
            frequency= interval.replace("minute", "min")

        elif interval=="1second":
            frequency= "1s"

        elif interval=="1day":
            frequency= "1D"

        idx=pd.date_range(start=from_date, end=to_date, freq=frequency) 
        idx = idx[
            (idx.time >= datetime.strptime("09:15:00", "%H:%M:%S").time()) &
            (idx.time <= datetime.strptime("15:29:00", "%H:%M:%S").time())
        ]

        return idx
    
    def get_option_df(self,strike_price, right, from_date, to_date,interval,symbol): 
        data = self.breeze.get_historical_data_v2( interval=interval, 
                                            from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                                            to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"), 
                                            stock_code=symbol, 
                                            exchange_code="NFO" ,
                                            product_type="options",
                                            expiry_date=self.expiry, 
                                            right=right,
                                            strike_price=strike_price )
        if "Success" not in data or not data["Success"]: 
            raise ValueError(f"{right.upper()} data empty for {strike_price}") 
        df = pd.DataFrame(data["Success"]) 
        df["datetime"] = pd.to_datetime(df["datetime"]) 
        df.set_index("datetime", inplace=True) 
        df = df[~df.index.duplicated(keep='last')] 
        # . Reindex to master timeline 
        master_index = self.generate_master_index(from_date, to_date,interval) 
        df = df.reindex(master_index) 
        # Optional: forward fill OHLC values for missing bars 
        df = df.ffill()
        return df
    
    def get_nifty_data(self,from_date, to_date,interval,symbol): 
        data = self.breeze.get_historical_data_v2(interval=interval,
                                        from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                                        to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                                        stock_code=symbol,
                                        exchange_code="NSE",
                                        product_type="cash")
        df = pd.DataFrame(data["Success"]) 
        df["datetime"] = pd.to_datetime(df["datetime"]) 
        df.set_index("datetime", inplace=True) 
        df = df[~df.index.duplicated(keep='last')] 
        # . Reindex to master timeline 
        master_index = self.generate_master_index(from_date, to_date,interval) 
        df = df.reindex(master_index) 
        # Optional: forward fill OHLC values for missing bars 
        df = df.ffill()
        return df 
    
    # Deleting position
    def clear_position_state(self):
        try:
            if os.path.exists(self.temp_file):
                os.remove(self.temp_file)
        except Exception as e:
            logger.error(f"Failed to Delete CSV {e}")
            raise
    
    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                db_session.refresh(deployed_strategy)
                current_date = self.start_date
                try:
                    symbol=self.fetch_stock_code(self.symbol)
                    entry_time = datetime.combine(current_date, self.TIME_1) 
                    now = datetime.now().replace(second=0, microsecond=0)-timedelta(minutes=1)
                    et = datetime.combine(self.end_date,self.TIME_2) 
                    exit_time=min(now,et)
                    print(f"et:{entry_time} || ex: {exit_time}")
                    expiry_dt = self.expiry + timedelta(hours=15, minutes=30)
                    time_to_expiry = (expiry_dt - now).total_seconds() / (365 * 24 * 60 * 60)
                    start = self.TIME_1
                    finish = self.TIME_2
                    current_time=entry_time 
                    df_nifty=self.get_nifty_data(entry_time,exit_time,self.interval,symbol)
                    value = int(''.join(filter(str.isdigit, self.interval)))
                    print(value) 
                    # print("hi") 
                    print(time_to_expiry) 
                    df_ce=self.get_option_df(self.strike,"call",entry_time,exit_time,self.interval,symbol) 
                    df_ce['ATR'] = ta.atr(df_ce['high'], df_ce['low'], df_ce['close'],length=14) 
                    df_pe=self.get_option_df(self.strike,"put",entry_time,exit_time,self.interval,symbol) 
                    df_pe['ATR'] = ta.atr(df_pe['high'], df_pe['low'], df_pe['close'],length=14) 
                    df_combined = pd.DataFrame()
                    df_combined["spot"]=df_nifty["open"]
                    df_combined["ce"]=df_ce["open"] 
                    df_combined["pe"]=df_pe["open"] 
                    df_combined["prem"] = df_combined["ce"]+df_combined["pe"] 
                    if self.interval=="1minute" or self.interval=="5minute" or self.interval=="30minute":
                        skip=timedelta(minutes=value) 
                            
                    elif self.interval=="1second":
                        skip=timedelta(seconds=value) 
                    
                    elif self.interval=="1day":
                        skip=timedelta(days=value) 

                    while current_time<= exit_time: 
                        try:
                            logger.info(f"{current_time}")
                            if not ((start <= current_time.time()) and (current_time.time()<= finish)):
                                current_time += skip
                                continue
                            print(f"Ct:{current_time} | Et:{exit_time}")
                            entry_ce=df_combined.loc[current_time]["ce"] 
                            entry_pe=df_combined.loc[current_time]["pe"] 
                            spot=df_combined.loc[current_time]["spot"] 

                            time_to_expiry = (expiry_dt - current_time).total_seconds() / (365 * 24 * 60 * 60)  
                            volatility_ce = self.calculate_iv(entry_ce, spot, self.strike, time_to_expiry, self.RISK_FREE_RATE, self.DIVIDEND, 'call')
                            print(volatility_ce)
                            volatility_pe = self.calculate_iv(entry_pe, spot, self.strike, time_to_expiry, self.RISK_FREE_RATE, self.DIVIDEND, 'put') 
                            print(volatility_pe)
                            delta_ce,theta_ce,vega_ce,gamma_ce,vomma_ce = self.calculate_greeks( spot, self.strike, time_to_expiry, volatility_ce,"call") 
                            delta_pe,theta_pe,vega_pe,gamma_pe,vomma_pe = self.calculate_greeks( spot, self.strike, time_to_expiry, volatility_pe,"put") 
                            GT_ce=(gamma_ce/theta_ce)*100
                            GT_pe=(gamma_pe/theta_pe)*100
                            GT_Combine=(abs(gamma_ce)+abs(gamma_pe))/(abs(theta_ce)+abs(theta_pe))
                            self.log_greeks([current_time,self.strike,delta_ce,theta_ce,vega_ce,gamma_ce,vomma_ce,GT_ce,volatility_ce,delta_pe,theta_pe,vega_pe,gamma_pe,vomma_pe,GT_pe,volatility_pe,GT_Combine]) 
                            current_time=current_time+skip

                        except Exception as e:
                            print(f"Minute error at {current_time}: {e}")
                            current_time=current_time+skip 
                        
                        
                        if current_time> exit_time: 
                            print("Greeks calculated ")
                            df=pd.read_csv(self.temp_file)
                            # Save
                            df.to_csv(self.csv_file, mode='w', index=True) 
                            self.clear_position_state()             
                            break

                except Exception as e:
                    logger.error(f"Fatal error in main loop:", exc_info = e)
                    print(f"Main loop error: {str(e)}")
                        
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e