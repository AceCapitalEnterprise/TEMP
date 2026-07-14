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


    # Merton (Black-Scholes with continuous dividend) Pricing & Greeks

    def merton_price(self, S, K, T, r, q, sigma, option_type):
        """Price a European option using the Merton continuous-dividend model."""
        if T <= 0 or sigma <= 0:
            return max(0.0, S - K) if option_type == 'call' else max(0.0, K - S)
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        if option_type == 'call':
            return S * np.exp(-q * T) * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else:
            return K * np.exp(-r * T) * norm.cdf(-d2) - S * np.exp(-q * T) * norm.cdf(-d1)

    def calculate_iv(self, option_price, S, K, T, r, q, option_type):
        """Calculate implied volatility using the Merton model via Brent's method."""
        try:
            if T <= 0 or option_price <= 0:
                return 0.001
            objective_func = lambda sigma: self.merton_price(S, K, T, r, q, sigma, option_type) - option_price
            return brentq(objective_func, 1e-4, 5.0)
        except ValueError:
            return np.nan

    def black_scholes_greeks(self, entry, spot, strike, time_to_expiry, volatility, risk_free_rate, dividend, option_type="call"):
        """Calculate Delta, Theta, Vega, Gamma, Vomma using the Merton model."""
        if time_to_expiry <= 0 or volatility <= 0 or np.isnan(volatility):
            return 0, 0, 0, 0, 0

        d1 = (np.log(spot / strike) + (risk_free_rate - dividend + 0.5 * volatility**2) * time_to_expiry) / (volatility * np.sqrt(time_to_expiry))
        d2 = d1 - volatility * np.sqrt(time_to_expiry)

        N_d1 = norm.cdf(d1)
        N_prime_d1 = norm.pdf(d1)

        gamma = (np.exp(-dividend * time_to_expiry) * N_prime_d1) / (spot * volatility * np.sqrt(time_to_expiry))
        vega = (spot * np.exp(-dividend * time_to_expiry) * N_prime_d1 * np.sqrt(time_to_expiry)) / 100
        vomma = vega * (d1 * d2) / volatility if volatility != 0 else 0

        if option_type == "call":
            delta = np.exp(-dividend * time_to_expiry) * N_d1
            theta = (
                (-np.exp(-dividend * time_to_expiry) * spot * N_prime_d1 * volatility / (2 * np.sqrt(time_to_expiry)))
                + (dividend * spot * np.exp(-dividend * time_to_expiry) * N_d1)
                - (risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2))
            ) / 365
        else:
            delta = np.exp(-dividend * time_to_expiry) * (N_d1 - 1)
            theta = (
                (-np.exp(-dividend * time_to_expiry) * spot * N_prime_d1 * volatility / (2 * np.sqrt(time_to_expiry)))
                - (dividend * spot * np.exp(-dividend * time_to_expiry) * norm.cdf(-d1))
                + (risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2))
            ) / 365

        # Near-expiry theta cap: theta cannot exceed extrinsic value
        intrinsic = max(0, spot - strike) if option_type == "call" else max(0, strike - spot)
        extrinsic = max(0, entry - intrinsic)
        if time_to_expiry < (1 / 365):
            theta = -min(abs(theta), extrinsic)

        return delta, theta, vega, gamma, vomma

    # Calculating Greeks 
    def calculate_greeks(self, nifty, strike, time_to_expiry, volatility, option_type, entry=0):
        delta, theta, vega, gamma, vomma = self.black_scholes_greeks(
            entry, nifty, strike, time_to_expiry, volatility,
            self.RISK_FREE_RATE, self.DIVIDEND, option_type
        )
        print(f"Delta : {delta} || Theta : {theta} || Vega: {vega}|| Gamma: {gamma} || Vomma: {vomma}")
        return delta, theta, vega, gamma, vomma
    
   # Creating Csv 
    def log_greeks(self,data): 
        # Added 'Premium_CE' and 'Premium_PE' to headers
        headers = ['Time', 'Strike', 'Delta_CE','Theta_CE','Vega_CE','Gamma_CE', 'Vomma_CE','GT_CE','IV_CE', 'Premium_CE', 'Delta_PE','Theta_PE','Vega_PE','Gamma_PE','Vomma_PE','GT_PE','IV_PE', 'Premium_PE', 'GT_Combine'] 
        try: 
            try:
                with open(self.temp_file, 'x', newline='') as file: 
                    writer = csv.writer(file) 
                    writer.writerow(headers) 
                    print("Created new CSV file") 
            except FileExistsError: pass 
            with open(self.temp_file, 'a', newline='') as file:  
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
                            delta_ce,theta_ce,vega_ce,gamma_ce,vomma_ce = self.calculate_greeks(spot, self.strike, time_to_expiry, volatility_ce, "call", entry=entry_ce) 
                            delta_pe,theta_pe,vega_pe,gamma_pe,vomma_pe = self.calculate_greeks(spot, self.strike, time_to_expiry, volatility_pe, "put", entry=entry_pe) 
                            GT_ce=(gamma_ce/theta_ce)*100
                            GT_pe=(gamma_pe/theta_pe)*100
                            GT_Combine=(abs(gamma_ce)+abs(gamma_pe))/(abs(theta_ce)+abs(theta_pe))
                            self.log_greeks([current_time,self.strike,delta_ce,theta_ce,vega_ce,gamma_ce,vomma_ce,GT_ce,volatility_ce,entry_ce,delta_pe,theta_pe,vega_pe,gamma_pe,vomma_pe,GT_pe,volatility_pe,entry_pe,GT_Combine]) 
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