from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
import time
from datetime import datetime, date, timedelta, time as t
import csv
import os
import requests
import threading
import signal
import sys
import logging
from ..common.logging_config import create_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = create_logger(__name__, 'trading_DS_Sanjay_Sir_strategy.log')

class StrategySanjaySir(BaseStrategy):
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
        self.order=0
        self.remaining_quantity = 130
        self.half_quantity = 65
        self.premium_half_exit = False
        self.otm_strike = None
        self.premium = None
        self.target_profit = None
        self.stop_loss = None
        self.ce_or_pe = None
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        self.one_tick=None
        self.tick_data= {}
        self.lock = threading.Lock()
        self.expiry = datetime.strptime(self.EXPIRY, '%Y-%m-%d').date()
        self.expiry1 = self.expiry.strftime('%d-%b-%Y') 

        self.nifty_spot = None
        self.atm_strike = None
        self.itm_strike = None
        self.itm_put_strike = None
        self.yesterday_high_call = None
        self.yesterday_high_call = None
        self.yesterday_high_put = None
        self.yesterday_high_put =None

    def on_ticks(self,ticks):
        self.tick_data
        try:
            key=str(ticks['strike_price'])+'_'+str(ticks['right'])
            if key in self.tick_data:
                self.tick_data[key] = ticks
            logger.debug(f"Received tick for {key}")
        except Exception as e:
            logger.error(f"Error in on_ticks: ", exc_info = e)
    
    def initiate_ws(self,CE_or_PE, strike_price):
        self.tick_data
        leg = self.breeze.subscribe_feeds(exchange_code="NFO",
                stock_code="NIFTY",
                product_type="options",
                expiry_date=self.expiry1,
                right=CE_or_PE,
                strike_price=str(strike_price),
                get_exchange_quotes=True,
                get_market_depth=False)
        CE_or_PE = CE_or_PE.title()
        self.tick_data[f'{strike_price}_{CE_or_PE}']=''
        print(leg)
    
    def deactivate_ws(self,CE_or_PE,strike_price):
        leg=self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=str(self.expiry1),
                                right=CE_or_PE,
                                strike_price=str(strike_price),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        CE_or_PE = CE_or_PE.title()
        if f'{strike_price}_{CE_or_PE}' in self.tick_data:
            self.tick_data.pop(f'{strike_price}_{CE_or_PE}')
        print(leg)
    
    def get_current_market_price(self,CE_or_PE, strike_price):
        # global current_price,tick_data
        self.tick_data 
        print(f"Fetching price for: CE_or_PE={CE_or_PE}, strike_price={strike_price}")
        
        CE_or_PE = CE_or_PE.title()
        if f'{strike_price}_{CE_or_PE}' in self.tick_data and self.tick_data[f'{strike_price}_{CE_or_PE}']!='':
            tick_entry = self.tick_data[f'{strike_price}_{CE_or_PE}']
            if tick_entry.get('right') == CE_or_PE:
                current_price = tick_entry.get('last')
                return current_price
        return None
    

    def closest_call_itm(self):
        # global nearest_premium, adding_pos
        nearest_premium = None
        adding_pos = None
        strikes = [self.atm_strike - i for i in range(50, 1100, 50)]
        
        ltps = []
        
        for strike in strikes:
            try:
                ltp_value = self.leg_premium("Call", strike)
                if ltp_value is not None:
                    ltps.append({'strike_price': strike, 'ltp': ltp_value})
            except Exception as e:
                print(f"Error fetching LTP for strike {strike}: {e}")
        
        if not ltps:
            print("No valid LTP data available.")
            closest_strike_ce = None
        else:
            target_ltp = 200
            nearest_premium = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['ltp']
            max_ltp = max(ltps, key=lambda x: x['ltp'])['ltp']
            closest_strike_ce = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['strike_price']
        
        
        print("Closest strike with LTP near target:", closest_strike_ce, "LTP:", nearest_premium)
        return closest_strike_ce
    
    def closest_put_itm(self):
        # global nearest_premium, adding_pos
        nearest_premium = None
        adding_pos = None
        strikes = [self.atm_strike + i for i in range(50, 1100, 50)]
        
        ltps = []
        
        for strike in strikes:
            try:
                ltp_value = self.leg_premium("Put", strike)
                if ltp_value is not None:
                    ltps.append({'strike_price': strike, 'ltp': ltp_value})
            except Exception as e:
                print(f"Error fetching LTP for strike {strike}: {e}")
        
        if not ltps:
            print("No valid LTP data available.")
            closest_strike_ce = None
        else:
            target_ltp = 200
            nearest_premium = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['ltp']
            max_ltp = max(ltps, key=lambda x: x['ltp'])['ltp']
            closest_strike_ce = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['strike_price']
        
        
        print("Closest strike with LTP near target:", closest_strike_ce, "LTP:", nearest_premium) 
        return closest_strike_ce
    

    def closest_call_otm(self):
        # global nearest_premium, adding_pos
        nearest_premium = None
        adding_pos = None
        strikes = [self.atm_strike + i for i in range(50, 1100, 50)]
        
        ltps = []
        
        for strike in strikes:
            try:
                ltp_value = self.leg_premium("call", strike)
                if ltp_value is not None:
                    ltps.append({'strike_price': strike, 'ltp': ltp_value})
            except Exception as e:
                print(f"Error fetching LTP for strike {strike}: {e}")
        
        if not ltps:
            print("No valid LTP data available.")
            closest_strike_ce = None
        else:
            target_ltp = 45
            nearest_premium = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['ltp']
            max_ltp = max(ltps, key=lambda x: x['ltp'])['ltp']
            closest_strike_ce = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['strike_price']
        
        
        print("Closest strike with LTP near target:", closest_strike_ce, "LTP:", nearest_premium) 
        return closest_strike_ce, nearest_premium

    def closest_put_otm(self):
        # global nearest_premium, adding_pos
        nearest_premium = None
        adding_pos = None
        strikes = [self.atm_strike - i for i in range(50, 1100, 50)]
        ltps = []
        
        for strike in strikes:
            try:
                ltp_value = self.leg_premium("put", strike)
                if ltp_value is not None:
                    ltps.append({'strike_price': strike, 'ltp': ltp_value})
            except Exception as e:
                print(f"Error fetching LTP for strike {strike}: {e}")
        
        if not ltps:
            print("No valid LTP data available.")
            closest_strike_ce = None
        else:
            target_ltp = 45
            nearest_premium = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['ltp']
            max_ltp = max(ltps, key=lambda x: x['ltp'])['ltp']
            closest_strike_ce = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['strike_price']
        
        
        print("Closest strike with LTP near target:", closest_strike_ce, "LTP:", nearest_premium) 
        return closest_strike_ce, nearest_premium
    
    def option_historical(self,ce_or_pe, strike, start, end):
        option_data = self.breeze.get_historical_data_v2(interval="1day",
                                                            from_date= f"{start}T07:00:00.000Z",
                                                            to_date= f"{end}T17:00:00.000Z",
                                                            stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                            right=ce_or_pe,
                                                            strike_price=strike)
        time.sleep(0.5)
        if option_data is not None:
            option_data = option_data['Success']
            option_data = pd.DataFrame(option_data)
            return option_data
        else:
            print("Error in fetching option historical data")
            self.option_historical(ce_or_pe, strike)
    
    def leg_premium(self,ce_or_pe, strike):
        leg = self.breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                    exchange_code="NFO",
                                                    product_type="options",
                                                    expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                    right=ce_or_pe,
                                                    strike_price=strike)
        time.sleep(0.5)
        if leg is not None:
            leg_df = leg['Success']
            leg_df = pd.DataFrame(leg_df)
            ltp_value = float(leg_df['ltp'].iloc[0])
            return ltp_value
        else:
            self.leg_premium(ce_or_pe, strike)
            print("Error in fetching option premium")
        
    
    def place_order_breeze(self,strike, cepe, action, quantity, price=None, order_type="Market"):
        """
        Placeholder for the actual Breeze place_order function.
        In a real scenario, you would use 'Limit' or 'Market' and manage order response.
        """
        print(f"--- ORDER PLACED (Simulated) ---")
        print(f"ACTION: {action} {quantity} {cepe} {strike}")
    
    def log_exit(self,strike, cepe, action, current_premium, quantity,pts):
        with open(self.csv_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime('%H:%M:%S'), strike, cepe, action, current_premium, quantity,pts])

    
    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                self.nifty_spot = self.breeze.get_quotes(exchange_code="NSE", stock_code="NIFTY")
                self.nifty_spot = float(self.nifty_spot['Success'][0]['ltp'])
                self.atm_strike = int(self.nifty_spot / 50) * 50
                self.itm_strike = self.closest_call_itm()
                self.itm_put_strike = self.closest_put_itm()
                print(f"ic:{self.itm_strike} : ip: {self.itm_put_strike}")
                logger.info(f"ic:{self.itm_strike} : ip: {self.itm_put_strike}")
                self.yesterday_high_call = self.option_historical('call', self.itm_strike, self.yesterday, self.today)
                self.yesterday_high_call = self.yesterday_high_call['high'].iloc[-1]
                self.yesterday_high_put = self.option_historical('put', self.itm_put_strike, self.yesterday, self.today)
                self.yesterday_high_put = self.yesterday_high_put['high'].iloc[-1]
                self.initiate_ws("call", self.itm_strike)
                self.initiate_ws("put", self.itm_put_strike)
                
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()
                        current_time_t = t(now.hour, now.minute)
                        
                        if current_time_t >= self.TIME_2 and self.order == 1:
                            print(f"EOD EXIT (15:20)! Exiting remaining position: {self.remaining_quantity} Quantity.")
                            logger.info(f"EOD EXIT (15:20)! Exiting remaining position: {self.remaining_quantity} Quantity.")

                            
                            current_premium = self.get_current_market_price(self.ce_or_pe, self.otm_strike)
                            pts = current_premium - self.premium
                            self.place_order_breeze(self.otm_strike, self.ce_or_pe, "Sell", self.remaining_quantity)
                            self.log_exit(self.otm_strike, self.ce_or_pe, 'sell', current_premium, self.remaining_quantity,pts)
                        
                            self.deactivate_ws(self.ce_or_pe, self.otm_strike)
                            self.order = 0
                            self.premium_half_exit = False
                            self.remaining_quantity = 130
                            print("Script finished successfully based on EOD time.")
                            logger.info("Script finished successfully based on EOD time.")
                            sys.exit()
                        
                        if self.TIME_1 < current_time_t <= self.TIME_2 and self.order == 0 and now.second == 0 :
                            time.sleep(1)
                            option_premium_call = self.get_current_market_price("call", self.itm_strike)
                            option_premium_put = self.get_current_market_price("put", self.itm_put_strike)
                            
                            if option_premium_call is None or option_premium_put is None:
                                print(f"{now}: Waiting for current market prices for ITM options...")
                                logger.info(f"{now}: Waiting for current market prices for ITM options...")
                                continue
                        
                            if option_premium_call > self.yesterday_high_call :
                                self.otm_strike, self.premium = self.closest_call_otm()
                                if self.premium is None: continue 
                                self.ce_or_pe = "call"
                                print(f"ENTRY: Buy {self.ce_or_pe.upper()} {self.otm_strike} at premium {self.premium} with quantity {self.QTY}")
                                logger.info(f" ENTRY: Buy {self.ce_or_pe.upper()} {self.otm_strike} at premium {self.premium} with quantity {self.QTY}")

                                
                                self.place_order_breeze(self.otm_strike, self.ce_or_pe, "Buy", self.QTY)
                        
                                self.order = 1
                                self.remaining_quantity = self.QTY
                                self.initiate_ws("call", self.otm_strike)
                                
                                try:
                                    with open(self.csv_file, 'x', newline='') as file:
                                        writer = csv.writer(file)
                                        writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium', 'Quantity','Points'])
                                except FileExistsError:
                                    pass
                                with open(self.csv_file, 'a', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow([self.today, datetime.now().strftime('%H:%M:%S'), self.otm_strike, 'call', 'buy', self.premium, self.QTY,0])
                        
                            elif option_premium_put > self.yesterday_high_put :
                                self.otm_strike, self.premium = self.closest_put_otm()
                                if self.premium is None: continue 
                                self.ce_or_pe = "put"
                                print(f"ENTRY: Buy {self.ce_or_pe.upper()} {self.otm_strike} at premium {self.premium} with quantity {self.QTY}")
                                logger.info(f" ENTRY: Buy {self.ce_or_pe.upper()} {self.otm_strike} at premium {self.premium} with quantity {self.QTY}")
                                
                                self.place_order_breeze(self.otm_strike, self.ce_or_pe, "Buy", self.QTY)
                                
                                self.order = 1
                                self.remaining_quantity = self.QTY
                                self.initiate_ws("put", self.otm_strike)
                        
                                # csv_file = "New_buying_strategy.csv"
                                try:
                                    with open(self.csv_file, 'x', newline='') as file:
                                        writer = csv.writer(file)
                                        writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium', 'Quantity','Points'])
                                except FileExistsError:
                                    pass
                                with open(self.csv_file, 'a', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow([self.today, datetime.now().strftime('%H:%M:%S'), self.otm_strike, 'put', 'buy', self.premium, self.QTY,0])
                            else:
                                print(now, "No entry condition met")
                                logger.info("No entry condition met")


                        if self.order == 1 :
                            time.sleep(1)
                            current_premium = self.get_current_market_price(self.ce_or_pe, self.otm_strike)
                            
                            if current_premium is None:
                                print(f"{now}: Waiting for current market price for OTM option...")
                                logger.info(f"{now}: Waiting for current market price for OTM option...")
                                continue
                        
                            pnl = current_premium - self.premium
                            print(f"Current PnL is: {pnl:.2f} (Current Premium: {current_premium:.2f})")

                            
                            if not self.premium_half_exit:
                                self.target_profit = self.premium + 15
                                self.stop_loss = self.premium - 15
                                
                                if current_premium >= self.target_profit:
                                    print(f"Half quantity ({self.half_quantity}) exit at Profit Target (+15 points)! Premium: {current_premium:.2f}")
                                    logger.info(f"Half quantity ({self.half_quantity}) exit at Profit Target (+15 points)! Premium: {current_premium:.2f}")
                                    self.premium_half_exit = True
                                    self.remaining_quantity -= self.half_quantity
                                    pts=current_premium-self.premium
                                    
                                    self.place_order_breeze(self.otm_strike, self.ce_or_pe, "Sell", self.half_quantity)
                                    self.log_exit(self.otm_strike, self.ce_or_pe, 'sell', current_premium, self.half_quantity,pts)
                                        
                                elif current_premium <= self.stop_loss:
                                    print(f"Full quantity ({self.QTY}) exit at Initial SL (-15 points)! Premium: {current_premium:.2f}")
                                    logger.info(f"Full quantity ({self.QTY}) exit at Initial SL (-15 points)! Premium: {current_premium:.2f}")
                                    
                                    self.place_order_breeze(self.otm_strike, self.ce_or_pe, "Sell", self.QTY)
                                    pts=current_premium-self.premium
                                    self.log_exit(self.otm_strike, self.ce_or_pe, 'sell', current_premium, self.QTY,pts)
                                    self.order = 0
                                    self.premium_half_exit = False
                                    self.remaining_quantity = 130
                                    self.deactivate_ws(self.ce_or_pe, self.otm_strike)

                            elif self.premium_half_exit:
                                revised_stop_loss = self.target_profit - 7.5
                                # revised_target_profit = self.target_profit + 15 
                                
                                if current_premium <= revised_stop_loss:
                                    print(f"Remaining half quantity ({self.half_quantity}) exit at Revised SL (Buy Price - 7.5)! Premium: {current_premium:.2f}")
                                    logger.info(f"Remaining half quantity ({self.half_quantity}) exit at Revised SL (Buy Price - 7.5)! Premium: {current_premium:.2f}")
                                    self.place_order_breeze(self.otm_strike, self.ce_or_pe, "Sell", self.half_quantity)
                                    pts=current_premium-self.premium
                                    self.log_exit(self.otm_strike, self.ce_or_pe, 'sell', current_premium, self.half_quantity,pts)
                                    self.order = 0
                                    self.premium_half_exit = False
                                    self.remaining_quantity = 130
                                    self.deactivate_ws(self.ce_or_pe, self.otm_strike)

                                # elif current_premium >= revised_target_profit:
                                #     print(f" Remaining half quantity ({self.half_quantity}) re-exit at Profit Target (+15 points)! Premium: {current_premium:.2f}")
                                #     logger.info(f"Remaining half quantity ({self.half_quantity}) re-exit at Profit Target (+15 points)! Premium: {current_premium:.2f}")

                                #     self.place_order_breeze(self.otm_strike, self.ce_or_pe, "Sell", self.half_quantity)
                                #     pts=current_premium-self.premium
                                #     self.log_exit(self.otm_strike, self.ce_or_pe, 'sell', current_premium, self.half_quantity,pts)
                                    
                                #     self.order = 0
                                #     self.premium_half_exit = False
                                #     self.remaining_quantity = 130
                                #     self.deactivate_ws(self.ce_or_pe,self.otm_strike)

                    except Exception as e:
                                logger.error(f"Fatal error in main loop:", exc_info = e)
                                
                        
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e                