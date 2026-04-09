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

logger = create_logger(__name__, 'trading_RPSinghal_directional_orb_strategy.log')


class StrategyRPSinghalDirectionalOrb(BaseStrategy):
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
        
        self.api_secret=params["api_secret"]
        self.session_token=params["session_token"]
        self.breeze.ws_connect()
        self.breeze.on_ticks=self.on_ticks
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        start_time = str(params["start_hour"]) + ":" + str(params["start_minute"])
        end_time = str(params["end_hour"]) + ":" + str(params["end_minute"])

        self.TIME_1 = datetime.strptime(start_time, "%H:%M").time()
        self.TIME_2 = datetime.strptime(end_time, "%H:%M").time()
        self.EXPIRY = params["expiry"]
        self.FUT_EXPIRY=params["fut_expiry"]
        self.QTY = params["qty"]
        self.csv_file = params["csv_file"]
        self.ATM_STRIKE = None
        self.ADDING_POS = True
        self.MAX_POSITION = params["max_position"]
        # self.MAX_POSITION = 14
        self.TODAY = datetime.now().strftime("%Y-%m-%d")
        self.YESTERDAY = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d") # CHANGE
        self.PATH_CE = "unclosed_positions_RPSinghal_directional_ce.csv"
        self.PATH_PE = "unclosed_positions_RPSinghal_directional_pe.csv"
        

        # Global Variables
        self.tick_data = {}
        self.positions_df_ce = pd.DataFrame(columns=['datetime', 'action', 'strike', 'CE_or_PE', 'premium', 'trailing_sl'])
        self.positions_df_pe = pd.DataFrame(columns=['datetime', 'action', 'strike', 'CE_or_PE', 'premium', 'trailing_sl'])
        self.ws_connected = False
        self.lock = threading.Lock()
        self.expiry = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry.strftime('%d-%b-%Y') 
        self.expiry2= self.expiry.strftime('%Y-%m-%d')
        
    
    # Graceful shutdown handler
    def signal_handler(self,sig, frame):
        logger.info("Received shutdown signal. Saving positions and closing WebSocket...")
        with self.lock:
            if not self.positions_df_ce.empty:
                self.positions_df_ce.to_csv(self.PATH_CE, header=True, index=False)  # need to check
            if not self.positions_df_pe.empty:
                self.positions_df_pe.to_csv(self.PATH_PE, header=True, index=False)  # need to check
        if self.ws_connected:
            self.breeze.ws_disconnect()
        logger.info("Shutdown complete.")
        sys.exit(0)

    # Re Login 
    def re_login(self):
        try:
            try:
                self.breeze.generate_session(
                    api_secret=self.api_secret,
                    session_token=self.session_token
                )
                logger.info("BreezeConnect initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize BreezeConnect: ", exc_info = e)
                raise e
        
            self.breeze.ws_connect()
            self.breeze.on_ticks=self.on_ticks
            time.sleep(2)
        except Exception as e:
            logger.error(f"Failed to initialize Webesocket: ", exc_info = e)
            raise e

    # Retry decorator
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
        self.tick_data
        try:
            key = f"{ticks['strike_price']}_{ticks['right']}"
            with self.lock:
                self.tick_data[key] = ticks
            logger.debug(f"Received tick for {key}")
        except Exception as e:
            logger.error(f"Error in on_ticks: ", exc_info = e)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def initiate_ws(self,ce_or_pe, strike_price):
        self.tick_data
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
    
    def load_positions(self):
        self.positions_df_ce, self.positions_df_pe
        try:
            if os.path.exists(self.PATH_CE):
                self.positions_df_ce = pd.read_csv(self.PATH_CE)
                if not self.positions_df_ce.empty:
                    for _, row in self.positions_df_ce.iterrows():
                        self.initiate_ws(row['CE_or_PE'], row['strike'])
                        time.sleep(3)
            if os.path.exists(self.PATH_PE):
                self.positions_df_pe = pd.read_csv(self.PATH_PE)
                if not self.positions_df_pe.empty:
                    for _, row in self.positions_df_pe.iterrows():
                        self.initiate_ws(row['CE_or_PE'], row['strike'])
                        time.sleep(3)
        except Exception as e:
            logger.error(f"Error loading positions: ", exc_info = e)

    def get_current_market_price(self,ce_or_pe, strike_price):
        self.tick_data
        try:
            ce_or_pe = ce_or_pe.title()
            key = f'{strike_price}_{ce_or_pe}'
            with self.lock:
                if key in self.tick_data and self.tick_data[key] != '':
                    tick_entry = self.tick_data[key]
                    if tick_entry.get('right') == ce_or_pe:
                        current_price = tick_entry.get('last')
                        return float(current_price) if current_price else None
            return None
        except Exception as e:
            logger.error(f"Error fetching market price for {ce_or_pe} {strike_price}: {e}")
            return None


        
    def place_order(self,ce_or_pe, strike, action, quantity):
        try:
            order_detail = self.breeze.place_order(
                stock_code="NIFTY",
                exchange_code="NFO",
                product="options",
                action=action,
                order_type="limit",
                stoploss="",
                quantity=str(quantity),
                price="10",
                validity="ioc",
                disclosed_quantity="0",
                expiry_date=f'{self.expiry2}T06:00:00.000Z',
                right=ce_or_pe,
                strike_price=str(strike)
            )
            time.sleep(5)
            order_id = order_detail['Success']['order_id']
            trade_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
            execution_price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
            return execution_price
        except Exception as e:
            logger.error(f"Error placing order for {ce_or_pe} {strike}: ", exc_info = e)
            print(f"[ERROR] | Error placing {action} order for {ce_or_pe} {strike}: {str(e)}")
            raise
    
    def square_off(self,ce_or_pe, strike, action, quantity,price):
        try:
            order_detail = self.breeze.square_off(
                stock_code="NIFTY",
                exchange_code="NFO",
                product="options",
                action=action,
                order_type="limit",
                stoploss="",
                quantity=str(quantity),
                price=str(price),
                validity="ioc",
                disclosed_quantity="0",
                expiry_date=f'{self.expiry2}T06:00:00.000Z',
                right=ce_or_pe,
                strike_price=str(strike)
            )
            time.sleep(5)
            order_id = order_detail['Success']['order_id']
            trade_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
            execution_price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
            return execution_price
        except Exception as e:
            logger.error(f"Error placing order for {ce_or_pe} {strike}: ", exc_info = e)
            print(f"[ERROR] | Error placing {action} order for {ce_or_pe} {strike}: {str(e)}")
            raise
   

    def update_trailing_sl(self,positions_df, path):
        try:
            print("Running Trailing SL updation")
            positions_to_exit = []
            for index, position in positions_df.iterrows():
                current_price = self.get_current_market_price(position['CE_or_PE'], position['strike'])
                if current_price is None:
                    logger.warning(f"No current price for {position['CE_or_PE']} {position['strike']}")
                    continue

                if current_price >= position['trailing_sl']:
                    execution_price = self.square_off(position['CE_or_PE'], position['strike'], "buy", self.QTY,(position['trailing_sl']+1))
                    pnl= position['premium']- execution_price#change
                    positions_to_exit.append(index)
                    self.deactivate_ws(position['CE_or_PE'], position['strike'])
                    self.write_to_csv([self.TODAY, datetime.now().strftime('%H:%M:%S'), position['strike'], position['CE_or_PE'], position['premium'], execution_price, pnl,self.QTY])
                    logger.info(f"Exited position: {position['CE_or_PE']} {position['strike']} at {execution_price}")

                elif current_price < (position['trailing_sl'] / 2):
                    with self.lock:
                        positions_df.at[index, 'trailing_sl'] = current_price * 2
                        positions_df.to_csv(path, header=True, index=False)

            with self.lock:
                for index in positions_to_exit:
                    positions_df.drop(index, inplace=True)
                positions_df.to_csv(path, header=True, index=False)

            return positions_df
        except Exception as e:
            logger.error(f"Error updating trailing SL: ", exc_info = e)
            return positions_df    
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def leg_premium(self,ce_or_pe, strike):
        try:
            leg = self.breeze.get_option_chain_quotes(
                stock_code="NIFTY",
                exchange_code="NFO",
                product_type="options",
                expiry_date=f'{self.EXPIRY}T06:00:00.000Z',
                right=ce_or_pe,
                strike_price=str(strike)
            )
            time.sleep(0.1)
            return float(pd.DataFrame(leg['Success'])['ltp'][0])
        except Exception as e:
            logger.error(f"Error fetching premium for {ce_or_pe} {strike}: ", exc_info = e)
            raise

    def closest_put_otm(self):
        self.ATM_STRIKE, self.ADDING_POS
        try:
            strikes = [self.ATM_STRIKE - i for i in range(300, 2100, 50)]
            ltps = []
            for strike in strikes:
                try:
                    ltp_value = self.leg_premium("put", strike)
                    ltps.append({'strike_price': strike, 'ltp': ltp_value})
                except Exception as e:
                    logger.warning(f"Error fetching LTP for put {strike}: ", exc_info = e)

            if not ltps:
                logger.warning("No valid LTP data for puts.")
                return None

            target_ltp = 11
            nearest = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))
            max_ltp = max(ltps, key=lambda x: x['ltp'])['ltp']
            self.ADDING_POS = max_ltp >= 10
            logger.info(f"Closest put strike: {nearest['strike_price']} LTP: {nearest['ltp']}")
            return nearest['strike_price']
        except Exception as e:
            logger.error(f"Error in closest_put_otm: ", exc_info = e)
            return None
        
    def closest_call_otm(self):
        self.ATM_STRIKE, self.ADDING_POS
        try:
            strikes = [self.ATM_STRIKE + i for i in range(300, 2100, 50)]
            ltps = []
            for strike in strikes:
                try:
                    ltp_value = self.leg_premium("call", strike)
                    ltps.append({'strike_price': strike, 'ltp': ltp_value})
                except Exception as e:
                    logger.warning(f"Error fetching LTP for call {strike}:", exc_info = e)

            if not ltps:
                logger.warning("No valid LTP data for calls.")
                return None

            target_ltp = 11
            nearest = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))
            max_ltp = max(ltps, key=lambda x: x['ltp'])['ltp']
            self.ADDING_POS = max_ltp >= 10
            logger.info(f"Closest call strike: {nearest['strike_price']} LTP: {nearest['ltp']}")
            return nearest['strike_price']
        except Exception as e:
            logger.error(f"Error in closest_call_otm: ", exc_info = e)
            return None
    
    def check_profit_target_and_add_position(self,positions_df, path, ce_or_pe):
        self.ADDING_POS, self.ATM_STRIKE
        try:
            print("Checking profit target and adding position")
            if not positions_df.empty:
                last_position = positions_df.iloc[-1]
                current_price = self.get_current_market_price(last_position['CE_or_PE'], last_position['strike'])
                if current_price is None:
                    logger.warning(f"No current price for last position {last_position['CE_or_PE']} {last_position['strike']}")
                    return positions_df
                target_price = last_position['premium'] * 0.8
                print(f"Price : {current_price} || Target Price : {target_price}")

            open_position = len(self.positions_df_pe) + len(self.positions_df_ce)
            now = datetime.now().time()
            if open_position < self.MAX_POSITION and self.TIME_1 < now <= self.TIME_2 and current_price is not None and current_price <= target_price and self.ADDING_POS:
                
                # <----------- Change ----------->
                # Fetch latest NIFTY Future 5-min data
                fut_df = self.nifty_fut_historical()
                last_row = fut_df.iloc[-1]
                moving_avg = last_row['MA']

                # Checking condition before adding position for call and put
                if (ce_or_pe == "call" and last_row['close'] >= moving_avg-25) or (ce_or_pe == "put" and last_row['close'] <= moving_avg+25):
                    print(f"Skipping adding {ce_or_pe} because condition not met.")
                    logger.info(f"Skipping adding {ce_or_pe} because condition not met.")
                    return positions_df
                # <----------- Change ----------->

                nifty_spot_response = self.breeze.get_quotes(
                    stock_code="NIFTY",
                    exchange_code="NSE",
                    expiry_date=f"{self.TODAY}T06:00:00.000Z",
                    product_type="cash",
                    right="others",
                    strike_price="0"
                )
                time.sleep(1)
                nifty_spot_price = float(pd.DataFrame(nifty_spot_response['Success'])['ltp'][0])
                self.ATM_STRIKE = round(nifty_spot_price / 50) * 50

                strike = self.closest_call_otm() if ce_or_pe == "call" else self.closest_put_otm()
                if not strike:
                    logger.warning(f"No valid strike for {ce_or_pe}")
                    return positions_df

                nearest_premium = self.leg_premium(ce_or_pe, strike)
                if 10 < nearest_premium < 12.5:
                    leg_price = self.place_order(ce_or_pe, strike, "sell", self.QTY)
                    pnl=leg_price #change
                    new_position = {
                        'datetime': datetime.now().strftime('%H:%M:%S'),
                        'action': 'sell',
                        'strike': strike,
                        'CE_or_PE': ce_or_pe,
                        'premium': leg_price,
                        'trailing_sl': 2 * leg_price,
                    }

                    with self.lock:
                        new_position_df = pd.DataFrame([new_position])
                        positions_df = pd.concat([positions_df, new_position_df], ignore_index=True)
                        positions_df.to_csv(path, header=True, index=False)
                    logger.info(f"New position added: {new_position}")
                    try:
                        self.initiate_ws(ce_or_pe, strike)
                    except Exception as e:
                        logger.error(f"Failed to initialize websocket for {strike}_{ce_or_pe} :{e}")
                        self.re_login()
                        self.initiate_ws(ce_or_pe, strike)

            return positions_df
        except Exception as e:
            logger.error(f"Error in check_profit_target_and_add_position: ", exc_info = e)
            return positions_df
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def nifty_fut_historical(self):
        try:
            print("Fetching nifty historical data")
            data = self.breeze.get_historical_data_v2(
                interval="5minute",
                from_date=f"{self.YESTERDAY}T00:00:00.000Z",
                to_date=f"{self.TODAY}T17:00:00.000Z",
                stock_code="NIFTY",
                exchange_code="NFO",
                product_type="futures",
                expiry_date=f'{self.FUT_EXPIRY}T07:00:00.000Z',
                right="others",
                strike_price="0"
            )
            time.sleep(0.5)
            olhc = pd.DataFrame(data['Success'])
            olhc['datetime'] = pd.to_datetime(olhc['datetime'])
            olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                        (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
            olhc['MA']=ta.sma(olhc['close'], length=75)  # change creating seperate column of moving average
            return olhc
        except Exception as e:
            logger.error(f"Error fetching Nifty futures historical data: ", exc_info = e)
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def option_historical(self,ce_or_pe, strike):
        try:
            print("Fetching option historical data")
            option_data = self.breeze.get_historical_data_v2(
                interval="5minute",
                from_date=f"{self.TODAY}T07:00:00.000Z",
                to_date=f"{self.TODAY}T17:00:00.000Z",
                stock_code="NIFTY",
                exchange_code="NFO",
                product_type="options",
                expiry_date=f"{self.EXPIRY}T07:00:00.000Z",
                right=ce_or_pe,
                strike_price=str(strike)
            )
            time.sleep(0.5)
            if option_data.get('Success'):
                print(f"Historical data fetched for {ce_or_pe} at strike {strike}")
                return pd.DataFrame(option_data['Success'])
            
        except Exception as e:
            logger.error(f"Error fetching option historical data for {ce_or_pe} {strike}: ", exc_info = e)
            print(f"Error fetching historical data for {ce_or_pe} {strike}: {str(e)}")
            raise

    # Creating Csv File
    # Mian function
    def write_to_csv(self,data):
        headers = ['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry Premium',
                'Exit Premium', 'PnL', 'Quantity']
        try:
            # Create file with headers if it doesn't exist
            if not os.path.exists(self.csv_file):
                with open(self.csv_file, 'x', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(headers)
                    logger.info("Created new PnL CSV file")

            # Append trade data
            with open(self.csv_file, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(data)
                logger.info(f"PnL entry written to CSV: {data}")
        except Exception as e:
            logger.error("Error writing to PnL CSV", exc_info=e)


    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                self.connect_websocket()
                self.load_positions()
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now()
                        open_position = len(self.positions_df_pe) + len(self.positions_df_ce)
                        if self.TIME_1 < now.time() <= self.TIME_2 and now.time().second == 0:
                            if self.positions_df_pe.empty and self.ADDING_POS:
                                try:
                                    olhc = self.nifty_fut_historical()
                                    print("Fetching of OLHC done")
                                    candles_3 = olhc.iloc[-7:-1]
                                    resistance = candles_3['high'].max()
                                    support = candles_3['low'].min()
                                    last_row = olhc.iloc[-1]
                                    moving_avg=olhc.iloc[-1]['MA'] # Change Fetching MA value
                                    print(f"candles_3 :- {candles_3} \n || resistance :- {resistance} || support :- {support} || last_row :- {last_row['close']} || moving_average:{moving_avg}")


                                    if (last_row['close'] > resistance) and (last_row['close']>moving_avg+25) and (open_position <self.MAX_POSITION): #Change entry condition for Put
                                        self.ATM_STRIKE = round(last_row['close'] / 50) * 50
                                        closest_strike_pe = self.closest_put_otm()
                                        print("Fetching of Closest strike pe done")
                                        print(f"Atm Strike {self.ATM_STRIKE} || Closest Strike PE {closest_strike_pe}")
                                        if closest_strike_pe:
                                            option_data = self.option_historical("put", closest_strike_pe)
                                            print("Fetching of options done")
                                            cand = option_data.iloc[-7:-1]
                                            sup = cand['low'].min()
                                            last = option_data.iloc[-1]
                                            print(f"cand :- {cand} \n || sup :- {sup} || last :- {last['close']} ") 

                                            if last['close'] <= sup and 10 < last['close'] < 12.5:
                                                print("OPtion condition Meet")
                                                entry_premium = self.place_order("put", closest_strike_pe, "sell", self.QTY)
                                                pnl=entry_premium
                                                position = {
                                                    'datetime': now.strftime('%H:%M:%S'),
                                                    'action': 'sell',
                                                    'strike': closest_strike_pe,
                                                    'CE_or_PE': 'put',
                                                    'premium': entry_premium,
                                                    'trailing_sl': entry_premium * 2,
                                                }
                                                self.positions_df_pe = pd.DataFrame([position])
                                                self.positions_df_pe.to_csv(self.PATH_PE, header=True, index=False)
                                                logger.info(f"Sell PUT {closest_strike_pe} at {entry_premium}")
                                                try:
                                                    self.initiate_ws('put', closest_strike_pe)
                                                except Exception as e:
                                                    logger.error(f"Failed to initialize websocket for {closest_strike_pe}_Put :{e}")
                                                    self.re_login()
                                                    self.initiate_ws('put', closest_strike_pe)
                                            else:
                                                print(" Option Condition Fails ")    
                                    else:
                                        print(" [last_row['close'] > resistance] PE Condition Failed Nifty value is less than resistance")   
                                except Exception as e:
                                    logger.error(f"Error in PE entry logic: ", exc_info = e)

                            if self.positions_df_ce.empty and self.ADDING_POS:
                                try:
                                    olhc = self.nifty_fut_historical()
                                    print("Fetching of OLHC done")
                                    candles_3 = olhc.iloc[-7:-1]
                                    resistance = candles_3['high'].max()
                                    support = candles_3['low'].min()
                                    last_row = olhc.iloc[-1]
                                    moving_avg=olhc.iloc[-1]['MA']
                                    print(f"candles_3 :- {candles_3} \n || resistance :- {resistance} || support :- {support} || last_row :- {last_row['close']}|| moving_average:{moving_avg}")

                                    if (last_row['close'] < support) and (last_row['close']< moving_avg-25) and (open_position <self.MAX_POSITION): #Change entry condition for Call
                                        self.ATM_STRIKE = round(last_row['close'] / 50) * 50
                                        closest_strike_ce = self.closest_call_otm()
                                        if closest_strike_ce:
                                            option_data = self.option_historical("call", closest_strike_ce)
                                            print("Fetching of options done")
                                            cand = option_data.iloc[-7:-1]
                                            sup = cand['low'].min()
                                            last = option_data.iloc[-1]
                                            print(f"cand :- {cand} \n || sup :- {sup} || last :- {last['close']} ")

                                            if last['close'] <= sup and 10 < last['close'] < 12.5:
                                                print("OPtion condition Meet")
                                                entry_premium = self.place_order("call", closest_strike_ce, "sell", self.QTY)
                                                pnl= entry_premium
                                                position = {
                                                    'datetime': now.strftime('%H:%M:%S'),
                                                    'action': 'sell',
                                                    'strike': closest_strike_ce,
                                                    'CE_or_PE': 'call',
                                                    'premium': entry_premium,
                                                    'trailing_sl': entry_premium * 2,
                                                }
                                                self.positions_df_ce = pd.DataFrame([position])
                                                self.positions_df_ce.to_csv(self.PATH_CE, header=True, index=False)
                                                logger.info(f"Sell CALL {closest_strike_ce} at {entry_premium}")
                                                try:
                                                    self.initiate_ws('call', closest_strike_ce)
                                                except Exception as e:
                                                    logger.error(f"Failed to initialize websocket for {closest_strike_ce}_Call :{e}")
                                                    self.re_login()
                                                    self.initiate_ws('call', closest_strike_ce)
                                            
                                            else:
                                                print(" Option Condition Fails ")
                                    else:
                                        print(" [last_row['close'] < support] CE Condition Failed Nifty value is greater than support")  

                                except Exception as e:
                                    logger.error(f"Error in CE entry logic: ", exc_info = e)

                        if not self.positions_df_pe.empty:
                            print(now)
                            self.positions_df_pe = self.update_trailing_sl(self.positions_df_pe, self.PATH_PE)
                            self.positions_df_pe = self.check_profit_target_and_add_position(self.positions_df_pe, self.PATH_PE, "put")
                            print(self.positions_df_pe)
                            if now.time() > self.TIME_2:
                                self.positions_df_pe.to_csv(self.PATH_PE, header=True, index=False)
                                logger.info("PE positions saved. Market closed.")
                                break

                        if not self.positions_df_ce.empty:
                            print(now)
                            self.positions_df_ce = self.update_trailing_sl(self.positions_df_ce, self.PATH_CE)
                            self.positions_df_ce = self.check_profit_target_and_add_position(self.positions_df_ce, self.PATH_CE, "call")
                            print(self.positions_df_ce)
                            if now.time() > self.TIME_2:
                                self.positions_df_ce.to_csv(self.PATH_CE, header=True, index=False)
                                logger.info("CE positions saved. Market closed.")
                                break

                        time.sleep(1)

                    except Exception as e:
                        logger.error(f"Fatal error in main loop:", exc_info = e)
                        self.signal_handler(None, None)
                        
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e

