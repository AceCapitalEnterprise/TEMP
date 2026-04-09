from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
from datetime import date, datetime, timedelta
import time
import csv
from ..common.logging_config import create_logger
from tenacity import retry, stop_after_attempt, wait_exponential


logger = create_logger(__name__, 'trading_macd_strategy.log')

# Constants
#TIME_1 = datetime.strptime("09:15", "%H:%M").time()
#TIME_2 = datetime.strptime("15:20", "%H:%M").time()
#EXPIRY = '2025-06-12'
#QTY = 75

# Global variables
#order = 0 
#sl = 0  
#spot_price=None
#tick_data={}
#expiry1 = datetime.strptime(EXPIRY, '%Y-%m-%d')
#expiry1 = expiry1.strftime('%d-%b-%Y')

class StrategyMACD(BaseStrategy):
    def __init__(self, params):
        super().__init__()

        self.params = params

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
        self.QTY = params["qty"]
        self.csv_file = params["csv_file"]

        # Global variables
        self.order = 0  # For CE
        self.sl = 0    # For CE
        self.spot_price=None
        self.tick_data={}

        self.expiry1 = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry1.strftime('%d-%b-%Y')


    def on_ticks(self, ticks):
        if 'strike_price' in ticks:
            data = ticks['strike_price']+'_'+ticks['right']
            if data in self.tick_data:
                self.tick_data[data]=ticks['last']
        else:
            self.spot_price=ticks['last']
    

    def initiate_ws(self, atm, right=''):
        if right=='call':
            leg=self.breeze.subscribe_feeds(exchange_code="NFO",
                                    stock_code="NIFTY",
                                    product_type="options",
                                    expiry_date=self.expiry1,
                                    right="call",
                                    strike_price=str(atm),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)
            self.tick_data[str(atm)+'_Call']=''
        elif right=='put':
            leg2=self.breeze.subscribe_feeds(exchange_code="NFO",
                                    stock_code="NIFTY",
                                    product_type="options",
                                    expiry_date=self.expiry1,
                                    right="put",
                                    strike_price=str(atm),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)
            self.tick_data[str(atm)+'_Put']=''
        elif right=='others':
            leg3=self.breeze.subscribe_feeds(exchange_code="NSE",
                                    stock_code="NIFTY",
                                    product_type="CASH",
                                    expiry_date=self.expiry1,
                                    right="others",
                                    strike_price=str(atm),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)


    def deactivate_ws(self, atm, right=''):
        if right=='call':    
            self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry1,
                                right="call",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
            self.tick_data.pop(str(atm)+'_Call')
        elif right=='put':
            self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry1,
                                right="put",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
            self.tick_data.pop(str(atm)+'_Put')


    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def nifty_spot(self):
        try:
            if self.spot_price is not None:
                return float(self.spot_price)
            raise ValueError("No success response from API")
        except Exception as e:
            logging.error(f"Error fetching Nifty spot: ", exc_info = e)
            raise


    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_historical(self,yesterday, today):
        try:
            option_data = self.breeze.get_historical_data_v2(
                interval="1minute",
                from_date=f"{yesterday}T07:00:00.000Z",
                to_date=f"{today}T17:00:00.000Z",
                stock_code="NIFTY",
                exchange_code="NSE",
                product_type="cash"
            )
            time.sleep(4)
            if option_data.get('Success'):
                df = pd.DataFrame(option_data['Success'])
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df[(df['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                            (df['datetime'].dt.time <= pd.to_datetime('15:29').time())]
                
                df['12_EMA'] = df['close'].ewm(span=12, adjust=False).mean()
                df['26_EMA'] = df['close'].ewm(span=26, adjust=False).mean()
                df['MACD_Line'] = df['12_EMA'] - df['26_EMA']
                df['Signal_Line'] = df['MACD_Line'].ewm(span=9, adjust=False).mean()
                df['MACD_Histogram'] = df['MACD_Line'] - df['Signal_Line']
                df['MACD'] = df['MACD_Line']
                
                df['close'] = pd.to_numeric(df['close'])
                df.ta.rsi(close='close', length=14, append=True)
                print(f"Historical data fetched ")
                return df
            raise ValueError("No success response from API")
        except Exception as e:
            logger.error(f"Error fetching historical data : ", exc_info = e)
            print(f"Error fetching historical data : {str(e)}")
            raise


    def place_order(self,action, right, strike, qty):
        max_attempts = 2
        attempt = 0
        while attempt < max_attempts:
            try:
                order_detail = self.breeze.place_order(
                    stock_code="NIFTY",
                    exchange_code="NFO",
                    product="options",
                    action=action,
                    order_type="market",
                    quantity=qty,
                    price="",
                    validity="day",
                    disclosed_quantity="0",
                    expiry_date=f'{self.EXPIRY}T06:00:00.000Z',
                    right=right,
                    strike_price=strike
                )
                time.sleep(4)
                if order_detail.get('Success'):
                    order_id = order_detail['Success']['order_id']
                    trade_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
                    if trade_detail.get('Success'):
                        price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
                        print(f"Order placed: {action} {right} at strike {strike} for {price}")
                        return price
                raise ValueError("Order placement failed")
            except Exception as e:
                attempt += 1
                logger.error(f"Attempt {attempt} - Error placing {action} order for {right} {strike}: ", exc_info = e)
                print(f"Attempt {attempt} - Error placing {action} order for {right} {strike}: {str(e)}")
                if "login" in str(e).lower() or "session" in str(e).lower() or attempt == max_attempts:
                    if attempt < max_attempts:
                        logger.info("Login credentials error detected, attempting re-login")
                        print("Login credentials error detected, attempting re-login")
                        try:
                            self.breeze.generate_session(
                                api_secret=self.params["api_secret"],
                                session_token=self.params["session_token"]
                            )
                            logger.info("Re-login successful")
                            print("Re-login successful")
                            time.sleep(2)
                        except Exception as re_login_e:
                            logger.error(f"Re-login failed: ", exc_info = re_login_e)
                            print(f"Re-login failed: {str(re_login_e)}")
                            if attempt == max_attempts:
                                logger.error(f"Max attempts reached, order placement failed: ", exc_info = re_login_e)
                                print(f"Max attempts reached, order placement failed: {str(re_login_e)}")
                                raise
                    else:
                        logger.error(f"Max attempts reached, order placement failed: ", exc_info = e)
                        print(f"Max attempts reached, order placement failed: {str(e)}")
                        raise
                else:
                    time.sleep(2)
        raise Exception("Order placement failed after maximum attempts")
    
    # Square off function
    def square_off_order(self,action, right, strike, qty):
        max_attempts = 2
        attempt = 0
        while attempt < max_attempts:
            try:
                order_detail = self.breeze.square_off(
                    stock_code="NIFTY",
                    exchange_code="NFO",
                    product="options",
                    action=action,
                    order_type="market",
                    quantity=qty,
                    price="",
                    validity="day",
                    disclosed_quantity="0",
                    expiry_date=f'{self.EXPIRY}T06:00:00.000Z',
                    right=right,
                    strike_price=strike
                )
                time.sleep(4)
                if order_detail.get('Success'):
                    order_id = order_detail['Success']['order_id']
                    trade_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
                    if trade_detail.get('Success'):
                        price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
                        print(f"Order placed: {action} {right} at strike {strike} for {price}")
                        return price
                raise ValueError("Order placement failed")
            except Exception as e:
                attempt += 1
                logger.error(f"Attempt {attempt} - Error placing {action} order for {right} {strike}: ", exc_info = e)
                print(f"Attempt {attempt} - Error placing {action} order for {right} {strike}: {str(e)}")
                if "login" in str(e).lower() or "session" in str(e).lower() or attempt == max_attempts:
                    if attempt < max_attempts:
                        logger.info("Login credentials error detected, attempting re-login")
                        print("Login credentials error detected, attempting re-login")
                        try:
                            self.breeze.generate_session(
                                api_secret=self.params["api_secret"],
                                session_token=self.params["session_token"]
                            )
                            logger.info("Re-login successful")
                            print("Re-login successful")
                            time.sleep(2)
                        except Exception as re_login_e:
                            logger.error(f"Re-login failed: ", exc_info = re_login_e)
                            print(f"Re-login failed: {str(re_login_e)}")
                            if attempt == max_attempts:
                                logger.error(f"Max attempts reached, order placement failed: ", exc_info = re_login_e)
                                print(f"Max attempts reached, order placement failed: {str(re_login_e)}")
                                raise
                    else:
                        logger.error(f"Max attempts reached, order placement failed: ", exc_info = e)
                        print(f"Max attempts reached, order placement failed: {str(e)}")
                        raise
                else:
                    time.sleep(2)
        raise Exception("Order placement failed after maximum attempts")
    
    

    def write_to_csv(self,data):
        headers = ['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium', 
                'Exit Time', 'Exit premium', 'PnL', 'Quantity']
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
                self.initiate_ws('0','others')
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)

                        now = datetime.now()
                        current_time = now.time()
                
                        if self.TIME_1 <= current_time <= self.TIME_2:
                            today = now.strftime('%Y-%m-%d')
                            yesterday = (now - timedelta(days=5)).strftime('%Y-%m-%d')
                    
                            # CE Entry Logic
                            if self.order == 0 and now.second == 1:
                                try:
                                    olhc = self.fetch_historical(yesterday, today)
                                    last_row = olhc.iloc[-1]
                                    second_last = olhc.iloc[-2]
                                    third_last = olhc.iloc[-3]
                                    rsi_ce_condition = (last_row['RSI_14'] > 70) and (second_last['RSI_14'] > 70)
                                    macd_ce_condition = (last_row['MACD'] > second_last['MACD'] and second_last['MACD'] > third_last['MACD'])
                                    rsi_pe_condition = (last_row['RSI_14'] < 30 and second_last['RSI_14'] < 30)
                                    macd_pe_condition = (last_row['MACD'] < second_last['MACD'] and second_last['MACD'] < third_last['MACD'])
                                    
                                    if rsi_ce_condition and macd_ce_condition:
                                        nifty = self.nifty_spot()
                                        atm = round(nifty / 50) * 50
                                        entry_time = now.strftime('%H:%M:%S')
                                        buy_price = self.place_order("buy", "call", atm, self.QTY)
                                        logger.info(f"Call entry at {buy_price} for strike {atm}")
                                        print(f"CE Entry executed at {buy_price}")
                                        self.initiate_ws(atm, 'call')
                                        data_key = str(atm) + '_Call'
                                        self.order = 1
                                        self.sl = buy_price - 15  #change
                                        ce_entry_data = [today, entry_time, atm, f"call_{self.EXPIRY}", buy_price]
                                    else:
                                        print(now, "No CE position taken - Conditions not met")

                                    if rsi_pe_condition and macd_pe_condition:
                                        nifty = self.nifty_spot()
                                        atm = round(nifty / 50) * 50
                                        entry_time = now.strftime('%H:%M:%S')
                                        buy_pe_price = self.place_order("buy", "put", atm, self.QTY)
                                        logger.info(f"Put entry at {buy_pe_price} for strike {atm}")
                                        print(f"PE Entry executed at {buy_pe_price}")
                                        self.initiate_ws(atm, 'put')
                                        data_key_pe = str(atm) + '_Put'
                                        self.order = -1
                                        self.sl = buy_pe_price - 15 #change
                                        pe_entry_data = [today, entry_time, atm, f"put_{self.EXPIRY}", buy_pe_price]
                                    else:
                                        print(now, "No PE position taken - Conditions not met")
                                except Exception as e:
                                    logger.error(f"Error in PE entry logic: ", exc_info = e)
                                    print(f"Error in PE entry: {str(e)}")
                    
                            # CE Exit Logic
                            if self.order == 1:
                                try:
                                    if now.second == 1:
                                        olhc = self.fetch_historical(yesterday, today)
                                    
                                    last_row = olhc.iloc[-1]
                                    second_last = olhc.iloc[-2]
                                    third_last = olhc.iloc[-3]
                                    macd_ce_exit = (last_row['MACD'] < second_last['MACD'] and second_last['MACD'] < third_last['MACD'])
                                    
                                    if data_key in self.tick_data:
                                        leg1_cmp = self.tick_data[data_key]

                                        print(f"CE Current Market Price: {leg1_cmp}")
                                        new_sl = leg1_cmp - 15
                                        self.sl = max(self.sl, new_sl)
                                        sl_hit=None
                                        if leg1_cmp<= self.sl:
                                            sl_hit = leg1_cmp<= self.sl
                                            logger.info("SL Hit for CE")
                                            print("CE SL Hit")
                                        
                                        time_exit = current_time >= datetime.strptime("15:15", "%H:%M").time()
                                        print(f"CE Exit Conditions - RSI: {last_row['RSI_14']} (macd: {macd_ce_exit}), "
                                            f"Time Exit: {time_exit}, SL: {self.sl}")
                                        
                                        if macd_ce_exit or time_exit or sl_hit:
                                            # sell_price = self.place_order("sell", "call", atm, self.QTY)
                                            sell_price = self.square_off_order("sell", "call", atm, self.QTY)
                                            exit_time = now.strftime('%H:%M:%S')
                                            pnl = round(sell_price - buy_price, 2)
                                            logger.info(f"Call exit, PnL: {pnl}")
                                            print(f"CE Exit executed at {sell_price}, PnL: {pnl}")
                                            self.write_to_csv(ce_entry_data + [exit_time, sell_price, pnl, self.QTY])
                                            self.deactivate_ws(atm, 'call')
                                            self.order = 0
                                        else:
                                            print(now, "No CE exit - Conditions not met")
                                    else:
                                        print(f"Strike:{data_key} not in tick_data:{self.tick_data} ")
                                except Exception as e:
                                    logger.error(f"Error in CE exit logic: ", exc_info = e)
                                    print(f"Error in CE exit: {str(e)}")

                            # PE exit logic
                            elif self.order == -1:
                                try:
                                    if now.second == 1:
                                        olhc = self.fetch_historical(yesterday, today)
                                    
                                    last_row = olhc.iloc[-1]
                                    second_last = olhc.iloc[-2]
                                    third_last = olhc.iloc[-3]
                                    macd_pe_exit = (last_row['MACD'] > second_last['MACD'] and second_last['MACD'] > third_last['MACD'])
                                    
                                    if data_key_pe in self.tick_data:
                                        leg2_cmp = self.tick_data[data_key_pe]
                                        print(f"PE Current Market Price: {leg2_cmp}")

                                        new_sl = leg2_cmp - 15
                                        self.sl = max(self.sl, new_sl)
                                        sl_hit=None
                                        if leg2_cmp<= self.sl:
                                            sl_hit = leg2_cmp<= self.sl
                                            logger.info("SL Hit for PE")
                                            print("PE SL Hit")
                                    
                                        time_exit = current_time >= datetime.strptime("15:19", "%H:%M").time()
                                        print(f"PE Exit Conditions - RSI: {last_row['RSI_14']} (macd:{macd_pe_exit}), "
                                            f"Time Exit: {time_exit}, SL: {self.sl}")
                                        
                                        if macd_pe_exit or time_exit or sl_hit:
                                            # sell_price = self.place_order("sell", "put", atm, self.QTY)
                                            sell_price = self.square_off_order("sell", "put", atm, self.QTY)
                                            exit_time = now.strftime('%H:%M:%S')
                                            pnl = round(sell_price - buy_pe_price, 2)
                                            logger.info(f"PUT exit, PnL: {pnl}")
                                            print(f"PE Exit executed at {sell_price}, PnL: {pnl}")
                                            self.write_to_csv(pe_entry_data + [exit_time, sell_price, pnl, self.QTY])
                                            self.deactivate_ws(atm, 'put')
                                            self.order = 0
                                        else:
                                            print(now, "No PE exit - Conditions not met")
                                    else:
                                        print(f"Strike:{data_key_pe} not in tick_data:{self.tick_data} ")
                                except Exception as e:
                                    logger.error(f"Error in PE exit logic: ", exc_info = e)
                                    print(f"Error in PE exit: {str(e)}")
                            
                        else:
                            print("Outside trading hours")
                        
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