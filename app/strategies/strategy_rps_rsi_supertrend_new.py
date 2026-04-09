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
import logging
from ..common.logging_config import create_logger
from tenacity import retry, stop_after_attempt, wait_exponential


logger = create_logger(__name__, 'trading_strategy.log')

# Constants
#TIME_1 = datetime.strptime("09:15", "%H:%M").time()
#TIME_2 = datetime.strptime("15:20", "%H:%M").time()
#EXPIRY = '2025-03-20'
#QTY = 75

# Global variables
#order = 0  # For CE
#order2 = 0  # For PE
#sl = 0    # For CE
#sl2 = 0   # For PE
#spot_price=None
#tick_data={}
#expiry1 = datetime.strptime(EXPIRY, '%Y-%m-%d')

#expiry1 = expiry1.strftime('%d-%b-%Y')


class StrategyRpsRsiSupertrendNew(BaseStrategy):
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
        self.QTY = params["qty"]
        self.csv_file = params["csv_file"]

        # Global variables
        self.order = 0  # For CE
        self.order2 = 0  # For PE
        self.sl = 0    # For CE
        self.sl2 = 0   # For PE
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

            # print(ticks)


    def initiate_ws(self, atm, right=''):
        if right=='call':    
            # print("hello")
            leg=self.breeze.subscribe_feeds(exchange_code="NFO",
                                    stock_code="NIFTY",
                                    product_type="options",
                                    # expiry_date=f'{expiry}T06:00:00.000Z',
                                    expiry_date=self.expiry1,
                                    right="call",
                                    strike_price=str(atm),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)
            self.tick_data[str(atm)+'_Call']=''
        # print(leg)
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
            # print(leg2)
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
    def option_historical(self, ce_or_pe, strike, yesterday, today):
        try:
            option_data = self.breeze.get_historical_data_v2(
                interval="1minute",
                from_date=f"{yesterday}T07:00:00.000Z",
                to_date=f"{today}T17:00:00.000Z",
                stock_code="NIFTY",
                exchange_code="NFO",
                product_type="options",
                expiry_date=f"{self.EXPIRY}T07:00:00.000Z",
                right=ce_or_pe,
                strike_price=strike
            )
            time.sleep(4)
            if option_data.get('Success'):
                df = pd.DataFrame(option_data['Success'])
                print(f"Historical data fetched for {ce_or_pe} at strike {strike}")
                return df
            raise ValueError("No success response from API")
        except Exception as e:
            logger.error(f"Error fetching historical data for {ce_or_pe} {strike}: ", exc_info = e)
            print(f"Error fetching historical data for {ce_or_pe} {strike}: {str(e)}")
            raise


    def place_order(self, action, right, strike, qty):
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
            logger.error(f"Error placing {action} order for {right} {strike}: ", exc_info = e)
            print(f"Error placing {action} order for {right} {strike}: {str(e)}")
            raise


    def write_to_csv(self, data):
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
                            if self.order == 0 and now.second == 0:
                                try:
                                    nifty = self.nifty_spot()
                                    atm = round(nifty / 50) * 50
                                    ce_otm = atm + 100
                                    print(f"CE OTM Strike: {ce_otm}")
                                    
                                    ce_option = self.option_historical("call", ce_otm, yesterday, today)
                                    ce_option.ta.rsi(close='close', length=14, append=True)
                                    supertrend = ta.supertrend(ce_option['high'], ce_option['low'], 
                                                            ce_option['close'], length=10, multiplier=2)
                                    ce_option['supertrend'] = supertrend['SUPERTd_10_2.0']
                                    ce_option['volume_avg'] = ce_option['volume'].rolling(window=5).mean()
                                    ce_option['volume_check'] = (ce_option['volume'] > 1.5 * ce_option['volume_avg']).astype(int)
                                    
                                    last_row = ce_option.iloc[-1]
                                    rsi_condition = last_row['RSI_14'] > 70
                                    supertrend_condition = last_row['supertrend'] == 1
                                    volume_condition = last_row['volume_check'] == 1
                                    print(f"CE Conditions - RSI: {last_row['RSI_14']} (>70: {rsi_condition}), "
                                        f"Supertrend: {last_row['supertrend']} (1: {supertrend_condition}), "
                                        f"Volume Check: {last_row['volume_check']} (1: {volume_condition})")
                                    
                                    if rsi_condition and supertrend_condition and volume_condition:
                                        entry_time = now.strftime('%H:%M:%S')
                                        buy_price = self.place_order("buy", "call", ce_otm, self.QTY)
                                        logger.info(f"Call entry at {buy_price} for strike {ce_otm}")
                                        print(f"CE Entry executed at {buy_price}")
                                        self.initiate_ws(ce_otm,'call')
                                        data_key=str(ce_otm)+'_Call'
                                        self.order = 1
                                        self.sl = 0
                                        ce_entry_data = [today, entry_time, ce_otm, f"call_{self.EXPIRY}", buy_price]
                                    else:
                                        print(f"No CE position taken - Conditions not met")
                                except Exception as e:
                                    logger.error(f"Error in CE entry logic: ", exc_info = e)
                                    print(f"Error in CE entry: {str(e)}")
                            
                            # CE Exit Logic
                            if self.order == 1:
                                try:
                                    time.sleep(20)
                                    ce_option = self.option_historical("call", ce_otm, yesterday, today)
                                    ce_option.ta.rsi(close='close', length=14, append=True)
                                    supertrend = ta.supertrend(ce_option['high'], ce_option['low'], 
                                                            ce_option['close'], length=10, multiplier=2)
                                    ce_option['supertrend'] = supertrend['SUPERTd_10_2.0']
                                    last_row = ce_option.iloc[-1]
                                    
                                    if data_key in self.tick_data:
                                        leg1_cmp=self.tick_data[data_key]
                                        print(f"CE Current Market Price: {leg1_cmp}")
                                        
                                        profit_condition = (leg1_cmp - buy_price) >= 10
                                        if profit_condition:
                                            self.sl = 1
                                            print("CE SL triggered (profit >= 10)")
                                        sl_hit = self.sl == 1 and leg1_cmp <= buy_price
                                        if sl_hit:
                                            self.sl = 2
                                            logger.info("SL Hit for CE")
                                            print("CE SL Hit")
                                        
                                        rsi_exit = last_row['RSI_14'] < 70
                                        supertrend_exit = last_row['supertrend'] != 1
                                        time_exit = current_time >= datetime.strptime("15:15", "%H:%M").time()
                                        print(f"CE Exit Conditions - RSI: {last_row['RSI_14']} (<70: {rsi_exit}), "
                                            f"Supertrend: {last_row['supertrend']} (!=1: {supertrend_exit}), "
                                            f"Time Exit: {time_exit}, SL: {self.sl == 2}")
                                        
                                        if rsi_exit or supertrend_exit or time_exit or self.sl == 2:
                                            sell_price = self.place_order("sell", "call", ce_otm, self.QTY)
                                            exit_time = now.strftime('%H:%M:%S')
                                            pnl = round(sell_price - buy_price, 2)
                                            logger.info(f"Call exit, PnL: {pnl}")
                                            print(f"CE Exit executed at {sell_price}, PnL: {pnl}")
                                            self.write_to_csv(ce_entry_data + [exit_time, sell_price, pnl, self.QTY])
                                            self.deactivate_ws(ce_otm,'call')
                                            self.order = 0
                                        else:
                                            print("No CE exit - Conditions not met")
                                    else:
                                        print(f"Strike:{data_key} not in tick data:{self.tick_data} ")
                                except Exception as e:
                                    logger.error(f"Error in CE exit logic: ", exc_info = e)
                                    print(f"Error in CE exit: {str(e)}")
                            
                            # PE Entry Logic
                            if self.order2 == 0 and now.second == 0:
                                try:
                                    nifty = self.nifty_spot()
                                    atm_strike = round(nifty / 50) * 50
                                    pe_otm = atm_strike - 100
                                    print(f"PE OTM Strike: {pe_otm}")
                                    
                                    pe_option = self.option_historical("put", pe_otm, yesterday, today)
                                    pe_option.ta.rsi(close='close', length=14, append=True)
                                    supertrend = ta.supertrend(pe_option['high'], pe_option['low'], 
                                                            pe_option['close'], length=10, multiplier=2)
                                    pe_option['supertrend'] = supertrend['SUPERTd_10_2.0']
                                    pe_option['volume_avg'] = pe_option['volume'].rolling(window=5).mean()
                                    pe_option['volume_check'] = (pe_option['volume'] > 1.5 * pe_option['volume_avg']).astype(int)
                                    
                                    last_row_pe = pe_option.iloc[-1]
                                    rsi_condition = last_row_pe['RSI_14'] > 70
                                    supertrend_condition = last_row_pe['supertrend'] == 1
                                    volume_condition = last_row_pe['volume_check'] == 1
                                    print(f"PE Conditions - RSI: {last_row_pe['RSI_14']} (>70: {rsi_condition}), "
                                        f"Supertrend: {last_row_pe['supertrend']} (1: {supertrend_condition}), "
                                        f"Volume Check: {last_row_pe['volume_check']} (1: {volume_condition})")
                                    
                                    if rsi_condition and supertrend_condition and volume_condition:
                                        entry_time = now.strftime('%H:%M:%S')
                                        buy_pe_price = self.place_order("buy", "put", pe_otm, self.QTY)
                                        logger.info(f"Put entry at {buy_pe_price} for strike {pe_otm}")
                                        print(f"PE Entry executed at {buy_pe_price}")
                                        self.initiate_ws(pe_otm,'put')
                                        data_key=str(pe_otm)+'_Put'
                                        self.order2 = 1
                                        self.sl2 = 0
                                        pe_entry_data = [today, entry_time, pe_otm, f"put_{self.EXPIRY}", buy_pe_price]
                                    else:
                                        print("No PE position taken - Conditions not met")
                                except Exception as e:
                                    logger.error(f"Error in PE entry logic: ", exc_info = e)
                                    print(f"Error in PE entry: {str(e)}")
                            
                            # PE Exit Logic
                            if self.order2 == 1:
                                try:
                                    time.sleep(20)
                                    pe_option = self.option_historical("put", pe_otm, yesterday, today)
                                    pe_option.ta.rsi(close='close', length=14, append=True)
                                    supertrend = ta.supertrend(pe_option['high'], pe_option['low'], 
                                                            pe_option['close'], length=10, multiplier=2)
                                    pe_option['supertrend'] = supertrend['SUPERTd_10_2.0']
                                    last_row_pe = pe_option.iloc[-1]
                                    
                                    if data_key in self.tick_data:
                                        leg2_cmp=self.tick_data[data_key]
                                        print(f"PE Current Market Price: {leg2_cmp}")
                                        
                                        profit_condition = (leg2_cmp - buy_pe_price) >= 15
                                        if profit_condition:
                                            self.sl2 = 1
                                            print("PE SL triggered (profit >= 15)")
                                        sl_hit = self.sl2 == 1 and leg2_cmp <= buy_pe_price
                                        if sl_hit:
                                            self.sl2 = 2
                                            logger.info("SL Hit for PE")
                                            print("PE SL Hit")
                                        
                                        rsi_exit = last_row_pe['RSI_14'] < 70
                                        supertrend_exit = last_row_pe['supertrend'] != 1
                                        time_exit = current_time >= datetime.strptime("15:19", "%H:%M").time()
                                        print(f"PE Exit Conditions - RSI: {last_row_pe['RSI_14']} (<70: {rsi_exit}), "
                                            f"Supertrend: {last_row_pe['supertrend']} (!=1: {supertrend_exit}), "
                                            f"Time Exit: {time_exit}, SL: {self.sl2 == 2}")
                                        
                                        if rsi_exit or supertrend_exit or time_exit or self.sl2 == 2:
                                            sell_pe_price = self.place_order("sell", "put", pe_otm, self.QTY)
                                            exit_time = now.strftime('%H:%M:%S')
                                            pnl = round(sell_pe_price - buy_pe_price, 2)
                                            logger.info(f"Put exit, PnL: {pnl}")
                                            print(f"PE Exit executed at {sell_pe_price}, PnL: {pnl}")
                                            self.write_to_csv(pe_entry_data + [exit_time, sell_pe_price, pnl, self.QTY])
                                            self.deactivate_ws(pe_otm,'put')
                                            self.order2 = 0
                                        else:
                                            print("No PE exit - Conditions not met")
                                    else:
                                        print(f"Strike:{data_key} not in tick data:{self.tick_data} ")
                                except Exception as e:
                                    logger.error(f"Error in PE exit logic: ", exc_info = e)
                                    print(f"Error in PE exit: {str(e)}")
                        else:
                            print("Outside trading hours")
                        
                        time.sleep(1)  # Prevent CPU overload
                        
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
