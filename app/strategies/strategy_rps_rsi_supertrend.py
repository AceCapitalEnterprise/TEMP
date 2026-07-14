from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
import time

from datetime import date, datetime, timedelta, time as t
import csv, re, time, math
import os


class StrategyRpsRsiSupertrend(BaseStrategy):
    tick_data=None
    def __init__(self, params):
        super().__init__()

        self.breeze = BreezeConnect(api_key=params["api_key"])
        self.breeze.generate_session(api_secret=params["api_secret"],
                        session_token=params["session_token"])
        
        self.today = datetime.now()
        self.yesterday = self.today - timedelta(days=5)
        self.yesterday = self.yesterday.strftime('%Y-%m-%d')
        self.today = self.today.strftime('%Y-%m-%d')

        self.csv_file = params["csv_file"]
        self.time_1 = t(params["start_hour"], params["start_minute"])
        self.time_2 = t(params["end_hour"], params["end_minute"])

        self.expiry = params["expiry"]
        self.expiry_ws=datetime.strptime(self.expiry, '%Y-%m-%d').strftime('%d-%b-%Y')
        self.qty = params["qty"]
        self.order = 0
        self.order2 = 0
        self.sl = 0
        self.sl2 = 0
        self.breeze.ws_connect()
        self.breeze.on_ticks = self.on_ticks


    def nifty_spot(self):
        nifty = self.breeze.get_quotes(stock_code="NIFTY",
                                exchange_code="NSE",
                                expiry_date=f"{self.today}T06:00:00.000Z",
                                product_type="cash",
                                right="others",
                                strike_price="0")
        time.sleep(0.5)
        if nifty is not None:
            nifty = nifty['Success']
            nifty = pd.DataFrame(nifty)
            nifty = nifty['ltp'][0]
            return nifty
        else:
            self.nifty_spot()
            print("Error in fetching nifty spot data")


    def option_historical(self, ce_or_pe, strike):
        option_data = self.breeze.get_historical_data_v2(interval="1minute",
                                                  from_date= f"{self.yesterday}T07:00:00.000Z",
                                                  to_date= f"{self.today}T17:00:00.000Z",
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
            self.option_historical(ce_or_pe, strike)
            print("Error in fetching historical data")


    def on_ticks(self, ticks):
        data = ticks['strike_price']+'_'+ticks['right']
        if self.tick_data != None and data in self.tick_data:
            self.tick_data[data]=ticks


    def activate(self,atm,right=''):
        if right=='call':    
        
            leg=self.breeze.subscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                # expiry_date=f'{expiry}T06:00:00.000Z',
                                expiry_date=self.expiry_ws,
                                right="call",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
            print(leg)
            if self.tick_data != None and f'{atm}_Call' not in self.tick_data:
                self.tick_data[f'{atm}_Call']=''

        elif right=='put':
            leg2=self.breeze.subscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry_ws,
                                right="put",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
            print(leg2)
            if self.tick_data != None and f'{atm}_Put' not in self.tick_data:
                self.tick_data[f'{atm}_Put']=''

    def deactivate_ws(self,atm,right=''):
        if right=='call':    
            leg=self.breeze.unsubscribe_feeds(exchange_code="NFO",
                            stock_code="NIFTY",
                            product_type="options",
                            expiry_date=self.expiry_ws,
                            right="call",
                            strike_price=str(atm),
                            get_exchange_quotes=True,
                            get_market_depth=False)
            print(leg)
            if self.tick_data != None and f'{atm}_Call' in self.tick_data and str(atm)==self.tick_data[f'{atm}_Call']:
                self.tick_data.pop(f'{atm}_Call')
        elif right=='put':
            leg2=self.breeze.unsubscribe_feeds(exchange_code="NFO",
                            stock_code="NIFTY",
                            product_type="options",
                            expiry_date=self.expiry_ws,
                            right="put",
                            strike_price=str(atm),
                            get_exchange_quotes=True,
                            get_market_depth=False)
            print(leg2)
            if self.tick_data != None and f'{atm}_Put' in self.tick_data and str(atm)==self.tick_data[f'{atm}_Put']:
                self.tick_data.pop(f'{atm}_Put')


    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )

            try:
                while deployed_strategy.status == "RUNNING":
                    now = datetime.now()
                    db_session.refresh(deployed_strategy)
                    if self.time_1 < t(datetime.now().time().hour, datetime.now().time().minute) < self.time_2:
                        
                        today = datetime.now()
                        yesterday = today - timedelta(days=5)
                        yesterday = yesterday.strftime('%Y-%m-%d')
                        today = today.strftime('%Y-%m-%d')
                        
                        if self.order == 0 and now.second == 1 :
                            nifty = self.nifty_spot() 
                            atm = round(nifty / 50) * 50
                            ce_otm = atm + 100

                            ce_option = self.option_historical("call", ce_otm)
                        
                            ce_option.ta.rsi(close='close', length=14, append=True)
                        
                            supertrend_result = ta.supertrend(ce_option['high'], ce_option['low'], ce_option['close'], length=10, multiplier=2)
                            ce_option['supertrend'] = supertrend_result['SUPERTd_10_2.0']
                        
                            ce_option['volume_avg'] = ce_option['volume'].rolling(window=5).mean()
                            ce_option['volume_check'] = (ce_option['volume'] > 1.5 * ce_option['volume_avg']).astype(int)
                            last_row = ce_option.iloc[-1]
                        
                            if last_row['RSI_14'] > 70 and last_row['supertrend'] == 1 and last_row['volume_check'] == 1 :
                                entry_time = datetime.now().strftime('%H:%M:%S')
                                self.order = 1
                                self.sl = 0
                                self.activate(ce_otm,'call')
                                for j in range(0, 5):
                                    try:
                                        order_detail = self.breeze.place_order(stock_code="NIFTY",
                                                                        exchange_code="NFO",
                                                                        product="options",
                                                                        action="buy",
                                                                        order_type="market",
                                                                        stoploss="",
                                                                        quantity=self.qty,
                                                                        price="",
                                                                        validity="day",
                                                                        disclosed_quantity="0",
                                                                        expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                                        right="call",
                                                                        strike_price=ce_otm)
                                        break
                                    except:
                                        pass
                                time.sleep(5)
                                order_detail = order_detail['Success']
                                order_detail = order_detail['order_id']
                                
                                order_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_detail)
                                order_detail = order_detail['Success']
                                order_detail = pd.DataFrame(order_detail)
                                buy_price = order_detail['execution_price']
                                buy_price = float(buy_price[0])
                                print('call entry', buy_price)
                            else:
                                print(now, 'no call position')
                                
                        if self.order == 1:
                            # time.sleep(20)

                            ce_option = self.option_historical("call", ce_otm)
                            ce_option.ta.rsi(close='close', length=14, append=True)
                        
                            supertrend_result = ta.supertrend(ce_option['high'], ce_option['low'], ce_option['close'], length=10, multiplier=2)
                            ce_option['supertrend'] = supertrend_result['SUPERTd_10_2.0']
                        
                            ce_option['volume_avg'] = ce_option['volume'].rolling(window=5).mean()
                            ce_option['volume_check'] = (ce_option['volume'] > 1.5 * ce_option['volume_avg']).astype(int)
                            last_row = ce_option.iloc[-1]
                            key=str(ce_otm)+'_'+'Call'
                        
                            if self.tick_data != None and key in self.tick_data and self.tick_data[key]!='':
                                leg1_cmp=self.tick_data[key]['last']
                            # leg1 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                            #                                         exchange_code="NFO",
                            #                                         product_type="options",
                            #                                         expiry_date=f'{expiry}T06:00:00.000Z',
                            #                                         right="call",
                            #                                         strike_price=ce_otm)
                            # leg1 = leg1['Success']
                            # leg1 = pd.DataFrame(leg1)
                            # leg1_cmp = float(leg1['ltp'][0])

                                if (leg1_cmp - buy_price)>=10 :
                                    self.sl = 1

                                if self.sl==1 and leg1_cmp <= buy_price:
                                    self.sl = 2
                                    print("SL HITS")
                        

                            
                                if last_row['RSI_14'] < 70 or last_row['supertrend'] != 1 or (t(datetime.now().time().hour, datetime.now().time().minute) == t(15,19)) or self.sl == 2 :
                                    self.order = 0
                                    exit_time = datetime.now().strftime('%H:%M:%S')
                                    order_detail = self.breeze.place_order(stock_code="NIFTY",
                                                                    exchange_code="NFO",
                                                                    product="options",
                                                                    action="sell",
                                                                    order_type="market",
                                                                    stoploss="",
                                                                    quantity=self.qty,
                                                                    price="",
                                                                    validity="day",
                                                                    disclosed_quantity="0",
                                                                    expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                                    right="call",
                                                                    strike_price=ce_otm)
                                    
                                    time.sleep(5)
                                    order_detail = order_detail['Success']
                                    order_detail = order_detail['order_id']
                                    
                                    order_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_detail)
                                    order_detail = order_detail['Success']
                                    order_detail = pd.DataFrame(order_detail)
                                    sell_price = order_detail['execution_price']
                                    sell_price = float(sell_price[0])
                                    pnl = round(sell_price - buy_price, 2)
                                    print('call exit, pnl is:', pnl)
                                    self.deactivate_ws(ce_otm,'call')
                                    try:
                                        with open(self.csv_file, 'x', newline='') as file:
                                            writer = csv.writer(file)
                                            writer.writerow(['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium','Exit Time', 'Exit premium', 'PnL', 'Quantity'])
                                    except FileExistsError:
                                        pass
                                        with open(self.csv_file, 'a', newline='') as file:
                                            writer = csv.writer(file)
                                            writer.writerow([today, entry_time, ce_otm, f'put_{self.expiry}', buy_price, exit_time, sell_price, pnl, self.qty])
                                else:
                                    print(now, 'no call exit')
                                
                                    
                                
                        if self.order2 == 0 and now.second == 1 :
                            time.sleep(1)
                            nifty = self.nifty_spot()
                            atm_strike = round(nifty / 50) * 50
                            pe_otm = atm_strike - 100
                        
                            pe_option = self.option_historical("put", pe_otm)
                        
                            pe_option.ta.rsi(close='close', length=14, append=True)
                        
                            supertrend_result = ta.supertrend(pe_option['high'], pe_option['low'], pe_option['close'], length=10, multiplier=2)
                            pe_option['supertrend'] = supertrend_result['SUPERTd_10_2.0']
                        
                            pe_option['volume_avg'] = pe_option['volume'].rolling(window=5).mean()
                            pe_option['volume_check'] = (pe_option['volume'] > 1.5 * pe_option['volume_avg']).astype(int)
                        
                            last_row_pe = pe_option.iloc[-1]
                        
                            if last_row_pe['RSI_14'] > 70 and last_row_pe['supertrend'] == 1 and last_row_pe['volume_check']==1 :
                                entry_time = datetime.now().strftime('%H:%M:%S')
                                self.order2 = 1
                                self.sl2 = 0
                                self.activate(pe_otm,'put')
                                order_detail = self.breeze.place_order(stock_code="NIFTY",
                                                                exchange_code="NFO",
                                                                product="options",
                                                                action="buy",
                                                                order_type="market",
                                                                stoploss="",
                                                                quantity=self.qty,
                                                                price="",
                                                                validity="day",
                                                                disclosed_quantity="0",
                                                                expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                                right="put",
                                                                strike_price=pe_otm)
                                
                                time.sleep(5)
                                order_detail = order_detail['Success']
                                order_detail = order_detail['order_id']
                                
                                order_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_detail)
                                order_detail = order_detail['Success']
                                order_detail = pd.DataFrame(order_detail)
                                buy_pe_price = order_detail['execution_price']
                                buy_pe_price = float(buy_pe_price[0])
                                print(pe_otm, 'put entry', buy_pe_price)
                            else:
                                print(now, 'no put position')
                                
                        if self.order2 == 1:
                            time.sleep(20)
                            pe_option = self.option_historical("put", pe_otm)
                            pe_option.ta.rsi(close='close', length=14, append=True)
                        
                            supertrend_result = ta.supertrend(pe_option['high'], pe_option['low'], pe_option['close'], length=10, multiplier=2)
                            pe_option['supertrend'] = supertrend_result['SUPERTd_10_2.0']
                        
                            pe_option['volume_avg'] = pe_option['volume'].rolling(window=5).mean()
                            pe_option['volume_check'] = (pe_option['volume'] > 1.5 * pe_option['volume_avg']).astype(int)
                            last_row_pe = pe_option.iloc[-1]
                            key2=str(pe_otm)+'_'+'Put'
                            if self.tick_data != None and key2 in self.tick_data and self.tick_data[key2]!='':
                                leg2_cmp=self.tick_data[key2]['last']
                            # leg2 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                            #                                         exchange_code="NFO",
                            #                                         product_type="options",
                            #                                         expiry_date=f'{expiry}T06:00:00.000Z',
                            #                                         right="put",
                            #                                         strike_price=pe_otm)
                            # leg2 = leg2['Success']
                            # leg2 = pd.DataFrame(leg2)
                            # leg2_cmp = float(leg2['ltp'][0])

                                if (leg2_cmp - buy_pe_price)>=15 :
                                    self.sl2 = 1

                                if self.sl2==1 and leg2_cmp <= buy_pe_price:
                                    self.sl2 = 2
                                    print("SL HITS")
                            
                                if last_row_pe['RSI_14'] < 70 or last_row_pe['supertrend'] != 1 or (t(datetime.now().time().hour, datetime.now().time().minute) == t(15,19)) or self.sl2 == 2 :
                                    self.order2 = 0
                                    exit_time = datetime.now().strftime('%H:%M:%S')
                                    order_detail = self.breeze.place_order(stock_code="NIFTY",
                                                                    exchange_code="NFO",
                                                                    product="options",
                                                                    action="sell",
                                                                    order_type="market",
                                                                    stoploss="",
                                                                    quantity=self.qty,
                                                                    price="",
                                                                    validity="day",
                                                                    disclosed_quantity="0",
                                                                    expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                                    right="put",
                                                                    strike_price=pe_otm)
                                    
                                    time.sleep(5)
                                    order_detail = order_detail['Success']
                                    order_detail = order_detail['order_id']
                                    
                                    order_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_detail)
                                    order_detail = order_detail['Success']
                                    order_detail = pd.DataFrame(order_detail)
                                    sell_pe_price = order_detail['execution_price']
                                    sell_pe_price = float(sell_pe_price[0])
                                    
                                    pnl = round(sell_pe_price - buy_pe_price, 2)
                                    print('put exit, pnl is:', pnl)
                                    self.deactivate_ws(pe_otm,'put')
                                    try:
                                        with open(self.csv_file, 'x', newline='') as file:
                                            writer = csv.writer(file)
                                            writer.writerow(['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium','Exit Time', 'Exit premium', 'PnL', 'Quantity'])
                                    except FileExistsError:
                                        pass
                                        with open(self.csv_file, 'a', newline='') as file:
                                            writer = csv.writer(file)
                                            writer.writerow([today, entry_time, pe_otm, f'put_{self.expiry}', buy_pe_price, exit_time, sell_pe_price, pnl, self.qty])        
                                else:
                                    print(now, 'no put exit')
                    
                    elif t(datetime.now().time().hour, datetime.now().time().minute) > self.time_2:
                        print('exiting')
                        self.breeze.ws_disconnect()
                        deployed_strategy.status="STOPPED"
                        db_session.add(deployed_strategy)
                        db_session.commit()
            except Exception as ex:
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise ex
