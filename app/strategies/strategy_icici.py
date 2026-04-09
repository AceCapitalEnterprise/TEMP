from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import urllib
import numpy as np
import pandas as pd
import pandas_ta as ta
import time

from datetime import date, datetime, timedelta, time as t
import csv, re, time, math
import os


class StrategyICICI(BaseStrategy):
    one_tick=None
    def __init__(self, params):
        super().__init__()

        self.breeze = BreezeConnect(api_key=params["api_key"])
        self.breeze.generate_session(api_secret=params["api_secret"],
                        session_token=params["session_token"])

        self.csv_file = params["csv_file"]
        self.time_1 = t(params["start_hour"], params["start_minute"])
        self.time_2 = t(params["end_hour"], params["end_minute"])

        self.expiry = params["expiry"]
        self.expiry1=datetime.strptime(self.expiry, '%Y-%m-%d').strftime('%d-%b-%Y')
        self.fut_expiry = params["fut_expiry"]
        self.order = 0
        self.order2 = 0
        self.total_pnl = 0
        self.SL = 0
        self.SL2 = 0
        self.breeze.ws_connect()
        self.breeze.on_ticks = self.on_ticks

    def on_ticks(self, ticks):
        print("-------------------------------------------------------------")
        self.one_tick=ticks

        print(ticks)


    def initiate_ws(self, atm,right=''):
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
            # print(leg2)
        

    def deactivate_ws(self, atm,right=''):
        if right=='call':    
            self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry1,
                                right="call",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        elif right=='put':
            self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry1,
                                right="put",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)



    def adjust_trailing_sl(premium, sl, order):
        """Adjust the trailing stop-loss based on the current price and factor."""
        if order in [1, -1]:
            new_sl = premium - 15 
            return max(new_sl, sl)


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
                        
                        if self.order == 0 and now.second == 0:
                            volume_condition = 0

                            data=None
                            for j in range(0, 5):
                                try:
                                    data = self.breeze.get_historical_data_v2(interval="1minute",
                                                                        from_date=f"{yesterday}T00:00:00.000Z",
                                                                        to_date=f"{today}T17:00:00.000Z",
                                                                        stock_code="NIFTY",
                                                                        exchange_code="NFO",
                                                                        product_type="futures",
                                                                        expiry_date=f'{self.fut_expiry}T07:00:00.000Z',
                                                                        right="others",
                                                                        strike_price="0")
                                    break
                                except:
                                    pass

                            olhc = data['Success']
                            olhc = pd.DataFrame(olhc)
                            olhc['datetime'] = pd.to_datetime(olhc['datetime'])
                            olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                                        (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
                            regime1 = olhc[-31:-21]['volume'].mean()
                            regime2 = olhc[-21:-11]['volume'].mean()
                            regime3 = olhc[-11:-1]['volume'].mean()
                            current_volume = olhc.iloc[-1]['volume']
                            
                            if regime1 <= regime2 and regime2 <= regime3 and regime3 <= current_volume:
                                volume_condition = 1
                                
                            nifty_spot = olhc.iloc[-1]['close']
                            atm = round(nifty_spot / 50) * 50
                            ce_otm = atm + 100
                            # pe_otm = atm + 100
                            
                            # Call option data
                            ce_option=None
                            for j in range(0, 5):
                                try:
                                    ce_option = self.breeze.get_historical_data_v2(interval="1minute",
                                                                            from_date=f"{yesterday}T07:00:00.000Z",
                                                                            to_date=f"{today}T17:00:00.000Z",
                                                                            stock_code="NIFTY",
                                                                            exchange_code="NFO",
                                                                            product_type="options",
                                                                            expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                                            right="call",
                                                                            strike_price=ce_otm)
                                    break
                                except:
                                    pass
                            ce_option = ce_option['Success']
                            ce_option = pd.DataFrame(ce_option)
                        
                            ce_option.ta.rsi(close='close', length=14, append=True)
                            ce_option['ATR'] = ta.atr(ce_option['high'], ce_option['low'], ce_option['close'], length=14)
                        
                            supertrend_result = ta.supertrend(ce_option['high'], ce_option['low'], ce_option['close'], length=10, multiplier=2)
                            ce_option['supertrend'] = supertrend_result['SUPERTd_10_2.0']
                        
                            ce_option['volume_avg'] = ce_option['volume'].rolling(window=5).mean()
                            ce_option['volume_check'] = (ce_option['volume'] > 1.5 * ce_option['volume_avg']).astype(int)
                        
                            # Put option data
                            for j in range(0, 5):
                                try:
                                    pe_option = self.breeze.get_historical_data_v2(interval="1minute",
                                                                            from_date=f"{yesterday}T07:00:00.000Z",
                                                                            to_date=f"{today}T17:00:00.000Z",
                                                                            stock_code="NIFTY",
                                                                            exchange_code="NFO",
                                                                            product_type="options",
                                                                            expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                                            right="put",
                                                                            strike_price=ce_otm)
                                    break
                                except:
                                    pass
                            pe_option = pe_option['Success']
                            pe_option = pd.DataFrame(pe_option)

                            last_row_call = ce_option.iloc[-2]
                            # last_row_put = pe_option.iloc[-2]
                            # print(last_row_call,'-------------------',last_row_put)
                            atr_call = last_row_call['ATR'] * 1.5
                            # atr_put = last_row_put['ATR'] * 1.5

                            # Entry condition for Call
                            if (last_row_call['RSI_14'] > 70 and last_row_call['supertrend'] == 1 and
                                last_row_call['volume_check'] == 1 and
                                ce_option.iloc[-1]['high'] > ce_option.iloc[-2]['high'] and
                                pe_option.iloc[-1]['low'] < pe_option.iloc[-2]['low']):
                                self.initiate_ws(atm=ce_otm,right='call')
                                entry_time = datetime.now().strftime('%H:%M:%S')
                                self.order = 1
                                initial_point = 0
                                # leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                #                                      exchange_code="NFO",
                                #                                      product_type="options",
                                #                                      expiry_date=f'{expiry}T06:00:00.000Z',
                                #                                      right="call",
                                #                                      strike_price=ce_otm)
                                
                                time.sleep(4)
                                # leg = leg['Success']
                                # leg = pd.DataFrame(leg)
                                # buy_price = float(leg['ltp'][0])
                                if self.one_tick['right']=='Call':
                                    buy_price=float(self.one_tick['last'])
                                self.SL = buy_price - atr_call
                                print('call entry', atm, 'at', buy_price)


                            else:
                                print(now,"no call position")

                            # Entry condition for Put
                        if self.order2 == 0 and now.second == 0 :
                            volume_condition = 0
                            for j in range(0, 5):
                                try:
                                    data = self.breeze.get_historical_data_v2(interval="1minute",
                                                                    from_date= f"{yesterday}T00:00:00.000Z",
                                                                    to_date= f"{today}T17:00:00.000Z",
                                                                    stock_code="NIFTY",
                                                                    exchange_code="NFO",
                                                                    product_type="futures",
                                                                    expiry_date=f'{self.fut_expiry}T07:00:00.000Z',
                                                                    right="others",
                                                                    strike_price="0")
                                    break
                                except:
                                    pass
                        
                            olhc = data['Success']
                            olhc = pd.DataFrame(olhc)
                            olhc['datetime'] = pd.to_datetime(olhc['datetime'])
                            olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                                        (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
                            regime1 = olhc[-31:-21]['volume'].mean()
                            regime2 = olhc[-21:-11]['volume'].mean()
                            regime3 = olhc[-11:-1]['volume'].mean()
                            current_volume = olhc.iloc[-1]['volume']
                            
                            if regime1 <= regime2 and regime2 <= regime3 and regime3 <= current_volume :
                                volume_condition = 1
                                
                            nifty_spot = olhc.iloc[-1]['close']
                            atm = round(nifty_spot / 50) * 50
                            pe_otm = atm - 100
                            
                            for j in range(0, 5):
                                try:
                                    p_option = self.breeze.get_historical_data_v2(interval="1minute",
                                                            from_date= f"{yesterday}T07:00:00.000Z",
                                                            to_date= f"{today}T17:00:00.000Z",
                                                            stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                            right="put",
                                                            strike_price=pe_otm)
                                    break
                                except:
                                    pass
                            p_option = p_option['Success']
                            p_option = pd.DataFrame(p_option)
                        
                            p_option.ta.rsi(close='close', length=14, append=True)
                            p_option['ATR'] = ta.atr(p_option['high'], p_option['low'], p_option['close'], length=14)
                        
                            supertrend_result = ta.supertrend(p_option['high'], p_option['low'], p_option['close'], length=10, multiplier=2)
                            p_option['supertrend'] = supertrend_result['SUPERTd_10_2.0']
                        
                            p_option['volume_avg'] = p_option['volume'].rolling(window=5).mean()
                            p_option['volume_check'] = (p_option['volume'] > 1.5 * p_option['volume_avg']).astype(int)
                        
                            last_row_pe = p_option.iloc[-2]
                            atr_pe = last_row_pe['ATR']*1.5

                            for j in range(0, 5):
                                try:
                                    c_option = self.breeze.get_historical_data_v2(interval="1minute",
                                                                            from_date=f"{yesterday}T07:00:00.000Z",
                                                                            to_date=f"{today}T17:00:00.000Z",
                                                                            stock_code="NIFTY",
                                                                            exchange_code="NFO",
                                                                            product_type="options",
                                                                            expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                                            right="call",
                                                                            strike_price=pe_otm)
                                    break
                                except:
                                    pass
                            c_option = c_option['Success']
                            c_option = pd.DataFrame(c_option)
                            

                            
                            if (last_row_pe['RSI_14'] > 70 and last_row_pe['supertrend'] == 1 and
                                last_row_pe['volume_check'] == 1 and
                                p_option.iloc[-1]['high'] > p_option.iloc[-2]['high'] and
                                c_option.iloc[-1]['low'] < c_option.iloc[-2]['low']):
                            
                                entry_time = datetime.now().strftime('%H:%M:%S')
                                self.order2 = 1
                                initial_point2 = 0
                                # leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                #                                      exchange_code="NFO",
                                #                                      product_type="options",
                                #                                      expiry_date=f'{expiry}T06:00:00.000Z',
                                #                                      right="put",
                                #                                      strike_price=pe_otm)
                                self.initiate_ws(atm=pe_otm,right='put')
                                time.sleep(5)
                                # leg = leg['Success']
                                # leg = pd.DataFrame(leg)
                                # buy_price_pe = float(leg['ltp'][0])
                                if self.one_tick['right']=='Put':
                                    buy_price_pe=float(self.one_tick['last'])
                                self.SL2 = buy_price_pe - atr_pe
                                print('put entry', pe_otm, 'at', buy_price_pe)
                            else:
                                print(now,'no put position')
                            # Other exit and trade management logic remains the same.
                        if self.order2 == 1:
                            # time.sleep(20)
                            for j in range(0, 5):
                                try:
                                    put_option = self.breeze.get_historical_data_v2(interval="1minute",
                                                            from_date= f"{yesterday}T07:00:00.000Z",
                                                            to_date= f"{today}T17:00:00.000Z",
                                                            stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                            right="put",
                                                            strike_price=pe_otm)
                                    break
                                except:
                                    pass
                            put_option = put_option['Success']
                            put_option = pd.DataFrame(put_option)
                        
                            put_option.ta.rsi(close='close', length=14, append=True)
                            put_option['ATR'] = ta.atr(put_option['high'], put_option['low'], put_option['close'], length=14)
                        
                            supertrend_result = ta.supertrend(put_option['high'], put_option['low'], put_option['close'], length=10, multiplier=2)
                            put_option['supertrend'] = supertrend_result['SUPERTd_10_2.0']
                        
                            put_option['volume_avg'] = put_option['volume'].rolling(window=5).mean()
                            put_option['volume_check'] = (put_option['volume'] > 1.5 * put_option['volume_avg']).astype(int)
                        
                            last_row = put_option.iloc[-1]
                            
                            # leg2 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                            #                                             exchange_code="NFO",
                            #                                             product_type="options",
                            #                                             expiry_date=f'{expiry}T06:00:00.000Z',
                            #                                             right="put",
                            #                                             strike_price=pe_otm)
                            # leg2 = leg2['Success']
                            # leg2 = pd.DataFrame(leg2)
                            # leg2 = float(leg2['ltp'][0])
                            if self.one_tick['right']=='Put':
                                leg2=float(self.one_tick['last'])
                            if leg2 - buy_price_pe > initial_point2 :
                                initial_point2 = leg2 - buy_price_pe
                                self.SL2 = leg2 - atr_pe
                            
                            
                            if last_row['RSI_14'] < 70 or last_row['supertrend'] != 1 or leg2 <= self.SL2 :
                                self.order2 = 0
                                exit_time = datetime.now().strftime('%H:%M:%S')
                                sell_price = leg2
                                self.deactivate_ws(atm=pe_otm,right='put')
                                pnl = round(sell_price - buy_price_pe, 2)
                                print('put exit, pnl is:', pnl)
                                
                                write_data=pd.DataFrame([[today, entry_time, pe_otm, 'put', buy_price_pe, exit_time, sell_price, pnl]],columns=['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium','Exit Time', 'Exit premium', 'PnL'])
                                if os.path.exists(self.csv_file):
                                    write_data.to_csv(self.csv_file,header=False,mode='a',index=False)
                                else:
                                    write_data.to_csv(self.csv_file,header=True,index=False)
                                
                            else:
                                print(now, 'no put exit')
                        if self.order == 1:
                            # time.sleep(20)
                            for j in range(0, 5):
                                try:
                                    call_option = self.breeze.get_historical_data_v2(interval="1minute",
                                                            from_date= f"{yesterday}T07:00:00.000Z",
                                                            to_date= f"{today}T17:00:00.000Z",
                                                            stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                            right="call",
                                                            strike_price=ce_otm)
                                    
                                    break
                                except:
                                    pass
                            call_option = call_option['Success']
                            call_option = pd.DataFrame(call_option)
                        
                            call_option.ta.rsi(close='close', length=14, append=True)
                            call_option['ATR'] = ta.atr(call_option['high'], call_option['low'], call_option['close'], length=14)
                        
                            supertrend_result = ta.supertrend(call_option['high'], call_option['low'], call_option['close'], length=10, multiplier=2)
                            call_option['supertrend'] = supertrend_result['SUPERTd_10_2.0']
                        
                            call_option['volume_avg'] = call_option['volume'].rolling(window=5).mean()
                            call_option['volume_check'] = (call_option['volume'] > 1.5 * call_option['volume_avg']).astype(int)
                        
                            last_row_c = call_option.iloc[-1]
                            # atr=last_row['ATR']
                            
                            # leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                            #                                             exchange_code="NFO",
                            #                                             product_type="options",
                            #                                             expiry_date=f'{expiry}T06:00:00.000Z',
                            #                                             right="call",
                            #                                             strike_price=ce_otm)
                            # leg = leg['Success']
                            # leg = pd.DataFrame(leg)
                            # leg = float(leg['ltp'][0])
                            leg=float(self.one_tick['last'])
                            
                            if leg - buy_price > initial_point :
                                initial_point = leg - buy_price
                                self.SL = leg - atr_call
                            
                            
                            if last_row_c['RSI_14'] < 70 or last_row_c['supertrend'] != 1 or leg <= self.SL :
                                self.order = 0
                                exit_time = datetime.now().strftime('%H:%M:%S')
                                sell_price = leg
                                self.deactivate_ws(atm=ce_otm,right='call')
                                pnl = round(sell_price - buy_price, 2)
                                print('call exit, pnl is:', pnl)
                                write_data=pd.DataFrame([[today, entry_time, ce_otm, 'call', buy_price, exit_time, sell_price, pnl]],columns=['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium','Exit Time', 'Exit premium', 'PnL'])
                                
                                if os.path.exists(self.csv_file):
                                    write_data.to_csv(self.csv_file,header=False,mode='a',index=False)
                                else:
                                    write_data.to_csv(self.csv_file,header=True,index=False)
                                
                            else:
                                print(now, 'no call exit')
                    
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
