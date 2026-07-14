from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine
from breeze_connect import BreezeConnect
import urllib




import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta, time as t
import csv, re, time, math
import time,os

import warnings
warnings.filterwarnings("ignore")

class StrategyORB(BaseStrategy):
    one_tick=None
    def __init__(self):
        super().__init__()
        breeze = BreezeConnect(api_key="d96783Qp368558*55FI36Z24W0ET39Lf")
        breeze.generate_session(api_secret="58836g597W4l8977h7~%eX9967^807x3",
                        session_token="49286062")
        time_1 = t(9, 30)
        time_2 = t(15, 30)
        order = 0
        expiry = '2024-11-28'
        expiry1 = '28-Nov-2024'
        fut_expiry = '2024-11-28'
        SL = 5
        path="unclosed_positions_directional.csv"
        if os.path.exists(path):
            positions_df=pd.read_csv(path)
        else:
            positions = []
            positions_df = pd.DataFrame(columns=['datetime', 'action', 'strike', 'premium', 'trailing_sl'])
        breeze.ws_connect()

    def on_ticks(ticks):
        global one_tick
        one_tick=ticks

    def initiate_ws(self, CE_or_PE, strike_price):
        leg = self.breeze.subscribe_feeds(exchange_code="NFO",
                                    stock_code="NIFTY",
                                    product_type="options",
                                    expiry_date=self.expiry1,
                                    right=CE_or_PE,
                                    strike_price=str(strike_price),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)
        # time.sleep(2)


    def deactivate_ws(self, CE_or_PE,strike_price):
        self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                    stock_code="NIFTY",
                                    product_type="options",
                                    expiry_date=self.expiry1,
                                    right=CE_or_PE,
                                    strike_price=str(strike_price),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)


    def get_current_market_price(CE_or_PE, strike_price):
        global current_price
        if one_tick is not None and (CE_or_PE==one_tick['right'] and strike_price==one_tick['strike_price']) :
            current_price=one_tick['last']
            return current_price
        return None

    def update_trailing_sl(self, positions_df):
        positions_to_exit = []

        for index, position in positions_df.iterrows():
            current_price = self.get_current_market_price(position['CE_or_PE'], position['strike'])

            
            if current_price is not None and float(current_price) >= position['trailing_sl']:
                current_price=float(current_price)
                positions_to_exit.append(index)
                time = datetime.now().strftime('%H:%M:%S')
                print('position exit')
                self.deactivate_ws(position['CE_or_PE'], position['strike'])
                csv_file = "Directional_selling.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([self.today, time, position['strike'], position['CE_or_PE'], 'Buy', -(current_price)])

            elif current_price is not None and current_price < position['trailing_sl'] - position['premium']:
                positions_df.at[index, 'trailing_sl'] = current_price + position['premium']
                
        for index in positions_to_exit:
            positions_df.drop(index, inplace=True)

        return positions_df



    def closest_put_otm(self):
        strikes = ["atm_strike-50", "atm_strike-100", "atm_strike-150", "atm_strike-200", "atm_strike-250", "atm_strike-300", "atm_strike-350", "atm_strike-400", "atm_strike-450", "atm_strike-500", "atm_strike-550", "atm_strike-600", "atm_strike-650", "atm_strike-700", "atm_strike-750", "atm_strike-800", "atm_strike-850", "atm_strike-900"]
                
        ltps = []

        for strike in strikes:
            for j in range(0, 5):
                try:
                    leg = self.breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                            right="put",
                                                            strike_price=strike)
                    leg_df = leg['Success']
                    leg_df = pd.DataFrame(leg_df)
                    ltp_value = float(leg_df['ltp'])
                    break
                except:
                    pass
        
            
            ltps.append({'strike_price': strike, 'ltp': ltp_value})
                        

        target_ltp = 12
        closest_strike_pe = None
        min_diff = float('inf')

        for ltp_data in ltps:
            ltp = ltp_data['ltp']
            diff = abs(ltp - target_ltp)
            if diff < min_diff:
                min_diff = diff
                closest_strike_pe = ltp_data['strike_price']
                
        return closest_strike_pe



    def closest_call_otm(self):
        strikes = ["atm_strike+50", "atm_strike+100", "atm_strike+150", "atm_strike+200", "atm_strike+250", "atm_strike+300", "atm_strike+350", "atm_strike+400", "atm_strike+450", "atm_strike+500", "atm_strike+550", "atm_strike+600", "atm_strike+650", "atm_strike+700", "atm_strike+750", "atm_strike+800", "atm_strike+850", "atm_strike+900"]
                
        ltps = []


        for strike in strikes:
            for j in range(0, 5):
                try:
                    leg = self.breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                            right="call",
                                                            strike_price=strike)
                    leg_df = leg['Success']
                    leg_df = pd.DataFrame(leg_df)
                    break
                except:
                    pass
        
            ltp_value = float(leg_df['ltp'])
            ltps.append({'strike_price': strike, 'ltp': ltp_value})
                        

        target_ltp = 12
        closest_strike_ce = None
        min_diff = float('inf')

        for ltp_data in ltps:
            ltp = ltp_data['ltp']
            diff = abs(ltp - target_ltp)
            if diff < min_diff:
                min_diff = diff
                closest_strike_ce = ltp_data['strike_price']
                
        return closest_strike_ce



    def check_profit_target_and_add_position(self, positions_df):
        if not positions_df.empty:
            last_position = positions_df.iloc[-1]
            # initiate_ws(last_position['CE_or_PE'],last_position['strike'])
            
            current_price = self.get_current_market_price(last_position['CE_or_PE'], last_position['strike'])

            target_price = last_position['premium'] * 0.75
            print(f"Current Price: {current_price}, Target Price: {target_price}")

            if current_price is not None and ((2.5) < float(current_price) <= target_price):
                current_price=float(current_price)
                for j in range(5):
                    try:
                        nifty_spot_response = self.breeze.get_quotes(stock_code="NIFTY", exchange_code="NSE",
                                                                expiry_date=f"{self.today}T06:00:00.000Z",
                                                                product_type="cash", right="others", strike_price="0")
                        nifty_spot = nifty_spot_response['Success']
                        nifty_spot = pd.DataFrame(nifty_spot)
                        nifty_spot_price = nifty_spot['ltp'][0]
                        print(f"Nifty Spot Price: {nifty_spot_price}")
                        break
                    except Exception as e:
                        print(f"Error fetching Nifty spot: {e}")
                        continue

                atm = round(nifty_spot_price / 50) * 50

                if last_position['CE_or_PE'] == 'put':
                    closest_strike_pe = self.closest_put_otm()
                    for j in range(5):
                        try:
                            leg_response = self.breeze.get_option_chain_quotes(stock_code="NIFTY", exchange_code="NFO",
                                                                        product_type="options", expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                                        right="put", strike_price=closest_strike_pe)
                            leg = leg_response['Success']
                            leg = pd.DataFrame(leg)
                            leg_price = float(leg['ltp'][0])
                            print(f"Leg Price for Put: {leg_price}")
                            break
                        except Exception as e:
                            print(f"Error fetching Put leg: {e}")
                            continue

                    new_position = {
                        'datetime': datetime.now(),
                        'action': 'sell',
                        'strike': closest_strike_pe,
                        'CE_or_PE': 'put',
                        'premium': leg_price,
                        'trailing_sl': 2*leg_price
                    }

                else:
                    closest_call_ce = self.closest_call_otm()
                    for j in range(10):
                        try:
                            leg_response = self.breeze.get_option_chain_quotes(stock_code="NIFTY", exchange_code="NFO",
                                                                        product_type="options", expiry_date=f'{self.expiry}T06:00:00.000Z',
                                                                        right="call", strike_price=closest_call_ce)
                            leg = leg_response['Success']
                            leg = pd.DataFrame(leg)
                            leg_price = float(leg['ltp'][0])
                            print(f"Leg Price for Call: {leg_price}")
                            break
                        except Exception as e:
                            print(f"Error fetching Call leg: {e}")
                            continue

                    new_position = {
                        'datetime': datetime.now(),
                        'action': 'sell',
                        'strike': closest_call_ce,
                        'CE_or_PE': 'call',
                        'premium': leg_price,
                        'trailing_sl': 2*leg_price
                    }

                with open(self.csv_file, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([self.today, datetime.now().strftime('%H:%M:%S'), new_position['strike'], new_position['CE_or_PE'], 'Sell', leg_price])

                # Create DataFrame for new position and concatenate
                new_position_df = pd.DataFrame([new_position])
                positions_df = pd.concat([positions_df, new_position_df], ignore_index=True)
                print(f"New Position Added: {new_position}")

            # Debug: Print the updated positions_df
            print(positions_df)
            # deactivate_ws(last_position['CE_or_PE'],last_position['strike'])
        return positions_df


        
    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            while deployed_strategy.status == "RUNNING":
                now = datetime.now()
                if t(9, 35)<=t(datetime.now().time().hour, datetime.now().time().minute)<t(9, 46) and now.second == 0 and positions_df.empty :
                    time.sleep(2)
                    today = datetime.now().strftime("%Y-%m-%d")
                    #yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
                    for j in range(0,5):
                        try:
                            data = self.breeze.get_historical_data_v2(interval="5minute",
                                                                from_date= f"{today}T00:00:00.000Z",
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
                    
                    candles_3 = olhc.iloc[-4:-1]
                    resistance = candles_3['high'].max()
                    support = candles_3['low'].min()
                    last_row = olhc.iloc[-1]
                    
                    if last_row['close'] > resistance :
                        atm_strike = round(last_row['close']/50) * 50
                        closest_price_pe = self.closest_put_otm()

                        for j in range(0,5):
                            try:
                                option_data = self.breeze.get_historical_data_v2(interval="5minute",
                                                                    from_date= f"{today}T07:00:00.000Z",
                                                                    to_date= f"{today}T17:00:00.000Z",
                                                                    stock_code="NIFTY",
                                                                    exchange_code="NFO",
                                                                    product_type="options",
                                                                    expiry_date=f"{expiry}T07:00:00.000Z",
                                                                    right="put",
                                                                    strike_price=closest_strike_pe)
                                break
                            except:
                                pass
                        
                        option_data = option_data['Success']
                        option_data = pd.DataFrame(option_data)
                        
                        cand = option_data.iloc[-4:-1]
                        sup = cand['low'].min()
                        last = option_data.iloc[-1]
                        
                        if last['close'] <= sup :
                            initial_point = 0
                            order = 1
                            time = datetime.now().strftime('%H:%M:%S')
                            entry_premium = last['close']
                            SL = entry_premium
                            tsl = entry_premium + SL
                            positions = []

                            position = {
                                'datetime': time,
                                'action': 'sell',
                                'strike': closest_strike_pe,
                                'CE_or_PE': 'put',
                                'premium': entry_premium,
                                'trailing_sl': tsl
                            }
                            
                            positions.append(position)
                            self.initiate_ws('put',closest_strike_pe)
                            print('SELL', closest_strike_pe, 'PUT at', entry_premium)
                            
                            csv_file = "Directional_selling.csv"
                            try:
                                with open(csv_file, 'x', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                            except FileExistsError:
                                pass
                                with open(csv_file, 'a', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow([today, time, closest_strike_pe, 'PE', 'Sell', entry_premium])
                                    
                        else:
                            print(now, 'No decay in option chart')
                    else:
                        print(now, 'Market in range')
                                    
                    if last_row['close'] < support :
                        atm_strike = round(last_row['close']/50) * 50
                        closest_price_ce = self.closest_call_otm()
                        
                        for j in range(0,5):
                            try:
                                option_data = self.breeze.get_historical_data_v2(interval="5minute",
                                                                    from_date= f"{today}T07:00:00.000Z",
                                                                    to_date= f"{today}T17:00:00.000Z",
                                                                    stock_code="NIFTY",
                                                                    exchange_code="NFO",
                                                                    product_type="options",
                                                                    expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                                    right="call",
                                                                    strike_price=closest_strike_ce)
                                break
                            except:
                                pass
                        
                        option_data = option_data['Success']
                        option_data = pd.DataFrame(option_data)
                        
                        cand = option_data.iloc[-4:-1]
                        sup = cand['low'].min()
                        last = option_data.iloc[-1]
                        
                        if last['close'] <= sup :
                            initial_point = 0
                            order = -1
                            time = datetime.now().strftime('%H:%M:%S')
                            entry_premium = last['close']
                            SL = entry_premium
                            tsl = entry_premium + SL
                            positions = []

                            position = {
                                'datetime': time,
                                'action': 'sell', 
                                'strike': closest_strike_ce,
                                'CE_or_PE': 'call',
                                'premium': entry_premium,
                                'trailing_sl': tsl
                            }
                            
                            positions.append(position)
                            self.initiate_ws('call',closest_strike_ce)
                            
                            print('SELL', closest_strike_ce, 'CALL at', entry_premium)
                            
                            csv_file = "Directional_selling.csv"
                            try:
                                with open(csv_file, 'x', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                            except FileExistsError:
                                pass
                                with open(csv_file, 'a', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow([today, time, closest_strike_ce, 'CE', 'Sell', entry_premium])
                                    
                        else:
                            print(now, 'no decay in option chart')
                    else:
                        print(now, 'Market in range')
                                    
                            
                            
                if t(9, 45)<t(datetime.now().time().hour, datetime.now().time().minute)<t(15, 20) and now.second == 0 and positions_df.empty :
                    time.sleep(2)
                    today = datetime.now().strftime("%Y-%m-%d")
                    #yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

                    for j in range(0,5):
                        try:
                            data = self.breeze.get_historical_data_v2(interval="5minute",
                                                                from_date= f"{today}T00:00:00.000Z",
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
                    
                    candles_3 = olhc.iloc[-7:-1]
                    resistance = candles_3['high'].max()
                    support = candles_3['low'].min()
                    last_row = olhc.iloc[-1]
                    
                    if last_row['close'] > resistance :
                        atm_strike = round(last_row['close']/50) * 50
                        closest_strike_pe = self.closest_put_otm()
                        

                        option_data = self.breeze.get_historical_data_v2(interval="5minute",
                                                                    from_date= f"{today}T07:00:00.000Z",
                                                                    to_date= f"{today}T17:00:00.000Z",
                                                                    stock_code="NIFTY",
                                                                    exchange_code="NFO",
                                                                    product_type="options",
                                                                    expiry_date=f"{expiry}T07:00:00.000Z",
                                                                    right="put",
                                                                    strike_price=closest_strike_pe)
                        option_data = option_data['Success']
                        option_data = pd.DataFrame(option_data)
                            
                        
                        cand = option_data.iloc[-7:-1]
                        sup = cand['low'].min()
                        last = option_data.iloc[-1]
                        
                        if last['close'] <= sup :
                            initial_point = 0
                            order = 1
                            time = datetime.now().strftime('%H:%M:%S')
                            entry_premium = last['close']
                            SL = entry_premium
                            tsl = entry_premium + SL
                            print('position excecuted')
                            positions = []

                            position = {
                                'datetime': time,
                                'action': 'sell',
                                'strike': closest_strike_pe,
                                'CE_or_PE': 'put',
                                'premium': entry_premium,
                                'trailing_sl': tsl
                            }
                            
                            positions.append(position)
                            positions_df = pd.DataFrame(positions)

                            self.initiate_ws('put',closest_strike_pe)
                            print('SELL', closest_strike_pe, 'PUT at', entry_premium)
                            
                            csv_file = "Directional_selling.csv"
                            try:
                                with open(csv_file, 'x', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                            except FileExistsError:
                                pass
                                with open(csv_file, 'a', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow([today, time, closest_strike_pe, 'PE', 'Sell', entry_premium])
                                    
                        else:
                            print(now, 'No decay in option chart')
                    else:
                        print(now, 'Market in range')
                                    
                    if last_row['close'] < support :
                        atm_strike = round(last_row['close']/50) * 50
                        closest_strike_ce = self.closest_call_otm()
                        
                        option_data = self.breeze.get_historical_data_v2(interval="5minute",
                                                                            from_date= f"{today}T07:00:00.000Z",
                                                                            to_date= f"{today}T17:00:00.000Z",
                                                                            stock_code="NIFTY",
                                                                            exchange_code="NFO",
                                                                            product_type="options",
                                                                            expiry_date=f"{self.expiry}T07:00:00.000Z",
                                                                            right="call",
                                                                            strike_price=closest_strike_ce)
                        option_data = option_data['Success']
                        option_data = pd.DataFrame(option_data)
                            
                        
                        cand = option_data.iloc[-7:-1]
                        sup = cand['low'].min()
                        last = option_data.iloc[-1]
                        
                        if last['close'] <= sup :
                            initial_point = 0
                            order = -1
                            time = datetime.now().strftime('%H:%M:%S')
                            entry_premium = last['close']
                            SL = entry_premium
                            tsl = entry_premium + SL
                            print('position executed')
                            positions = []
                            
                            position = {
                                'datetime': time,
                                'action': 'sell',
                                'strike': closest_strike_ce,
                                'CE_or_PE': 'call',
                                'premium': entry_premium,
                                'trailing_sl': tsl
                            }
                            
                            positions.append(position)

                            positions_df = pd.DataFrame(positions)

                            self.initiate_ws('call',closest_strike_ce)
                            print('SELL', closest_strike_ce, 'CALL at', entry_premium)
                            
                            csv_file = "Directional_selling.csv"
                            try:
                                with open(csv_file, 'x', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                            except FileExistsError:
                                pass
                                with open(csv_file, 'a', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow([today, time, closest_strike_ce, 'CE', 'Sell', entry_premium])
                                    
                        else:
                            print(now, 'no decay in option chart')
                    else:
                        print(now, 'Market in range')
                        
                        
                                
                if not positions_df.empty:
                    import time,os
                    positions_df = self.update_trailing_sl(positions_df)
                    positions_df = self.check_profit_target_and_add_position(positions_df)
                    if now.time() >= t(15, 20):
                        path="unclosed_positions_directional.csv"
                        # if os.path.exists(path):

                        #     positions_df.to_csv(csv_file,header=False,mode='a',index=False)
                        # else:
                        positions_df.to_csv(path,header=True,index=False)
                        print("All open Positions Saved and Market closed")
                        quit()
                    print(now)        
                    time.sleep(20)

                db_session.refresh(deployed_strategy)
                print('RUNNING')
