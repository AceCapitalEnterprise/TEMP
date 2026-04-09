from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
import time
from datetime import datetime, timedelta
import warnings
from zerodha import Zerodha
import csv
import os
import requests
import threading
import signal
import sys
import logging
from ..common.logging_config import create_logger
from tenacity import retry, stop_after_attempt, wait_exponential

warnings.filterwarnings("ignore")
logger = create_logger(__name__, 'trading_momentum_live_strategy.log')

class StrategyMomentumLive(BaseStrategy):
    def __init__(self, params):
        super().__init__()
        
        try:
            self.kite = Zerodha(user_id=params["api_key"], password=params["api_secret"], twofa=params["session_token"])
            self.kite.login()
            logger.info("Zerodha initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Zerodha: ", exc_info = e)
            raise e
        
        # Constants
        self.ROC = [1, 2, 4, 6]
        self.ROC_PERCENTAGE = [0.4, 0.3, 0.2, 0.1]
        self.PORTFOLIO_SIZE = params["portfolio_size"]
        self.END_MONTH_STOCKS = params["end_month_size"]
        self.STOP_LOSS_PERCENT = 0.0
        self.INITIAL_INVESTMENT = params["portfolio_price"]
        self.first_day_of_month = str(params["first_day_of_month"]) # need to change in params format
        self.last_day_of_month = str(params["last_day_of_month"]) # need to change in params format
        self.csv_file = params["csv_file"]

        self.equity_details = pd.read_csv("nifty_500_symbol.csv")
        self.nifty_500_symbols = self.equity_details['Symbol'].tolist()

        # Fetch all instruments once
        self.instruments = self.kite.instruments("NSE")

    # Function to fetch historical data for one month
    def fetch_month_data(self,symbol, today, interval='day'):
        try:
            instrument_token = next((inst['instrument_token'] for inst in self.instruments if inst['tradingsymbol'] == symbol), None)
            if not instrument_token:
                print(f"No instrument found for {symbol}")
                return None
            # Fix 7: Use dynamic start date (1 year prior for SMA_132)
            self.start_date = (pd.to_datetime(today) - pd.Timedelta(days=365)).strftime('%Y-%m-%d')
            data = self.kite.historical_data(instrument_token, "2022-01-01", today, interval)
            if data and all(key in data[0] for key in ['date', 'close']):  # Fix 3: Validate keys
                df = pd.DataFrame(data)
                df['Symbol'] = symbol
                df['Date'] = pd.to_datetime(df['date']).dt.date
                return df
            print(f"No valid data for {symbol}")
            return None
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None
        
    # Function to calculate SMAs
    def calculate_smas(self,df):
        df['SMA_22'] = df['close'].rolling(window=22).mean()
        df['SMA_66'] = df['close'].rolling(window=66).mean()
        df['SMA_132'] = df['close'].rolling(window=132).mean()
        df['STD_132'] = df['close'].rolling(window=132).std()
        df['Return'] = df['close'].pct_change()
        df['volatility'] = df['Return'].rolling(window=132).std() * np.sqrt(252)
        return df
        
    def rank_stocks_roc(self,selected_date, daily_stocks_data, last_day_of_month):
        print(f"Ranking stocks for ROC on {selected_date}...")
        logger.info(f"[INFO]:Ranking stocks for ROC on {selected_date}...")
        selected_date = pd.to_datetime(selected_date).date()
        last_day_of_month = pd.to_datetime(last_day_of_month).date()
        # Get the earliest and latest dates in the dataset
        min_date = daily_stocks_data['Date'].min()
        max_date = daily_stocks_data['Date'].max()

        # Calculate start dates for ROC periods, ensuring they are within data range
        start_dates = []
        for roc in self.ROC:
            target_date = (pd.to_datetime(selected_date) - pd.offsets.MonthEnd(roc)).date()
            # If target_date is in the future or after max_date, use max_date
            if target_date > max_date:
                start_dates.append(max_date)
            # If target_date is before min_date, use min_date
            elif target_date < min_date:
                start_dates.append(min_date)
            else:
                # Find the closest available date <= target_date
                available_dates = daily_stocks_data[daily_stocks_data['Date'] <= target_date]['Date'].unique()
                if len(available_dates) > 0:
                    closest_date = max(available_dates)
                    start_dates.append(closest_date)
                else:
                    start_dates.append(min_date)
            

        # Replace with last_day_of_month if in the same month and not in the future
        start_dates = [last_day_of_month if start_date.month == last_day_of_month.month and start_date <= max_date else start_date for start_date in start_dates]

        daily_data = daily_stocks_data[daily_stocks_data['Date'] == selected_date]
        top_roc_symbols = []
        for symbol, stock_data in daily_data.groupby('Symbol'):
            roc_values = []
            for start_date in start_dates:
                start_price_data = daily_stocks_data[(daily_stocks_data['Symbol'] == symbol) & (daily_stocks_data['Date'] == start_date)]
                if len(start_price_data) == 1:
                    start_price = start_price_data['close'].values[0]
                    end_price_data = stock_data[stock_data['Date'] == selected_date]
                    if not end_price_data.empty:
                        end_price = end_price_data['close'].values[0]
                        roc = (end_price - start_price) / start_price
                        roc_values.append(roc)
                    else:
                        roc_values.append(0.0)
                        print(f"No end price data for {symbol} on {selected_date}")
                else:
                    roc_values.append(0.0)
                    print(f"No start price data for {symbol} on {start_date}")
                
            # Ensure SMA values exist and are valid
            latest_data = stock_data.iloc[-1]
            sma_22 = latest_data['SMA_22'] if not pd.isna(latest_data['SMA_22']) else 0
            sma_66 = latest_data['SMA_66'] if not pd.isna(latest_data['SMA_66']) else 0
            sma_132 = latest_data['SMA_132'] if not pd.isna(latest_data['SMA_132']) else 0
                
            if sma_22 > sma_66 > sma_132 > 0:
                combined_roc = sum(roc * weight for roc, weight in zip(roc_values, self.ROC_PERCENTAGE))
                top_roc_symbols.append((symbol, combined_roc))
                print(f"{symbol}: ROC values {roc_values}, Combined ROC {combined_roc}")
            else:
                print(f"{symbol} failed SMA filter: SMA_22={sma_22}, SMA_66={sma_66}, SMA_132={sma_132}")
            
        top_roc_symbols.sort(key=lambda x: x[1], reverse=True)
        return [symbol for symbol, _ in top_roc_symbols]
        
    # Function to create or update portfolio CSV
    def create_or_update_portfolio(self,selected_date, daily_stocks_data, last_day_of_month):
        selected_date = pd.to_datetime(selected_date).date()
        ranked_stocks = self.rank_stocks_roc(selected_date, daily_stocks_data, last_day_of_month)
            
        is_new_portfolio = not os.path.exists(self.csv_file)
        if is_new_portfolio:
            portfolio_df = pd.DataFrame(columns=['Symbol', 'Entry Date', 'Weight', 'Exit Date', 'Current Price', 'Number of Shares'])
            current_stocks = []
            num_stocks = 0
        else:
            portfolio_df = pd.read_csv(self.csv_file)
            # Ensure required columns exist
            if 'Current Price' not in portfolio_df.columns:
                portfolio_df['Current Price'] = np.nan
            if 'Number of Shares' not in portfolio_df.columns:
                portfolio_df['Number of Shares'] = 0.0
            current_stocks = portfolio_df['Symbol'].tolist()
            num_stocks = len(current_stocks)
            
        if num_stocks < self.PORTFOLIO_SIZE:
            new_stocks_candidates = [s for s in ranked_stocks if s not in current_stocks]
            new_stocks_to_add = new_stocks_candidates[:self.PORTFOLIO_SIZE - num_stocks]
            stocks_to_hold = current_stocks + new_stocks_to_add
        else:
            stocks_to_hold = current_stocks
            new_stocks_to_add = []

        # Calculate volatilities and current prices for stocks_to_hold
        volatilities = []
        current_prices = []
        for stock in stocks_to_hold:
            stock_data = daily_stocks_data[
                (daily_stocks_data['Symbol'] == stock) & 
                (daily_stocks_data['Date'] == selected_date)
            ]
            if not stock_data.empty:
                vol = stock_data['volatility'].iloc[0] if not pd.isna(stock_data['volatility'].iloc[0]) else np.nan
                price = stock_data['close'].iloc[0]
                volatilities.append(vol)
                current_prices.append(price)
            else:
                volatilities.append(np.nan)
                current_prices.append(0)
            
        # Handle volatilities
        valid_volatilities = [v for v in volatilities if not np.isnan(v) and v > 0]
        avg_volatility = np.mean(valid_volatilities) if valid_volatilities else 0.01
        volatilities = [v if not np.isnan(v) and v > 0 else avg_volatility for v in volatilities]
        inverse_volatilities = [1.0 / v for v in volatilities]
        sum_inverse_vol = sum(inverse_volatilities)
        weights = [iv / sum_inverse_vol for iv in inverse_volatilities] if sum_inverse_vol > 0 else [1.0 / len(stocks_to_hold)] * len(stocks_to_hold)
            
        # Update or add entries
        for stock, weight, price in zip(stocks_to_hold, weights, current_prices):
            shares = round((weight * self.INITIAL_INVESTMENT) / price) if price > 0 else 0
            if stock in current_stocks:
                # Update existing
                idx = portfolio_df[portfolio_df['Symbol'] == stock].index[0]
                portfolio_df.loc[idx, 'Weight'] = weight
                portfolio_df.loc[idx, 'Current Price'] = price
                portfolio_df.loc[idx, 'Number of Shares'] = shares
            else:
                # Add new
                new_entry = {
                    'Symbol': stock,
                    'Entry Date': selected_date.strftime('%Y-%m-%d'),
                    'Weight': weight,
                    'Current Price': price,
                    'Number of Shares': shares,
                    'Exit Date': pd.NA
                }
                portfolio_df = pd.concat([portfolio_df, pd.DataFrame([new_entry])], ignore_index=True)
            
        portfolio_df.to_csv(self.csv_file, index=False)
            
        if is_new_portfolio:
            print(f"Created new portfolio with {len(stocks_to_hold)} stocks: {stocks_to_hold}")
        elif new_stocks_to_add:
            print(f"Added {len(new_stocks_to_add)} new stocks to portfolio: {new_stocks_to_add}")
        else:
            print("No new stocks to add to portfolio.")
        
    # Function to check intra-month exits
    def check_intra_month_exits(self,portfolio_df, first_day, last_day, daily_stocks_data):
        print(f"Checking intra-month exits from {first_day} to {last_day}...")
        logger.info(f"[INFO]: Checking intra-month exits from {first_day} to {last_day}...") 
        exits = []
        for _, row in portfolio_df.iterrows():
            stock = row['Symbol']
            stock_data = daily_stocks_data[
                (daily_stocks_data['Symbol'] == stock) &
                (daily_stocks_data['Date'] >= first_day) &
                (daily_stocks_data['Date'] <= last_day)
            ]
            if not stock_data.empty:
                entry_price = stock_data.iloc[0]['close']
                for _, data_row in stock_data.iterrows():
                     if data_row['SMA_22'] < data_row['SMA_66'] or (
                        self.STOP_LOSS_PERCENT > 0 and 
                        (entry_price - data_row['close']) / entry_price >= self.STOP_LOSS_PERCENT
                    ):
                        portfolio_df.loc[portfolio_df['Symbol'] == stock, 'Exit Date'] = data_row['Date'].strftime('%Y-%m-%d')
                        exits.append(stock)
                        break
        if exits:
            print(f"Intra-month exits: {exits}")
            portfolio_df = portfolio_df[~portfolio_df['Symbol'].isin(exits)]
            portfolio_df.to_csv(self.csv_file, index=False)
        return portfolio_df
        
    def check_month_end_exits(self,portfolio_df, last_day, daily_stocks_data):
        print(f"Checking month-end exits on {last_day}...")
        logger.info(f"[INFO]: Checking month-end exits on {last_day}...") 

        top_stocks = self.rank_stocks_roc(last_day, daily_stocks_data, last_day)[:self.END_MONTH_STOCKS]
        portfolio_df = portfolio_df[portfolio_df['Exit Date'].isna()]
        exits = []
        for _, row in portfolio_df.iterrows():
            if row['Symbol'] not in top_stocks:
                portfolio_df.loc[portfolio_df['Symbol'] == row['Symbol'], 'Exit Date'] = last_day.strftime('%Y-%m-%d')
                exits.append(row['Symbol'])
        if exits:
            print(f"Month-end exits: {exits}")
            portfolio_df = portfolio_df[~portfolio_df['Symbol'].isin(exits)]
            portfolio_df.to_csv(self.csv_file, index=False)
        return portfolio_df
        
    # <------------Main loop------>
    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                # while deployed_strategy.status == "RUNNING":
                try:
                    db_session.refresh(deployed_strategy)
                    today = datetime.now().date()
                    print( f"[Before] first_day_of_month:{self.first_day_of_month} || Type: {type(self.first_day_of_month)}")
                    self.first_day_of_month = datetime.strptime(self.first_day_of_month, "%Y-%m-%d").date()
                    print( f"[After] first_day_of_month:{self.first_day_of_month} || Type: {type(self.first_day_of_month)}")
                    print( f"[Before] last_day_of_month:{self.last_day_of_month} || Type: {type(self.last_day_of_month)}")
                    self.last_day_of_month = datetime.strptime(self.last_day_of_month, "%Y-%m-%d").date()
                    print( f"[After] last_day_of_month:{self.last_day_of_month} || Type: {type(self.last_day_of_month)}")
                    # Fetch data for current month
                    all_data = []
                    for symbol in self.nifty_500_symbols:
                        df = self.fetch_month_data(symbol, today)
                        if df is not None and not df.empty:
                            df = self.calculate_smas(df)
                            all_data.append(df)
                    if all_data:
                        daily_stocks_data = pd.concat(all_data, ignore_index=True)
                        daily_stocks_data.to_csv("Nifty500_current_month.csv", index=False)
                        print("✅ Nifty 500 data saved to Nifty500_current_month.csv")
                        logger.info(f"[INFO]: Nifty 500 data saved to Nifty500_current_month.csv")
                    else:
                        print("No data fetched. Exiting.")
                        logger.info(f"[INFO]: No data fetched. Exiting.")
                        return
                            
                    if today == self.first_day_of_month:
                        print("Today is the first day of the month. Creating/Updating portfolio...")
                        logger.info(f"[INFO]: Today is the first day of the month. Creating/Updating portfolio...")
                        portfolio_df = self.create_or_update_portfolio(self.first_day_of_month, daily_stocks_data, self.last_day_of_month)
                    elif today == self.last_day_of_month:
                        print("Today is the last day of the month. Checking month-end exits...")
                        logger.info(f"[INFO]: Today is the last day of the month. Checking month-end exits...")
                        if os.path.exists(self.csv_file):
                            portfolio_df = pd.read_csv(self.csv_file)
                            portfolio_df = self.check_month_end_exits(portfolio_df, self.last_day_of_month, daily_stocks_data)
                        else:
                            print("No portfolio exists. Creating new portfolio...")
                            logger.info(f"[INFO]: No portfolio exists. Creating new portfolio...")  
                            portfolio_df = self.create_or_update_portfolio(self.first_day_of_month, daily_stocks_data,self.last_day_of_month)
                    else:
                        print("Checking intra-month exits...")
                        logger.info(f"[INFO]: Checking intra-month exits...") 
                        if os.path.exists(self.csv_file):
                            portfolio_df = pd.read_csv(self.csv_file)
                            portfolio_df = self.check_intra_month_exits(portfolio_df, self.first_day_of_month, self.last_day_of_month, daily_stocks_data)
                        else:
                            print("No portfolio exists. Waiting for first day of month.")
                            logger.info(f"[INFO]: No portfolio exists. Waiting for first day of month.") 
                    
                    print("YAY! We have successfully reached the end!")    
                    logger.info(f"[INFO]: YAY! We have successfully reached the end!") 


                except Exception as e:
                        logger.error(f"Fatal error in main loop:", exc_info = e)
                        print(f"Main loop error: {str(e)}")
                        
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e
