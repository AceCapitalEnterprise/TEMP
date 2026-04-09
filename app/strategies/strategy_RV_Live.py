from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import csv, re, time, math
import numpy as np
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import json

import logging
from ..common.logging_config import create_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = create_logger(__name__, 'RV_Live.log')

class StrategyRVLive(BaseStrategy):
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
        self.csv_file = params["csv_file"]
        self.start_date = datetime.strptime(
            params["first_day_of_month"], "%Y-%m-%d"
        ).date()

        self.end_date = datetime.strptime(
            params["last_day_of_month"], "%Y-%m-%d"
        ).date()
    
    # fetching symbol
    def fetch_stock_code(self,symbol):
        symbol=str(symbol)
        data=self.breeze.get_names(exchange_code = 'NSE',stock_code = symbol)
        code= data["isec_stock_code"]
        print(f"Code:{code}")
        return code


    # --- Helper Function ---
    def get_historical_data(self,symbol, from_date, to_date, interval):
        return self.breeze.get_historical_data_v2(
            interval=str(interval),
            from_date=from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            to_date=to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            stock_code=symbol,
            exchange_code="NSE",
            product_type="cash"
        )

    
    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        now = datetime.now().replace(second=0, microsecond=0)-timedelta(minutes=1)
                        value = int(''.join(filter(str.isdigit, self.interval)))

                        if now.time() >= self.TIME_2:
                            print("End of the day ")
                            break

                        if self.TIME_1 <= now.time() < self.TIME_2 and (now.time().minute % value==0) and now.time().second == 0:
                            current_date = self.start_date
                            first_write = True  # to write header only once
                            now = datetime.now().replace(second=0, microsecond=0)-timedelta(minutes=1)
                            entry_time = datetime.combine(current_date, self.TIME_1)
                            et = datetime.combine(self.end_date,self.TIME_2)
                            exit_time=min(now,et)
                            print(f"et:{entry_time} || ex: {exit_time}")
                            symbol=self.fetch_stock_code(self.symbol)
                            nifty_data = self.get_historical_data(symbol, (entry_time-timedelta(days=4)).replace(hour=9, minute=15), exit_time,self.interval)
                            df_nifty = pd.DataFrame(nifty_data["Success"])
                            df_nifty['datetime'] = pd.to_datetime(df_nifty['datetime'])
                            df_nifty.set_index('datetime', inplace=True)

                            # Calculate realized volatility
                            df_nifty["log_ret"] = np.log(df_nifty['close'] / df_nifty['close'].shift(1))
                            df_nifty["RV"] = (round(df_nifty["log_ret"].rolling(window=10).std() * np.sqrt(252*390),5))*100
                            print(self.interval)
                            
                            if self.interval != "1day":
                                # KEEP ONLY REQUIRED TIME RANGE
                                df_nifty = df_nifty.loc[
                                    (df_nifty.index >= entry_time) &
                                    (df_nifty.index <= exit_time)
                                ]

                                # Remove candles beyond exit time (important for 5-min)
                                df_nifty = df_nifty.between_time("09:15", "15:29")

                            # Append to CSV
                            df_nifty.to_csv(
                                self.csv_file,
                                mode='w',
                                header=first_write,
                                index=True
                            )
                            first_write = False  # after first write, disable headers


                            print(f"\n Data for {self.start_date} to {self.end_date} saved in '{self.csv_file}'")
                            logging.info(f"\n Data for {self.start_date} to {self.end_date} saved in '{self.csv_file}'")
                            


                    except Exception as e:
                        logger.error(f"Fatal error in main loop:", exc_info = e)
                        print(f"Main loop error: {str(e)}")
                    
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e