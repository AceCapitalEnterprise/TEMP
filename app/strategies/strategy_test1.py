from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
import math
from datetime import datetime, date, timedelta, time as dt
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

logger = create_logger(__name__, 'trading_test.log')

class StrategyTest(BaseStrategy):
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
        self.EXPIRY = params["expiry"]
        self.FUT_EXPIRY=params["fut_expiry"]
        self.QTY = params["qty"]
        self.expiry = datetime.strptime(self.EXPIRY, '%Y-%m-%d')
        self.expiry1 = self.expiry.strftime('%Y-%m-%d') 
        self.expiry2 = self.expiry.strftime('%d-%b-%Y')  
        self.exchange=params["exchange"]
        self.symbol=params["symbol"]
        self.action=params["action"]
        self.product=params["product"]
        self.price=params["price"]
        self.strike=int(params["strike"])
        self.right=params["right"]
        self.SLICE_QTY=params["slicer"]

    
    # <--------------------- Fetching Symbol ------------------->

    def fetch_stock_code(self,symbol):
        symbol=str(symbol)
        data=self.breeze.get_names(exchange_code = 'NSE',stock_code = symbol)
        code= data["isec_stock_code"]
        print(f"Code:{code}")
        return code


    # <--------------------- Buying and Squaring off  Order ------------------->

    # Buying

    def place_order(self,total_qty,slice_qty,symbol):
        try:
            if slice_qty <= 0:
                logger.info(f"Slicer is 0 or invalid. Treating total quantity {total_qty} as a single slice.")
                num_slices = 1
                slice_qty = total_qty
                remainder = 0
            else:
                num_slices = total_qty // slice_qty
                remainder = total_qty % slice_qty
                
            execution_price=None
            if self.product=="options":
                print(f"Placing options order for {total_qty} units in {num_slices} slices of {slice_qty} with remainder {remainder}.")
                for i in range(num_slices):
                    try:
                        print(f"Placing slice {i+1} for {slice_qty} units...")
                        order_detail = self.breeze.place_order(
                            stock_code=str(symbol),
                            exchange_code=str(self.exchange),
                            product="options",
                            action=str(self.action),
                            order_type="limit",
                            quantity=str(slice_qty),
                            price=str(self.price),
                            validity="day",
                            disclosed_quantity="0",
                            expiry_date=f'{self.expiry1}T06:00:00.000Z',
                            right=str(self.right),
                            strike_price=str(self.strike)
                        )
                        print(f"Slice {i+1} response: {order_detail}")
                    except Exception as e:
                        logger.error(f"Error placing slice {i+1} order for {str(self.right)} {str(self.strike)}: {e}")
                        print(f"Error in slice {i+1}: {e}")
                    time.sleep(2)
                
                if remainder > 0:
                    try:
                        print(f"Placing remainder slice for {remainder} units...")
                        order_detail = self.breeze.place_order(
                            stock_code=str(symbol),
                            exchange_code=str(self.exchange),
                            product="options",
                            action=str(self.action),
                            order_type="limit",
                            quantity=str(remainder),
                            price=str(self.price),
                            validity="day",
                            disclosed_quantity="0",
                            expiry_date=f'{self.expiry1}T06:00:00.000Z',
                            right=str(self.right),
                            strike_price=str(self.strike)
                        )
                        time.sleep(2)
                        print(f"Remainder slice response: {order_detail}")
                    except Exception as e:
                        logger.error(f"Error placing remainder slice order for {str(self.right)} {str(self.strike)}: {e}")
                        print(f"Error in remainder slice: {e}")
                if remainder == 0:
                    return 

                # time.sleep(5)
                # logger.info(f" Prder Detail : {order_detail}")
                # order_id = order_detail['Success']['order_id']
                # trade_detail = self.breeze.get_trade_detail(exchange_code=str(self.exchange), order_id=order_id)
                # execution_price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
                # print(f"Order placed: {self.action} {self.quantity} {self.right} at strike {self.strike} for {execution_price}")
                logger.info(f" All Order placed: {self.action} {self.QTY} {self.right} at strike {self.strike} ")

            elif self.product=="cash":
                order_detail = self.breeze.place_order(
                    stock_code=str(symbol),
                    exchange_code=str(self.exchange),
                    product="cash",
                    action=str(self.action),
                    order_type="limit",
                    stoploss="",
                    quantity=str(self.QTY),
                    price=str(self.price),
                    validity="day"
                )
                time.sleep(5)
                logger.info(f" Prder Detail : {order_detail}")
                # order_id = order_detail['Success']['order_id']
                # trade_detail = self.breeze.get_trade_detail(exchange_code=str(self.exchange), order_id=order_id)
                # execution_price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
                # print(f"Order placed: {self.action} {self.quantity} {self.symbol}  for {execution_price}")
                logger.info(f"Order placed: {self.action} {self.QTY} {self.symbol} ")

            # return execution_price

        except Exception as e:
            logger.error(f"Error placing order {self.action}: {e}")
            print(f"[ERROR] | Error placing {self.action} order: {str(e)}")
            

    

    # <---------------------------------- Main loop ------------------------------>

    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                print("[START] Waiting for signal...")
                logger.info("[START] Waiting for signal...")
                
                # while deployed_strategy.status == "RUNNING":
                db_session.refresh(deployed_strategy)
                symbol=self.fetch_stock_code(self.symbol)
                logger.info(f"Action:{self.action} type{type(self.action)}")
                logger.info(f"Price: {self.price} type{type(self.price)}")
                logger.info(f"Exchange: {self.exchange} type{type(self.exchange)}")
                logger.info(f"Product: {self.product} type{type(self.product)}")
                logger.info(f"Strike: {self.strike} type{type(self.strike)}")
                logger.info(f"Qty: {self.QTY} type{type(self.QTY)}")
                logger.info(f"Slice Qty: {self.SLICE_QTY} type{type(self.SLICE_QTY)}")
                logger.info(f"Symbol: {symbol} type{type(symbol)}")
                logger.info(f"Expiry Used{self.expiry1} type{type(self.expiry1)}")

                self.place_order(self.QTY,self.SLICE_QTY,symbol)

                logger.info("[Stop] Strategy Stopped...")
                
                    
            
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e