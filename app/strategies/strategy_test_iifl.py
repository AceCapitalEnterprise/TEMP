from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

from breeze_connect import BreezeConnect
import hashlib
import numpy as np
import pandas as pd
import pandas_ta as ta
import math
from datetime import datetime, date, timedelta, time as t
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

logger = create_logger(__name__, 'trading_IIFL_TESTING.log')

class StrategyIIFL_TESTING(BaseStrategy):
    def __init__(self, params):
        super().__init__()

        # IIFL Parameters
        self.BASE_URL = "https://api.iiflcapital.com/v1"
        self.URL = {
            "NSEEQ":   "https://api.iiflcapital.com/v1/contractfiles/NSEEQ.json",
            "INDICES": "https://api.iiflcapital.com/v1/contractfiles/INDICES.json",
            "NSEFO":   "https://api.iiflcapital.com/v1/contractfiles/NSEFO.json",
        }

        self.USER_SESSION = params["auth_code"]

        self.headers = {
            
            "Authorization": f"Bearer {self.USER_SESSION}",
        }

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

    def place_order(self, payload):
        try:
            response = requests.post(
                f"{self.BASE_URL}/orders",
                headers=self.headers, json=payload, timeout=10
            )
            response.raise_for_status()
            data = response.json()
            logger.info(f"{data}")
            if data.get("status") != "Ok":
                raise Exception(f"Order failed: {data}")
            return data["result"][0]["brokerOrderId"]
        except Exception as e:
            logger.error(f"[ERROR] place_order failed: {e}")

    def get_instrument_id(self, exchange, symbol, expiry=None, strike=None, opt=None):
        try:
            data   = requests.get(self.URL[exchange]).json()
            symbol = symbol.upper()

            if exchange == "NSEEQ":
                return next((i["instrumentId"] for i in data
                            if i["underlyingInstrumentSymbol"] == symbol), None)
            if exchange == "INDICES":
                return next((i["instrumentId"] for i in data
                            if symbol in (i["underlyingInstrumentSymbol"],
                                        i["tradingSymbol"].replace(" INDEX", ""))), None)
            if exchange == "NSEFO":
                exp = datetime.strptime(expiry, "%Y-%m-%d").strftime("%d-%b-%Y")
                return next((i["instrumentId"] for i in data
                            if i["underlyingInstrumentSymbol"] == symbol
                            and exp in i["expiry"]
                            and (opt is None or (i["optionType"] == opt and
                                                float(i["strikePrice"]) == float(strike)))), None)
        except Exception as e:
            logger.error(f"[ERROR] Failed to Fetch Instrument ID: {e}")
            return None
        
    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(db_session, deployed_strategy_id)
            try:
                logger.info("[START] Waiting for signal...")
                db_session.refresh(deployed_strategy)

                action = self.action.upper()  # "buy" -> "BUY"

                opt_type_map = {"call": "CE", "put": "PE"}
                opt_type = opt_type_map.get(self.right)

                if opt_type is None:
                    raise ValueError(f"Invalid right: {self.right}. Must be 'call' or 'put'.")

                ID = self.get_instrument_id("NSEFO", "NIFTY", self.EXPIRY, self.strike, opt_type)
                
                if ID is None:
                    raise ValueError(f"Instrument ID not found for strike {self.strike}, type {opt_type}")

                logger.info(f"Placing {action} order for {opt_type}...")
                logger.info(f"ID:{ID}")
                logger.info(f"Action:{action}")
                logger.info(f"Price:{self.price}")
                payload = [{
                    "instrumentId": str(ID),
                    "exchange": "NSEFO",
                    "transactionType": str(action),
                    "quantity": str(self.QTY),
                    "orderComplexity": "REGULAR",
                    "product": "INTRADAY",
                    "orderType": "LIMIT",
                    "price": str(self.price),
                    "validity": "IOC",  
                    "orderTag": "TEST_BUY"
                }]
                logger.info(f"{payload}")
                order_id = self.place_order(payload)
                logger.info(f"Order placed. ID: {order_id}")
                logger.info("[STOP] Strategy finished.")

            except Exception as e:
                logger.error(f"Error in strategy run {e}")
                deployed_strategy.status = "STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise