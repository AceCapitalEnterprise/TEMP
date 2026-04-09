from sqlmodel import Session

from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine

import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time, timedelta
import sys

# sys.path.append(r"C:\Users\hdhin\demo")
import zerodha
from zerodha import Zerodha
from kiteconnect import KiteConnect
from breeze_connect import BreezeConnect
import csv
import logging
from ..common.logging_config import create_logger
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm
from statsmodels.tsa.statespace.sarimax import SARIMAX

        
# # #check these   
# LIVE_PORTFOLIO_PATH = "live_portfolio.csv"
# EXIT_TRADES_PATH   = "exit_trades.csv"
# MASTER_SIGNAL_CACHE = "master_signals.csv"
# nse_master = pd.read_csv("nse_scrip_master.csv")

logger = create_logger(__name__, 'Arimax.log')

class StrategyArimax(BaseStrategy):
    def __init__(self, params):
        super().__init__()
        # ---- File Paths ----
        self.INITIAL_PORTFOLIO_TODAY = "initial_portfolio_today.csv"
        self.TICKERS = "ind_nifty50list.csv"
        self.LIVE_PORTFOLIO_PATH = "live_portfolio.csv"
        self.EXIT_TRADES_PATH = "exit_trades.csv"
        self.MASTER_SIGNAL_CACHE = "master_signals.csv"

        # ---- Load Static Data Once ----
        self.nse_master = pd.read_csv("nse_scrip_master.csv")

        
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
        try:
            self.kite = Zerodha(
                user_id='AI2416',
                password='Vinay@14541228',
                twofa='6TWJRKAU6TYJ3O2U7EYAIJEKPS3BDQBD'
            )
            self.kite.login()
            logger.info("Zerodha initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Zerodha: ", exc_info = e)
            raise e
            
            # ---------------------- INPUT ----------------------
        
        self.api_secret=params["api_secret"]
        self.session_token=params["session_token"]
        self.ws_connected = False
        self.breeze.ws_connect()
        self.breeze.on_ticks=self.on_ticks

        #global variables
        self.NO_CANDIDATES_TODAY = False
        self.portfolio_today = None
        self.LAST_PORTFOLIO_PRINT = None
        self.PORTFOLIO_PRINT_INTERVAL = 300 
        self.latest_ticks = {}   
        self.INITIAL_CAPITAL = 1_000_000.0
        self.UNIVERSE_SIZE = 20
        self.MAX_HOLDINGS = 10
        self.MULTIPLE = 1
        self.Bull_Ratio = 1.2
        self.total_pnl = self.get_realized_pnl()
        self.ws_connected = False
        self.all_frames=[]
        self.csv=params["csv_file"]
        


    def get_realized_pnl(self):
        try:
                
            if not os.path.exists(self.EXIT_TRADES_PATH):
                return 0.0
        
            exits = pd.read_csv(self.EXIT_TRADES_PATH)
            if exits.empty:
                return 0.0
        
            pnl = (
                (exits['Sell_Price'] - exits['entry_price']) *
                exits['qty']
            ).sum()
        
            return float(pnl)
            
        except Exception as e:
            logger.error(f"Error in getting realized pnl: ", exc_info = e)


    def can_generate_signals(self):
        now = datetime.now().time()
        return now >= dt_time(15, 32) 

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


    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def generate_master_signals(self):
        try:
            equity_details = pd.read_csv(self.TICKERS)
            tickers = equity_details['Symbol'].tolist()
            print(tickers)
            signal_end = pd.Timestamp.today().normalize().tz_localize(None)
            signal_start = (signal_end - pd.tseries.offsets.BDay(52)).tz_localize(None)
            start_date = signal_start - pd.DateOffset(months=26)
            end_date   = signal_end + pd.Timedelta(days=5)
            os.makedirs("data_raw", exist_ok=True)
            logger.info(f"Downloading NIFTY index...")
        
            
            instruments = self.kite.instruments("NSE")
            nifty_token = next(
                (i['instrument_token'] for i in instruments if i['tradingsymbol'] == "NIFTY 50"),
                None
            )
            nifty_raw = self.kite.historical_data(
                nifty_token,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                "day"
            )    
            nifty = pd.DataFrame(nifty_raw)
            nifty['Date'] = pd.to_datetime(nifty['date']).dt.tz_localize(None)
            nifty = nifty[['Date','close']].rename(columns={'close':'Index_Close'})
            nifty.to_csv("data_raw/NIFTY.csv", index=False)
            
            # ---------------- STOCK FETCH FUNCTION ----------------
            def get_token(symbol):
                return next(
                    (i['instrument_token'] for i in instruments if i['tradingsymbol'] == symbol),
                    None
                )
            
            def fetch(symbol):
                try:
                    token = get_token(symbol)
                    if token is None:
                        # print(f"❌ No token for {symbol}")
                        logger.info(f"❌ No token for {symbol}")
                        return None
            
                    raw = self.kite.historical_data(
                        token,
                        start_date.strftime('%Y-%m-%d'),
                        end_date.strftime('%Y-%m-%d'),
                        "day"
                    )
            
                    if not raw:
                        # print(f"❌ No data for {symbol}")
                        logger.info(f"❌ No data for {symbol}")
                        return None
            
                    df = pd.DataFrame(raw)
                    df['Date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
            
                    df = df.rename(columns={
                        'open':'Open',
                        'high':'High',
                        'low':'Low',
                        'close':'Close',
                        'volume':'Volume'
                    })
            
                    df = df[['Date','Open','High','Low','Close','Volume']]
                    return df
            
                except Exception as e:
                    #print(f"❌ Error {symbol}: {e}")
                    logger.error(f"❌ Error {symbol}: {e}")
                    return None
            
                # ---------------- DOWNLOAD ALL STOCKS ----------------
            self.all_frames = []
            
            for sym in tqdm(tickers):
                df = fetch(sym)
                if df is None:
                    continue
            
                # Merge NIFTY
                df = df.merge(nifty, on="Date", how="left")
                df['Index_Close'] = df['Index_Close'].ffill()
            
                df = df.sort_values("Date").reset_index(drop=True)
            
                df.to_csv(f"data_raw/{sym}.csv", index=False)
                self.all_frames.append(df.assign(Ticker=sym))
            
                # print(f"✔ Saved {sym}.csv")
            
            # ---------------- SAVE COMBINED CSV ----------------
            combined = pd.concat(self.all_frames, ignore_index=True)
            combined.to_csv("data_raw/all_stocks_combined.csv", index=False)
            
            # print("\n\n✅ BLOCK 1 COMPLETED — ALL CSV FILES SAVED")
            
            # -------------- INPUTS ----------------------
            data_folder = "data_raw"
            tickers = pd.read_csv(self.TICKERS)["Symbol"].tolist()
            
            # signal_start = pd.to_datetime("2025-10-01")
            # signal_end   = pd.to_datetime("2026-01-06")
            
            train_window = 504
            rv_window = 10
            # ---------------- FUNCTIONS ----------------
            
            def get_alpha_beta(stock_returns, index_returns):
                df = pd.concat([stock_returns, index_returns], axis=1).dropna()
                df.columns = ['stock','index']
            
                if len(df) < 50:
                    return None, None
            
                cov  = np.cov(df['stock'], df['index'], ddof=1)[0,1]
                var  = np.var(df['index'], ddof=1)
                if var == 0:
                    return None, None
            
                beta  = cov / var
                alpha = df['stock'].mean() - beta*df['index'].mean()
                return float(alpha), float(beta)
            
            def arimax_signal(history):
                df = history.copy()
            
                df['log_return']  = np.log(df['Close']/df['Close'].shift(1))
                df['RV']          = df['log_return'].rolling(rv_window).std()*np.sqrt(252)
                df['Volume_Diff'] = df['Volume'].pct_change()
                df['Index_Return']= np.log(df['Index_Close']/df['Index_Close'].shift(1))
            
                df = df.replace([np.inf,-np.inf],np.nan).dropna()
                if len(df) < 80:
                    return None, None, None, None
            
                alpha, beta = get_alpha_beta(df['log_return'], df['Index_Return'])
                if alpha is None:
                    return None, None, None, None
            
                y = df['Close']
                X = df[['RV','Volume_Diff']].copy()
                X['Alpha'] = alpha
                X['Beta']  = beta
            
                try:
                    model = SARIMAX(
                        y, exog=X, order=(1,1,1),
                        enforce_stationarity=False,
                        enforce_invertibility=False
                    )
                    res = model.fit(disp=False)
            
                    last_exog = X.iloc[-1].values.reshape(1,-1)
                    fc = res.get_forecast(steps=1, exog=last_exog)
            
                    next_price = float(fc.predicted_mean.iloc[0])
                    last_close = float(y.iloc[-1])
            
                    signal = 1 if next_price > last_close else 0
                    rv = float(df['RV'].iloc[-1])
            
                    return signal, rv, alpha, beta
            
                except:
                    return None, None, None, None
            
            # ---------------- MAIN LOOP ----------------
            
            master_rows = []
            
            for sym in tqdm(tickers):
                try:
                    df = pd.read_csv(f"{data_folder}/{sym}.csv")
                except:
                    continue
            
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values("Date")
            
                for i in range(train_window, len(df)):
            
                    current_date = df.iloc[i]['Date']
                    if not (signal_start <= current_date <= signal_end):
                        continue
            
                    history = df.iloc[i-train_window:i].copy().set_index("Date")
            
                    signal, rv, alpha, beta = arimax_signal(history)
                    if signal is None:
                        continue
            
                    today = df.iloc[i]
            
                    master_rows.append({
                        "Signal_Date": current_date.date(),
                        "Ticker": sym,
                        "Open": today["Open"],
                        "High": today["High"],
                        "Low": today["Low"],
                        "Close": today["Close"],
                        "Volume": today["Volume"],
                        "Index_Close": today["Index_Close"],
                        "Signal": signal,
                        "RV": rv,
                        "Alpha": alpha,
                        "Beta": beta
                    })
            
            # ------------ SAVE OUTPUT ----------------
            master_df = pd.DataFrame(master_rows)
            master_df = master_df.sort_values(["Signal_Date","Ticker"])
            # ---------- EMA (compute once) ----------
            master_df['EMA_5'] = master_df.groupby('Ticker')['Close'].transform(
                lambda x: x.ewm(span=5, adjust=False).mean()
            )
            ema_watchlist = {}   # {ticker: expiry_date}
            
            
            
            # # FIX DATE
            #master_df["Date"] = pd.to_datetime(master_df["Date"], format="%d-%m-%Y")
            master_df["Signal_Date"] = pd.to_datetime(master_df["Signal_Date"], yearfirst=True)
            # sort first (very important)
            master_df = master_df.sort_values(["Ticker", "Signal_Date"]).reset_index(drop=True)
            
            bull = (
                master_df
                .groupby('Ticker')['Signal']
                .rolling(44)
                .sum()
                .reset_index(level=0, drop=True)
            )
            
            bear = (
                master_df
                .groupby('Ticker')['Signal']
                .rolling(44)
                .apply(lambda x: (x == 0).sum())
                .reset_index(level=0, drop=True)
            )
            
            master_df['Bull_Ratio_44'] = bull / bear.replace(0, np.nan)
            master_df = master_df.sort_values(["Ticker", "Signal_Date"]).reset_index(drop=True)
            master_df["Return"] = (
                master_df.groupby("Ticker")["Close"]
                         .pct_change()
            )
            master_df["volatility"] = (
                master_df.groupby("Ticker")["Return"]
                         .rolling(44)
                         .std()
                         .reset_index(level=0, drop=True)
                         * np.sqrt(252)     # annualized volatility
            )
            master_df = master_df.dropna().reset_index(drop=True)    
        
            # ================= PREP =================
            master_df['Signal_Date'] = pd.to_datetime(master_df['Signal_Date'])
            master_df = master_df.sort_values(['Signal_Date','Ticker']).reset_index(drop=True)
            today = master_df['Signal_Date'].max()
            today_df = master_df[master_df['Signal_Date'] == today].copy()
            
            today_df.to_csv(self.MASTER_SIGNAL_CACHE, index=False)  
            logger.info(f"signals completed")
            return today_df

        except Exception as e:
            logger.error(f"Error in generating master signals: ", exc_info = e)

    def generate_initial_portfolio_helper(self):
        try:
                
            master_signals = self.generate_master_signals()
            if master_signals is None or master_signals.empty:
                logger.error("No master signals generated")
                return None
            today_df = master_signals.copy()
            candidates = today_df[
                today_df['Bull_Ratio_44'] >= self.Bull_Ratio
            ].copy()
            
            if candidates.empty:
                logger.error(f"❌ No candidates found for today")
                raise ValueError("❌ No candidates found for today")
                
                
            
            # ================= SORT & UNIVERSE =================
            candidates = candidates.sort_values(
                by=['Bull_Ratio_44', 'RV', 'Signal', 'Close'],
                ascending=[False, True, False, True]
            ).head(self.MAX_HOLDINGS)
            
            # ================= VOL CLEAN =================
            candidates['volatility'] = candidates['volatility'].replace([np.inf, -np.inf], np.nan)
            
            avg_vol = (
                candidates['volatility'].dropna().mean()
                if candidates['volatility'].dropna().size > 0
                else 0.02
            )
            
            candidates['volatility'] = candidates['volatility'].apply(
                lambda v: v if (pd.notna(v) and v > 0) else avg_vol
            )
            
            # ================= INVERSE VOL WEIGHTS =================
            candidates['inv_vol'] = 1.0 / candidates['volatility']
            total_inv = candidates['inv_vol'].sum()
            
            candidates['weight'] = (
                candidates['inv_vol'] / total_inv if total_inv != 0 else 1 / len(candidates)
            )
        
        
            #check dynamic cap logic
            dynamic_cap = self.INITIAL_CAPITAL
            if len(candidates)<self.MAX_HOLDINGS:
                dynamic_cap = self.INITIAL_CAPITAL * (len(candidates)/self.MAX_HOLDINGS)
                
            
            # ================= LIMIT HOLDINGS =================
            #portfolio_df = candidates.head(MAX_HOLDINGS).copy()
            portfolio_df = candidates.head(self.MAX_HOLDINGS).copy()
            # ================= CAPITAL ALLOCATION =================
            portfolio_df['capital_alloc'] = (
                portfolio_df['weight'] * dynamic_cap
            ).round(2)
            
            # ================= STOP LOSS % =================
            portfolio_df['SL_%'] = (
                portfolio_df['RV'] * self.MULTIPLE * 10
            ).round(2)
            # portfolio_df['SL_%'] = 0.1   
            # ================= FINAL PORTFOLIO =================
            portfolio_df = portfolio_df[[
                'Signal_Date',
                'Ticker',
                'Close',
                'Bull_Ratio_44',
                'RV',
                'volatility',
                'weight',
                'capital_alloc',
                'SL_%'
            ]].sort_values('weight', ascending=False).reset_index(drop=True)
            
            # ================= SAVE =================
            portfolio_df.to_csv(self.INITIAL_PORTFOLIO_TODAY, index=False)
            
            # print("\n✅ PORTFOLIO CREATED")
            logger.info("✅ PORTFOLIO CREATED")
            # print(portfolio_df)
        
            return portfolio_df

        except Exception as e:
            logger.error(f"Error in generating initial portfolio helper: ", exc_info = e)

    
    def generate_initial_portfolio(self):
        try:
            self.portfolio_today = self.generate_initial_portfolio_helper()
            final_df = self.portfolio_today.copy() 
    
            final_df = final_df.merge(
                self.nse_master[['ExchangeCode','Token']],
                left_on='Ticker',
                right_on='ExchangeCode',
                how='left'
            ).drop(columns=['ExchangeCode'])
            final_df['breeze_token'] = "4.1!" + final_df['Token'].astype(str)
            # ---- ADD LIVE FIELDS ----
            for col in [
                'entry_price','qty','CMP',
                'peak_price','trailing_sl',
                'buy_date','buy_time'
            ]:
                final_df[col] = None
            final_df.to_csv(self.LIVE_PORTFOLIO_PATH, index=False)
            
            logger.info("✅ Initial live portfolio created")    
            return final_df

        except Exception as e:
            logger.error(f"Error in generating initital portfolio: ", exc_info = e)
    
    def get_cash_available(self):
        try:
            deployed = 0.0
        
            if os.path.exists(self.LIVE_PORTFOLIO_PATH):
                df = pd.read_csv(self.LIVE_PORTFOLIO_PATH)
                if not df.empty:
                    deployed = (
                        df['entry_price']
                        .fillna(0) *
                        df['qty'].fillna(0)
                    ).sum()
        
            realized = 0.0
            if os.path.exists(self.EXIT_TRADES_PATH):
                exits = pd.read_csv(self.EXIT_TRADES_PATH)
                realized = self.get_realized_pnl()
        
            cash = self.INITIAL_CAPITAL + realized - deployed
            return round(float(cash), 2)
        except Exception as e:
            logger.error(f"Error in getting available cash: ", exc_info = e)  

            
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    
    def get_recently_sold_tickers(self,lookback_days=10):
        try:
            if not os.path.exists(self.EXIT_TRADES_PATH):
                return set()
        
            exits = pd.read_csv(self.EXIT_TRADES_PATH)
            exits['Date'] = pd.to_datetime(exits['Date']).dt.normalize()
        
            today = pd.Timestamp.today().normalize()
        
            # ================= FIND LAST NIFTY TRADING DAY (OTHER THAN TODAY) =================
            last_trading_day = None
        
            for i in range(1, lookback_days + 1):
                d = today - pd.Timedelta(days=i)
        
                try:
                    resp = self.breeze.get_historical_data(
                        interval="1day",
                        from_date=(d - pd.Timedelta(days=10)).strftime("%Y-%m-%dT00:00:00"),
                        to_date=d.strftime("%Y-%m-%dT23:59:59"),
                        stock_code="NIFTY",
                        exchange_code="NSE",
                        product_type="cash"
                    )
                except Exception:
                    continue
        
                # ✅ Breeze response handling
                data = resp.get("Success", []) if isinstance(resp, dict) else []
        
                if not data:
                    continue
        
                df = pd.DataFrame(data)
                if "datetime" not in df.columns:
                    continue
        
                traded_dates = pd.to_datetime(df["datetime"]).dt.normalize()
        
                if d in set(traded_dates):
                    last_trading_day = d
                    break
        
            if last_trading_day is None:
                # print("❌ Could not determine last NIFTY trading day")
                logger.info("❌ Could not determine last NIFTY trading day")
                return set()
        
            # print("✅ Last NIFTY trading day (actual):", last_trading_day.date())
            logger.info(f"✅ Last NIFTY trading day (actual):{last_trading_day.date()}")
        
            # ================= BLOCK LOGIC =================
            block_days = [last_trading_day, today]
        
            blocked = exits[
                exits['Date'].isin(block_days)
            ]['Ticker'].unique()
        
            return set(blocked)
        except Exception as e:
            logger.error(f"Error in getting recently sold tickers", exc_info = e)

    def step7_fill_vacancies(self):
        try:
            live_portfolio_df = pd.read_csv(self.LIVE_PORTFOLIO_PATH)
            
            open_positions = len(live_portfolio_df)
            vacancies = self.MAX_HOLDINGS - open_positions
        
            if vacancies <= 0:
                return None
        
            cash_available = self.get_cash_available()
            logger.info(f"💰 Cash Available: {cash_available}")
        
            if cash_available <= 0:
                print("❌ No cash available")
                logger.info("❌ No cash available")
                return None
        
            today = pd.Timestamp.today().normalize()
            today_df = pd.read_csv(self.MASTER_SIGNAL_CACHE)
            today_df['Signal_Date'] = pd.to_datetime(today_df['Signal_Date'])
                
            # ================= FILTER =================
            candidates = today_df[
                today_df['Bull_Ratio_44'] >= self.Bull_Ratio
            ].copy()
            
            if candidates.empty:
                print("❌ No candidates found today")
                logger.info("❌ No candidates found today")
                self.NO_CANDIDATES_TODAY = True
                return None
            live_portfolio_df = pd.read_csv(self.LIVE_PORTFOLIO_PATH)
            held = set(live_portfolio_df['Ticker'].tolist())
            recently_sold = self.get_recently_sold_tickers()        
            blocked = held.union(recently_sold)
            candidates = candidates[
                ~candidates['Ticker'].isin(blocked)
            ]    
            if candidates.empty:
                print("❌ All candidates already held or recently sold")
                logger.info("❌ All candidates already held or recently sold")
                self.NO_CANDIDATES_TODAY = True
                return None
        
            # ================= SORT =================
            candidates = candidates.sort_values(
                by=['Bull_Ratio_44','RV','Signal','Close'],
                ascending=[False, True, False, True]
            ).head(vacancies)    
            # ================= VOL CLEAN =================
            avg_vol = candidates['volatility'].dropna().mean()
            candidates['volatility'] = candidates['volatility'].apply(
                lambda v: v if pd.notna(v) and v > 0 else avg_vol
            )
            live_portfolio_df = pd.read_csv(self.LIVE_PORTFOLIO_PATH)
            open_positions = len(live_portfolio_df)
            vacancies = self.MAX_HOLDINGS - open_positions
        
            if vacancies <= 0:
                return None
            # ================= CAPITAL ALLOCATION =================
            deployable_cash = cash_available * (len(candidates) / vacancies)
        
            candidates['inv_vol'] = 1 / candidates['volatility']
            candidates['weight'] = candidates['inv_vol'] / candidates['inv_vol'].sum()
            candidates['capital_alloc'] = candidates['weight'] * deployable_cash
        
            # ================= SL =================
            candidates['SL_%'] = (candidates['RV'] * self.MULTIPLE * 10).round(2)
        
            # ================= FINAL FORMAT =================
            new_positions_df = candidates[[
                'Signal_Date','Ticker','Close','Bull_Ratio_44','RV',
                'volatility','weight','capital_alloc','SL_%'
            ]].copy()
            new_positions_df['Signal_Date'] = pd.to_datetime(new_positions_df['Signal_Date']).dt.date
        
            # ================= TOKEN =================
            new_positions_df = new_positions_df.merge(
                self.nse_master[['ExchangeCode','Token']],
                left_on='Ticker',
                right_on='ExchangeCode',
                how='left'
            ).drop(columns=['ExchangeCode'])
        
            new_positions_df['breeze_token'] = "4.1!" + new_positions_df['Token'].astype(str)
        
            # ================= LIVE FIELDS =================
            for col in [
                'entry_price','qty','CMP',
                'peak_price','trailing_sl',
                'buy_date','buy_time'
            ]:
                new_positions_df[col] = None
        
            # ================= APPEND =================
            if not new_positions_df.empty:
                self.portfolio_today = pd.read_csv(self.LIVE_PORTFOLIO_PATH)
                    # 🔥 PREVENT DUPLICATES
                new_positions_df = new_positions_df[
                    ~new_positions_df['Ticker'].isin(self.portfolio_today['Ticker'])
                ]
                self.portfolio_today = pd.concat(
                    [self.portfolio_today, new_positions_df],
                    ignore_index=True
                )
                
                self.portfolio_today.to_csv(self.LIVE_PORTFOLIO_PATH, index=False)
                for t in new_positions_df['breeze_token']:
                    self.breeze.subscribe_feeds(stock_token=t)
                    time.sleep(0.2)
            
                print(f"✅ Added {len(new_positions_df)} new positions")
                logger.info(f"✅ Added {len(new_positions_df)} new positions")
                return new_positions_df
        except Exception as e:
            logger.error(f"Error in filling vacancies: ", exc_info = e)    
        # return new_positions_df
        
    def is_market_open(self):
        now = datetime.now().time()
        return dt_time(9, 15) <= now <= dt_time(15, 30)
        
    def on_ticks(self,ticks):
        try:        
            token = ticks['symbol']
            ltp   = ticks['last']
        
            self.latest_ticks[token] = {
                "ltp": ltp,
                "time": datetime.now()
            }
        except Exception as e:
            logger.error(f"Error in on_ticks: ", exc_info = e)

    # self.breeze.on_ticks = on_ticks
    
    # # ---- connect websocket ONCE ----
    # breeze.ws_connect()    

    def startup(self):
        # ---- attach tick handler ONCE ----
        self.breeze.on_ticks = self.on_ticks
        # self.connect_websocket()
        # # ---- connect websocket ONCE ----
        # breeze.ws_connect()
        if os.path.exists(self.LIVE_PORTFOLIO_PATH):
            print("📂 Live portfolio found")
            logger.info("📂 Live portfolio found")
            self.portfolio_today = pd.read_csv(self.LIVE_PORTFOLIO_PATH)
        else:
            print("🆕 No portfolio found, generating initial portfolio")
            logger.info("🆕 No portfolio found, generating initial portfolio")
            self.portfolio_today = self.generate_initial_portfolio()
    
        # subscribe all tokens
        for t in self.portfolio_today['breeze_token']:
            self.breeze.subscribe_feeds(stock_token=t)
            time.sleep(0.2)
    
    def update_cmp(self):
        for i, row in self.portfolio_today.iterrows():
            token = row['breeze_token']
            if token in self.latest_ticks:
                self.portfolio_today.at[i, 'CMP'] = self.latest_ticks[token]['ltp']
    
    def handle_entries(self):

        for i, row in self.portfolio_today.iterrows():
            if pd.notna(row['entry_price']):
                continue
    
            if pd.isna(row['CMP']):
                continue
    
            entry = row['CMP']
            qty   = int(row['capital_alloc'] // entry)
            if qty <= 0:
                continue
    
            self.portfolio_today.at[i, 'entry_price'] = entry
            self.portfolio_today.at[i, 'qty'] = qty
            self.portfolio_today.at[i, 'peak_price'] = entry
            self.portfolio_today.at[i, 'trailing_sl'] = (
                entry - entry * row['SL_%'] / 100
            )
            self.portfolio_today.at[i, 'buy_date'] = datetime.now().date()
            self.portfolio_today.at[i, 'buy_time'] = datetime.now().time()
    
            print(f"🟢 ENTRY {row['Ticker']} @ {entry}")
            logger.info(f"🟢 ENTRY {row['Ticker']} @ {entry}")
            self.portfolio_today.to_csv(self.LIVE_PORTFOLIO_PATH, index=False)
    
    def update_trailing_sl(self):
        for i, row in self.portfolio_today.iterrows():
            if pd.isna(row['entry_price']) or pd.isna(row['CMP']):
                continue
    
            if row['CMP'] > row['peak_price']:
                self.portfolio_today.at[i, 'peak_price'] = row['CMP']
                self.portfolio_today.at[i, 'trailing_sl'] = (
                    row['CMP'] - row['entry_price'] * row['SL_%'] / 100
                )
                self.portfolio_today.to_csv(self.LIVE_PORTFOLIO_PATH, index=False)
                
            # ================= UNREALIZED =================
        portfolio_mtm = 0.0
    
        for _, row in self.portfolio_today.iterrows():
            if pd.isna(row['entry_price']) or pd.isna(row['CMP']):
                continue
    
            portfolio_mtm += (
                (row['CMP'] - row['entry_price']) * row['qty']
            )
        realized_pnl = self.get_realized_pnl()
        self.total_pnl = realized_pnl + portfolio_mtm
        

    def handle_exits(self):

        bad_rows = self.portfolio_today[
            self.portfolio_today.duplicated('Ticker', keep=False) &
            (
                self.portfolio_today['entry_price'].isna() |
                self.portfolio_today['CMP'].isna() |
                self.portfolio_today['qty'].isna()
            )
        ].index
    
        if not bad_rows.empty:
            print("🧹 Removing broken duplicate rows:")
            print(self.portfolio_today.loc[bad_rows, ['Ticker','entry_price','CMP','qty']])
            logger.info("🧹 Removing broken duplicate rows:")
            logger.info(self.portfolio_today.loc[bad_rows, ['Ticker','entry_price','CMP','qty']])
            self.portfolio_today.drop(bad_rows, inplace=True)
            self.portfolio_today.reset_index(drop=True, inplace=True)
            self.portfolio_today.to_csv(self.LIVE_PORTFOLIO_PATH, index=False)
    
    
        # 🩹 FIX: unique ticker not subscribed (SUNPHARMA case)
        ticker_counts = self.portfolio_today['Ticker'].value_counts()
    
        for i, row in self.portfolio_today.iterrows():
    
            if (
                ticker_counts.get(row['Ticker'], 0) == 1 and
                pd.isna(row['entry_price']) and
                pd.isna(row['CMP'])
            ):
                try:
                    self.breeze.subscribe_feeds(stock_token=row['breeze_token'])
                    time.sleep(0.2)
                    print(f"📡 Subscribed missing unique ticker {row['Ticker']}")
                    logger.info(f"📡 Subscribed missing unique ticker {row['Ticker']}")
                except Exception as e:
                    print(f"❌ Subscribe failed {row['Ticker']}: {e}")
                    logger.info(f"❌ Subscribe failed {row['Ticker']}: {e}")
    
        
        exit_indices = []
    
        for i, row in self.portfolio_today.iterrows():
            if pd.isna(row['entry_price']) or pd.isna(row['CMP']):
                continue
    
            if row['CMP'] <= row['trailing_sl']:
                exit_row = row.to_dict()
                exit_row.update({
                    "Sell_Price": row['CMP'],
                    "Date": datetime.now().date(),
                    "Sell_Time": datetime.now().time(),
                    # "PnL": (row['Sell_Price'] - row['entry_price'])/row['qty']
                    "PnL": (row['CMP'] - row['entry_price'])*row['qty']
                })
                self.total_pnl = self.get_realized_pnl()
    
                pd.DataFrame([exit_row]).to_csv(
                    self.EXIT_TRADES_PATH,
                    mode='a',
                    header=not os.path.exists(self.EXIT_TRADES_PATH),
                    index=False
                )

                # Need to add csv logic here
                # <<-------- check ------->>
                pd.DataFrame([exit_row]).to_csv(
                    self.csv,
                    mode='a',
                    header=not os.path.exists(self.csv),
                    index=False
                )
                # <<-------- check ------->>
                try:
                    self.breeze.unsubscribe_feeds(stock_token=row['breeze_token'])
                except:
                    pass
    
                print(f"🔴 EXIT {row['Ticker']} @ {row['CMP']}")
                logger.info(f"🔴 EXIT {row['Ticker']} @ {row['CMP']}")
                exit_indices.append(i)
    
        if exit_indices:
            self.portfolio_today.drop(exit_indices, inplace=True)
            self.portfolio_today.reset_index(drop=True, inplace=True)
            self.portfolio_today.to_csv(self.LIVE_PORTFOLIO_PATH, index=False)

    def maybe_fill_vacancies(self):
        if self.NO_CANDIDATES_TODAY and self.is_market_open():
            return
        
        open_positions = len(self.portfolio_today)
        if(open_positions<=0):
            return
        if open_positions >= self.MAX_HOLDINGS:
            return
    
        cash = self.get_cash_available()
        if cash <= 0:
            return
    
        new_positions = self.step7_fill_vacancies()
        
    def trading_date(self):
        return datetime.now().date()


    def run(self, deployed_strategy_id: int) -> None:
        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(
                db_session,
                deployed_strategy_id
            )
            try:
                self.connect_websocket()   
                #check on ticks
                signals_done = False
                startup_done_today = False
                current_trading_date = self.trading_date()
                while deployed_strategy.status == "RUNNING":
                    try:
                        db_session.refresh(deployed_strategy)
                        if self.trading_date() != current_trading_date:
                            print("🆕 New trading day detected → resetting states")
                            logger.info("🆕 New trading day detected → resetting states")
                            current_trading_date = self.trading_date()
                            signals_done = False
                            startup_done_today = False
                            
                        if self.can_generate_signals() and not signals_done:
                            print("🧠 Market closed → generating signals ANY HOW")
                            logger.info("🧠 Market closed → generating signals ANY HOW")
                            try:
                                self.generate_master_signals()
                                signals_done = True
                                self.NO_CANDIDATES_TODAY = False 
                                print('SIGNALS GENERATED FOR TOMORROW')
                                logger.info('SIGNALS GENERATED FOR TOMORROW')
                            except Exception as e:
                                logger.error(f"Error in generating signals ", exc_info = e)
                                
                        if self.is_market_open() and not startup_done_today:
                            print("🚀 Market open → running startup")
                            logger.info("🚀 Market open → running startup")
                            try:
                                self.startup()
                                startup_done_today = True
                            except Exception as e:
                                logger.error(f"Error in startup ", exc_info = e)
                                
                                    
                        if not self.is_market_open():
                            time.sleep(60)
                            # print('market is closed')
                            continue
            
                        self.update_cmp()
                        self.handle_entries()
                        self.update_trailing_sl()
                        self.handle_exits()
                        self.maybe_fill_vacancies()
                        now = datetime.now()   
                        
                        if (
                            self.LAST_PORTFOLIO_PRINT is None or
                            (now - self.LAST_PORTFOLIO_PRINT).seconds >= self.PORTFOLIO_PRINT_INTERVAL
                        ):
                            print(f"\n📊 PORTFOLIO SNAPSHOT @ {now.strftime('%H:%M:%S')}")
                            print(
                                self.portfolio_today[
                                    [
                                        'Ticker',
                                        'CMP',
                                        'entry_price',
                                        'qty',
                                        'peak_price',
                                        'trailing_sl'
                                    ]
                                ].to_string(index=False)
                            )
                            logger.info(f"\n📊 PORTFOLIO SNAPSHOT @ {now.strftime('%H:%M:%S')}")
                            logger.info(
                                self.portfolio_today[
                                    [
                                        'Ticker',
                                        'CMP',
                                        'entry_price',
                                        'qty',
                                        'peak_price',
                                        'trailing_sl'
                                    ]
                                ].to_string(index=False)
                            )
                
                            self.portfolio_today.to_csv(self.LIVE_PORTFOLIO_PATH, index=False)
                            self.LAST_PORTFOLIO_PRINT = now
                
                        time.sleep(2)
                    
                    except Exception as e:
                        logger.error(f"Fatal error in main loop:", exc_info = e)
                        self.signal_handler(None, None)
                        
            except Exception as e:
                logging.error(f"Error: ", exc_info = e)
                deployed_strategy.status="STOPPED"
                db_session.add(deployed_strategy)
                db_session.commit()
                raise e