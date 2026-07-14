from sqlmodel import Session
from pathlib import Path
from ..dao.db import engine
import pandas as pd
import numpy as np
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor  # Added ProcessPoolExecutor

# NEW IMPORTS FOR SCHEDULER
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor as ApsProcessExecutor, ThreadPoolExecutor as ApsThreadExecutor
from apscheduler.jobstores.base import JobLookupError

from ..models.create_models import DeployStrategyCreate
from ..models.db_models import Strategy
from ..dao import strategy_dao, deployed_strategy_dao, user_strategy_dao
from ..services import session_token_service
from ..strategies.strategy_rps_rsi_supertrend import StrategyRpsRsiSupertrend
from ..strategies.strategy_rps_rsi_supertrend_new import StrategyRpsRsiSupertrendNew
from ..strategies.strategy_icici import StrategyICICI
from ..strategies.strategy_macd import StrategyMACD
from ..strategies.strategy_directional_orb import StrategyRpsDirectionalOrb
from ..strategies.strategy_carara_directional_orb import StrategyCararaDirectionalOrb
from ..strategies.strategy_fivepaisa_straddle import StrategyStraddleFivePaisa
from ..strategies.strategy_DS_directional import StrategyDSDirectionalOrb
from ..strategies.strategy_RPSinghal_directional import StrategyRPSinghalDirectionalOrb
from ..strategies.strategy_Rahulsinghal13_directional import StrategyRahulsinghal13DirectionalOrb
from ..strategies.strategy_Sarlasinghal_directional import StrategySarlasinghalDirectionalOrb
from ..strategies.strategy_fivepaisa_Old_PaperTrade_straddle import StrategyStraddleOldPaperFivePaisa
from ..strategies.strategy_fivepaisa_No_Condition_PaperTrade_straddle import StrategyStraddleNoConditionPaperFivePaisa
from ..strategies.strategy_fivepaisa_Sensex_PaperTrade_straddle import StrategyStraddleSensexPaperFivePaisa
from ..strategies.strategy_fivepaisa_15_Lag_PaperTrade_straddle import StrategyStraddleFifteenLagPaperFivePaisa
from ..strategies.strategy_momentum import StrategyMomentumLive
from ..strategies.strategy_option_buying_Papertrade import StrategyOptionBuying
from ..strategies.strategy_option_buying_Mohit_paper_trade import StrategyOptionBuyingIndex
from ..strategies.strategy_sanjay_sir import StrategySanjaySir
from ..strategies.strategy_DS_directional_PT import StrategyDirectionalPTOrb
from ..strategies.strategy_RV import StrategyRV
from ..strategies.strategy_RV_Live import StrategyRVLive
from ..strategies.strategy_Greek import StrategyGreek
from ..strategies.strategy_arimax import StrategyArimax
from ..strategies.strategy_IIFL_Straddle import StrategyStraddleIIFL
from ..strategies.strategy_test1 import StrategyTest
from ..strategies.strategy_test_iifl import StrategyIIFL_TESTING
from ..strategies.strategy_IIFL_Delta_Straddle import StrategyStraddleIIFLDelta
from ..strategies.strategy_IIFL_Vomma_Straddle import StrategyStraddleIIFLVomma
from ..strategies.strategy_P1 import StrategyP1Cash    
from ..strategies.strategy_P2 import StrategyP2Cash    
from ..strategies.strategy_P3 import StrategyP3Cash    
from ..strategies.strategy_spread import StrategyNayanCreditSpreads   
from ..strategies.strategy_option_buying import StrategyNayanOptionBuying   
from ..strategies.strategy_Vega_Live import StrategyVegaLive  

from ..config import settings

# Max workers config
MAX_CONCURRENT_STRATEGIES = 50
strategy_thread_pool = ThreadPoolExecutor(
    max_workers=MAX_CONCURRENT_STRATEGIES,
    thread_name_prefix='strategy_'
)

# Dedicated Process Pool for executing heavy continuous multi-strategy loops without blocking the GIL
strategy_process_pool = ProcessPoolExecutor(max_workers=4)

print(f"Strategy thread pool initialized with max_workers={MAX_CONCURRENT_STRATEGIES}")

# Top-level standalone wrapper required for serialization safety across process boundaries
def run_isolated_strategy_process(strategy_id: int, db_strategy_id: int, params: dict):
    with Session(engine) as db_session:
        db_strategy = strategy_dao.get_strategy_by_id(db_session, db_strategy_id)
        # Re-resolve class mapping cleanly inside the newborn environment instance
        strategy_instance = get_strategy(db_strategy, params)
        if strategy_instance:
            strategy_instance.run(strategy_id)

# 1. Initialize global scheduler with specialized executor separation
scheduler_executors = {
    'default': ApsThreadExecutor(20),
    'processpool': ApsProcessExecutor(5)
}
scheduler = BackgroundScheduler(executors=scheduler_executors)
scheduler.start()


def get_all_strategies(db_session: Session):
    return strategy_dao.get_all_strategies(db_session)


def get_strategy_by_id(db_session: Session, strategy_id: int):
    return strategy_dao.get_strategy_by_id(db_session, strategy_id)


def get_deployed_strategies_by_user_id(db_session: Session, user_id: int):
    return deployed_strategy_dao.get_deployed_strategies_by_user_id(db_session, user_id)


def get_deployed_strategy_by_strategy_id(db_session: Session, strategy_id: int):
    return deployed_strategy_dao.get_deployed_strategy_by_strategy_id(db_session, strategy_id)


def deploy_strategy(
    db_session: Session,
    strategy_id: int,
    user_id: int,
    strategy_data: DeployStrategyCreate
):
    session_token = session_token_service.get_session_token_by_id(db_session, strategy_data.session_token_id)
    
    db_strategy = strategy_dao.get_strategy_by_id(db_session, strategy_id)
    if not db_strategy:
        return None

    params = {}
    params["api_key"] = session_token.api_key
    params["api_secret"] = session_token.api_secret
    params["session_token"] = session_token.token
    params["csv_file"] = settings.csv_folder_path + db_strategy.file_name
    params["start_hour"] = int(strategy_data.start_hour)
    params["start_minute"] = int(strategy_data.start_minute)
    params["end_hour"] = int(strategy_data.end_hour)
    params["end_minute"] = int(strategy_data.end_minute)
    params["expiry"] = strategy_data.expiry
    params["fut_expiry"] = strategy_data.fut_expiry
    params["qty"] = int(strategy_data.qty)
    params["max_position"] = int(strategy_data.max_position) 
    params["slicer"] = int(strategy_data.slicer) 
    params["portfolio_size"] = int(strategy_data.portfolio_size) 
    params["end_month_size"] = int(strategy_data.end_month_size) 
    params["first_day_of_month"] = strategy_data.first_day_of_month 
    params["last_day_of_month"] = strategy_data.last_day_of_month 
    params["portfolio_price"] = int(strategy_data.portfolio_price) 
    params["interval"] = strategy_data.interval 
    params["symbol"] = strategy_data.symbol 
    params["strike"] = strategy_data.strike 
    params["auth_code"] = strategy_data.auth_code 
    params["action"] = strategy_data.action
    params["exchange"] = strategy_data.exchange
    params["product"] = strategy_data.product
    params["price"] = strategy_data.price
    params["right"] = strategy_data.right
    params["schedule"] = strategy_data.schedule

    strategy = get_strategy(db_strategy, params)
    deployed_strategy = deployed_strategy_dao.create_deployed_strategy(
        db_session,
        strategy_id,
        user_id,
        strategy_data.session_token_id,
        params
    )

    # 3. Handle Scheduling and immediate processing based on execution footprint
    if getattr(db_strategy, "category", "") == "Cash":
        interval_minutes = int(params.get("schedule", 60))
        
        # Add interval job to the isolated process pool executor
        scheduler.add_job(
            run_isolated_strategy_process,
            'interval',
            minutes=interval_minutes,
            args=[deployed_strategy.id, db_strategy.id, params],
            id=f"cash_job_{deployed_strategy.id}",
            replace_existing=True,
            executor='processpool'  # Routed out of the main app worker scope
        )
        print(f"Scheduled Cash Strategy {deployed_strategy.id} onto standalone ProcessPool to fire every {interval_minutes} minutes.")
        
        # Trigger immediate run inside the detached background process pool
        loop = asyncio.get_event_loop()
        loop.run_in_executor(strategy_process_pool, run_isolated_strategy_process, deployed_strategy.id, db_strategy.id, params)

    else:
        # Keep real-time streaming connections on lightweight ThreadPool layout
        async def run_strategy_in_thread():
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(
                    strategy_thread_pool,
                    strategy.run,
                    deployed_strategy.id
                )
            finally:
                with Session(engine) as cleanup_session:
                    ds = deployed_strategy_dao.get_deployed_strategy_by_id(cleanup_session, deployed_strategy.id)
                    if ds and ds.status == "RUNNING":
                        ds.status = "STOPPED"
                        cleanup_session.add(ds)
                        cleanup_session.commit()
        
        task = asyncio.create_task(run_strategy_in_thread())
        
    return deployed_strategy


def stop_strategy(db_session: Session, strategy_id: int):
    deployed_strategy = deployed_strategy_dao.get_deployed_strategy_by_strategy_id(db_session, strategy_id)
    if deployed_strategy != None:
        deployed_strategy.status = "STOPPED"
        db_session.add(deployed_strategy)
        db_session.commit()

        try:
            scheduler.remove_job(f"cash_job_{deployed_strategy.id}")
            print(f"Successfully stopped Cash Strategy Scheduler for job {deployed_strategy.id}")
        except JobLookupError:
            pass

    return deployed_strategy
 

def get_strategy(strategy: Strategy, params: dict):
    if strategy.name == "RPS_Rsi_supertrend":
        return StrategyRpsRsiSupertrend(params)
    elif strategy.name == "RPS_Ent_rsi_supertrend_new":
        return StrategyRpsRsiSupertrendNew(params)
    elif strategy.name == "icici_rsi_st":
        return StrategyICICI(params)
    elif strategy.name == "RPS_Ent_macd": 
        return StrategyMACD(params)
    elif strategy.name == "RPS_Ent_Directional": 
        return StrategyRpsDirectionalOrb(params)
    elif strategy.name == "Carara_Directional": 
        return StrategyCararaDirectionalOrb(params)
    elif strategy.name == "Five_Paisa_Straddle": 
        return StrategyStraddleFivePaisa(params)
    elif strategy.name == "DS_Directional": 
        return StrategyDSDirectionalOrb(params)
    elif strategy.name == "RPSinghal_Directional": 
        return StrategyRPSinghalDirectionalOrb(params)
    elif strategy.name == "Rahulsinghal13_Directional": 
        return StrategyRahulsinghal13DirectionalOrb(params)
    elif strategy.name == "Sarlasinghal_Directional": 
        return StrategySarlasinghalDirectionalOrb(params)
    elif strategy.name == "Five_Paisa_Old_Paper_Straddle": 
        return StrategyStraddleOldPaperFivePaisa(params)
    elif strategy.name == "Five_Paisa_No_Condition_Paper_Straddle": 
        return StrategyStraddleNoConditionPaperFivePaisa(params)
    elif strategy.name == "Five_Paisa_Sensex_Paper_Straddle": 
        return StrategyStraddleSensexPaperFivePaisa(params)
    elif strategy.name == "Five_Paisa_15_Lag_Paper_Straddle": 
        return StrategyStraddleFifteenLagPaperFivePaisa(params)
    elif strategy.name == "Zerodha_Momentum": 
        return StrategyMomentumLive(params)
    elif strategy.name == "Option_Buying_NTSL": 
        return StrategyOptionBuying(params)
    elif strategy.name == "Option_Buying_2RV": 
        return StrategyOptionBuyingIndex(params)
    elif strategy.name == "Sanjay_Sir_PT": 
        return StrategySanjaySir(params)
    elif strategy.name == "Directional_PT": 
        return StrategyDirectionalPTOrb(params)
    elif strategy.name == "RV_Calculator": 
        return StrategyRV(params)
    elif strategy.name == "RV_Live": 
        return StrategyRVLive(params)
    elif strategy.name == "Greek_Calculator": 
        return StrategyGreek(params)
    elif strategy.name == "Arimax": 
        return StrategyArimax(params)
    elif strategy.name == "IIFL_Straddle": 
        return StrategyStraddleIIFL(params)
    elif strategy.name == "Manual_Order": 
        return StrategyTest(params)
    elif strategy.name == "Test_IIFL": 
        return StrategyIIFL_TESTING(params)
    elif strategy.name == "IIFL_Straddle_Vomma": 
        return StrategyStraddleIIFLVomma(params)
    elif strategy.name == "IIFL_Straddle_Delta": 
        return StrategyStraddleIIFLDelta(params)
    elif strategy.name == "Zerodha_P1": 
        return StrategyP1Cash(params)
    elif strategy.name == "Zerodha_P2": 
        return StrategyP2Cash(params)
    elif strategy.name == "Zerodha_P3": 
        return StrategyP3Cash(params)
    elif strategy.name == "Credit_Spread": 
        return StrategyNayanCreditSpreads(params)
    elif strategy.name == "Option_Buying_Nayan": 
        return StrategyNayanOptionBuying(params)
    elif strategy.name == "Vega_Live": 
        return StrategyVegaLive(params)


def get_pnl(file_name: str, date: str = None):
    path = settings.csv_folder_path + file_name
    csv_file = Path(path)
 
    if not csv_file.is_file() or file_name in ["Zerodha_Momentum.csv", "Zerodha_P1.csv", "autologin_report.csv", "Zerodha_P2.csv", "Zerodha_P3.csv", "sanjay_sir.csv", "five_paisa_old_paper_straddle.csv", "RV_Calculator.csv", "Greek_Calculator.csv", "Greek.csv", "Vega_Live.csv", "RV_Live.csv", "Test.csv", "Test_IIFL.csv", "Manual_Order.csv"]:
        return None

    df = pd.read_csv(path)
    if date:
        df = df[df["Date"] == date]
    if len(df.index) > 0:
        return np.around(df['PnL'].sum(), 2)
    else:
        return None


def generate_strategy_report(file_name: str):
    path = settings.csv_folder_path + file_name
    csv_file = Path(path)

    if not csv_file.is_file() or file_name in ["Zerodha_Momentum.csv", "sanjay_sir.csv", "autologin_report.csv", "five_paisa_old_paper_straddle.csv", "RV_Calculator.csv", "Greek_Calculator.csv", "Greek.csv", "Vega_Live.csv", "RV_Live.csv", "Test.csv", "Test_IIFL.csv", "Manual_Order.csv"]:
        return None

    df = pd.read_csv(path)
    if df.empty or "PnL" not in df.columns:
        return None

    total_trades = len(df)
    win_trades = len(df[df["PnL"] > 0])
    loss_trades = len(df[df["PnL"] < 0])

    avg_profit = df["PnL"].mean()
    total_profit = df["PnL"].sum()
    win_rate = (win_trades / total_trades) * 100 if total_trades > 0 else 0

    df["equity"] = df["PnL"].cumsum()
    df["peak"] = df["equity"].cummax()
    df["drawdown"] = df["equity"] - df["peak"]
    max_drawdown = df["drawdown"].min()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        days = (df["Date"].max() - df["Date"].min()).days
        years = days / 365 if days > 0 else 1
        initial_capital = 100000
        final_capital = initial_capital + total_profit
        cagr = ((final_capital / initial_capital) ** (1 / years) - 1) * 100 if years > 0 else 0
    else:
        cagr = 0

    return {
        "Total Trades": total_trades,
        "Win Trades": win_trades,
        "Loss Trades": loss_trades,
        "Win Rate": round(win_rate, 2),
        "Avg Profit": round(avg_profit, 2),
        "Total Profit": round(total_profit, 2),
        "Max Drawdown": round(max_drawdown, 2),
        "CAGR": round(cagr, 2)
    }


def save_strategy_report(file_name: str):
    if "P1" in file_name or "P2" in file_name or "P3" in file_name:
        return file_name.replace(".csv", "_report.csv")

    report = generate_strategy_report(file_name)
    if not report:
        return None

    report_file_name = file_name.replace(".csv", "_report.csv")
    report_path = os.path.join(settings.csv_folder_path, report_file_name)

    df = pd.DataFrame([report])
    df.to_csv(report_path, index=False)
    return report_file_name


def create_strategy(db_session: Session, strategy_data: dict):
    return strategy_dao.create_strategy(db_session, strategy_data)

def delete_strategy(db_session: Session, strategy_id: int):
    return strategy_dao.delete_strategy(db_session, strategy_id)