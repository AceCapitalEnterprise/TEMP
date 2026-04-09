
from sqlmodel import Session
from pathlib import Path
import pandas as pd
import numpy as np
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor  #Change

from ..models.create_models import DeployStrategyCreate
from ..models.db_models import Strategy
from ..dao import strategy_dao, deployed_strategy_dao,user_strategy_dao
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



from ..config import settings

# <-------------------------------- Change---------------------------------->

# NEW: Create custom thread pool for strategies

MAX_CONCURRENT_STRATEGIES = 50  # Adjust this based on your needs
strategy_thread_pool = ThreadPoolExecutor(
    max_workers=MAX_CONCURRENT_STRATEGIES,
    thread_name_prefix='strategy_'
)

print(f"Strategy thread pool initialized with max_workers={MAX_CONCURRENT_STRATEGIES}")

# <-------------------------------- Change---------------------------------->


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
    params["action"] = strategy_data.action # Change
    params["exchange"] = strategy_data.exchange # Change
    params["product"] = strategy_data.product # Change
    params["price"] = strategy_data.price # Change
    params["right"] = strategy_data.right # Change



    print(params["expiry"])

    strategy = get_strategy(db_strategy, params)
    deployed_strategy = deployed_strategy_dao.create_deployed_strategy(
        db_session,
        strategy_id,
        user_id,
        strategy_data.session_token_id,
        params
    )

    # # create a coroutine for a blocking function
    # blocking_coro = asyncio.to_thread(strategy.run, deployed_strategy.id)
    # # execute the blocking function independently
    # task = asyncio.create_task(blocking_coro)
    # return deployed_strategy
    # <--------------------------- Change------------------>
    # MODIFIED: Use custom thread pool instead of default asyncio.to_thread

    async def run_strategy_in_thread():
        loop = asyncio.get_event_loop()
        # Run in custom thread pool
        await loop.run_in_executor(
            strategy_thread_pool,
            strategy.run,
            deployed_strategy.id
        )
    
    # Create task from the coroutine
    task = asyncio.create_task(run_strategy_in_thread())
    # <--------------------------- Change------------------>



def stop_strategy(
    db_session: Session,
    strategy_id: int
):
    deployed_strategy = deployed_strategy_dao.get_deployed_strategy_by_strategy_id(db_session, strategy_id)
    if deployed_strategy != None:
        deployed_strategy.status = "STOPPED"
        db_session.add(deployed_strategy)
        db_session.commit()

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
    elif strategy.name == "Option_Buying_PT": 
        return StrategyOptionBuying(params)
    elif strategy.name == "Option_Buying_Mohit_PT": 
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

def get_pnl(file_name: str, date: str = None):
    path = settings.csv_folder_path + file_name
    csv_file = Path(path)
 
    if not csv_file.is_file() or file_name == "portfolio.csv" or  file_name == "sanjay_sir.csv" or file_name=="five_paisa_old_paper_straddle.csv" or file_name=="RV.csv" or file_name=="Greek.csv" or file_name=="RV_Live.csv" or file_name=="Test.csv":
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

    if not csv_file.is_file() or file_name == "portfolio.csv" or  file_name == "sanjay_sir.csv" or file_name=="five_paisa_old_paper_straddle.csv" or file_name=="RV.csv" or file_name=="Greek.csv" or file_name=="RV_Live.csv":
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

    # Equity curve for drawdown
    df["equity"] = df["PnL"].cumsum()
    df["peak"] = df["equity"].cummax()
    df["drawdown"] = df["equity"] - df["peak"]
    max_drawdown = df["drawdown"].min()

    #  CAGR (approx)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        days = (df["Date"].max() - df["Date"].min()).days
        years = days / 365 if days > 0 else 1
        initial_capital = 100000  # change this
        final_capital = initial_capital + total_profit
        cagr = ((final_capital / initial_capital) ** (1 / years) - 1) * 100 if years > 0 else 0
    else:
        cagr = 0

    report = {
        "Total Trades": total_trades,
        "Win Trades": win_trades,
        "Loss Trades": loss_trades,
        "Win Rate": round(win_rate, 2),
        "Avg Profit": round(avg_profit, 2),
        "Total Profit": round(total_profit, 2),
        "Max Drawdown": round(max_drawdown, 2),
        "CAGR": round(cagr, 2)
    }

    return report

def save_strategy_report(file_name: str):
    report = generate_strategy_report(file_name)

    if not report:
        return None

    report_file_name = file_name.replace(".csv", "_report.csv")
    report_path = os.path.join(settings.csv_folder_path, report_file_name)

    df = pd.DataFrame([report])
    df.to_csv(report_path, index=False)

    return report_file_name


#<---------------------------- change --------------------->

# Add this to the bottom of services/strategy_service.py
def create_strategy(db_session: Session, strategy_data: dict):
    return strategy_dao.create_strategy(db_session, strategy_data)

def delete_strategy(db_session: Session, strategy_id: int):
    return strategy_dao.delete_strategy(db_session, strategy_id)

#<---------------------------- change --------------------->

