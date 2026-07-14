from sqlmodel import Session, select

from ..models.db_models import Strategy, DeployedStrategy, UserStrategy


def get_deployed_strategies_by_user_id(db_session: Session, user_id: int):
    statement = select(Strategy, DeployedStrategy, UserStrategy).where(
        Strategy.id == DeployedStrategy.strategy_id,
        UserStrategy.strategy_id == Strategy.id,
        UserStrategy.user_id == user_id,
        DeployedStrategy.status == 'RUNNING'
    )
    results = db_session.exec(statement)
    return results.all()


def create_deployed_strategy(db_session: Session, strategy_id: int, 
                             user_id: int, session_token_id: int,
                             params: dict):
    deployed_strategy = DeployedStrategy(
        status="RUNNING",
        start_hour=params["start_hour"],
        start_minute=params["start_minute"],
        end_hour=params["end_hour"],
        end_minute=params["end_minute"],
        expiry=params["expiry"],
        fut_expiry=params["fut_expiry"],
        qty=params["qty"],
        strategy_id=strategy_id,
        user_id=user_id,
        session_token_id=session_token_id
    )
    db_session.add(deployed_strategy)
    db_session.commit()
    db_session.refresh(deployed_strategy)
    return deployed_strategy


def get_deployed_strategy_by_id(
    db_session: Session,
    deployed_strategy_id: int
):
    statement = select(DeployedStrategy).where(
        DeployedStrategy.id == deployed_strategy_id
    )
    results = db_session.exec(statement)
    return results.first()


def get_deployed_strategy_by_strategy_id(
    db_session: Session,
    strategy_id: int
):
    statement = select(DeployedStrategy).where(
        DeployedStrategy.strategy_id == strategy_id,
        DeployedStrategy.status == "RUNNING"
    )
    results = db_session.exec(statement)
    return results.first()
