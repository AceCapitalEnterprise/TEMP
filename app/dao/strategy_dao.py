from sqlmodel import Session, select

from ..models.db_models import Strategy, DeployedStrategy, UserStrategy

def get_all_strategies(db_session: Session):
    statement = select(Strategy)
    results = db_session.exec(statement)
    return results.all()


def get_strategy_by_id(db_session: Session, strategy_id: int):
    statement = select(Strategy).where(Strategy.id == strategy_id)
    results = db_session.exec(statement)
    return results.first()


def get_strategy_by_id(db_session: Session, strategy_id: int):
    statement = select(Strategy).where(Strategy.id == strategy_id)
    results = db_session.exec(statement)
    return results.first()

#<---------------------------- change --------------------->

def create_strategy(db_session: Session, strategy_data: dict):
    strategy = Strategy(**strategy_data)
    db_session.add(strategy)
    db_session.commit()
    db_session.refresh(strategy)
    return strategy

def delete_strategy(db_session: Session, strategy_id: int):
    strategy = get_strategy_by_id(db_session, strategy_id)
    if strategy:
        db_session.delete(strategy)
        db_session.commit()
    return strategy

#<---------------------------- change --------------------->