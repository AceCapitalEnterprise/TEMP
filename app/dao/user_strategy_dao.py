from sqlmodel import Session, select, delete

from ..models.db_models import UserStrategy

def get_user_strategies_by_user_id(db_session: Session, user_id: int):
    statement = select(UserStrategy).where(UserStrategy.user_id == user_id)
    results = db_session.exec(statement)
    return results.all()


def get_user_strategies_by_user_id_and_strategy_id(db_session: Session, user_id: int, strategy_id):
    statement = select(UserStrategy).where(UserStrategy.user_id == user_id, UserStrategy.strategy_id == strategy_id)
    results = db_session.exec(statement)
    return results.all()


def create_user_strategy(db_session: Session, user_id: int, strategy_id: int):
    user_strategy = UserStrategy(
        user_id=user_id,
        strategy_id=strategy_id
    )
    db_session.add(user_strategy)
    db_session.commit()
    db_session.refresh(user_strategy)
    return user_strategy


def delete_user_strategy_by_id(db_session: Session, user_strategy_id: int):
    statement = delete(UserStrategy).where(UserStrategy.id == user_strategy_id)
    db_session.exec(statement)
    db_session.commit()


def delete_user_strategy_by_user_id(db_session: Session, user_id: int):
    statement = delete(UserStrategy).where(UserStrategy.user_id == user_id)
    db_session.exec(statement)
    db_session.commit()