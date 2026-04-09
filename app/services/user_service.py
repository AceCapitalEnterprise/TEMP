from sqlmodel import Session

from . import auth_service

from ..dao import user_dao, user_strategy_dao
from ..models.create_models import UserCreate
from ..models.create_models import UserBase

def get_all_users(db_session: Session):
    return user_dao.get_users(db_session)


def get_user_by_id(db_session: Session, user_id: int):
    return user_dao.get_user_by_id(db_session, user_id)


def delete_user(db_session: Session, user_id: int):
    user_strategy_dao.delete_user_strategy_by_user_id(db_session, user_id)
    return user_dao.delete_user(db_session, user_id)


def create_user(db_session: Session, user_create: UserCreate):  
    user_create['password'] = auth_service.get_password_hash(user_create['password'])
    return user_dao.create_user(db_session, user_create)


def update_user(db_session: Session, user_create: UserBase, user_id):   
    return user_dao.update_user(db_session, user_create, user_id)


def get_user_strategies_by_user_id(db_session: Session, user_id: int):
    return user_strategy_dao.get_user_strategies_by_user_id(db_session, user_id)


def get_user_strategies_by_user_id_and_strategy_id(db_session: Session, user_id: int, strategy_id: int):
    return user_strategy_dao.get_user_strategies_by_user_id_and_strategy_id(db_session, user_id, strategy_id)


def create_user_strategy(db_session: Session, user_id: int, strategy_id: int):
    return user_strategy_dao.create_user_strategy(db_session, user_id, strategy_id)


def delete_user_strategy_by_id(db_session: Session, user_strategy_id: int):
    user_strategy_dao.delete_user_strategy_by_id(db_session, user_strategy_id)
