from sqlmodel import create_engine, Session, SQLModel

from ..models.create_models import UserCreate
from ..models.db_models import Strategy
from .user_dao import create_user

from ..config import settings

sqlite_file_name = settings.db_path
sqlite_url = f"sqlite:///{sqlite_file_name}"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def create_super_admin(password: str):
    user_create = UserCreate(
        name='Super Admin',
        email='admin@strategyengine.com', 
        password=password,
        user_type='SUPER_ADMIN', 
        is_active=1
    )    
    create_user(Session(engine), user_create)

def insert_strategy(name):
    strat = Strategy(
        name=name        
    )
    db_session = Session(engine)
    db_session.add(strat)
    db_session.commit()
    db_session.refresh(strat)
    
def get_session():
    try:
        with Session(engine) as session:
            yield session
    finally:
        session.close()  
