from sqlmodel import Session


from ..models.create_models import SessionTokenCreate
from ..dao import session_token_dao

def get_all_session_tokens(db_session: Session):
    return session_token_dao.get_all_session_tokens(db_session)


def get_session_token_by_id(db_session: Session, session_token_id: int):
    return session_token_dao.get_session_token_by_id(db_session, session_token_id)


def create_session_token(db_session: Session, session_token_create: SessionTokenCreate):
    session_token = get_session_token_by_id(db_session, session_token_create.session_token_id)
    session_token.token = session_token_create.token
    session_token.created_on = session_token_create.created_on
    db_session.add(session_token)
    db_session.commit()
    db_session.refresh(session_token)

#<------------------ change ----------------->


def add_new_session_token(db_session: Session, account_data: dict):
    return session_token_dao.add_session_token(db_session, account_data)

def delete_session_token(db_session: Session, session_token_id: int):
    return session_token_dao.delete_session_token(db_session, session_token_id)

#<------------------ change ----------------->