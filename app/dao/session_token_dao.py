from sqlmodel import Session, select, delete

from ..models.create_models import SessionTokenCreate
from ..models.db_models import SessionToken, DeployedStrategy


def get_all_session_tokens(db_session: Session):
    statement = select(SessionToken)
    results = db_session.exec(statement)
    return results.all()


def get_session_token_by_id(db_session: Session, session_token_id: int):
    statement = select(SessionToken).where(SessionToken.id == session_token_id)
    results = db_session.exec(statement)
    return results.first()


def create_session_token(db_session: Session, session_token_create: SessionTokenCreate):
    session_token = SessionToken.model_validate(session_token_create)
    db_session.add(session_token)
    db_session.commit()
    db_session.refresh(session_token)
    return session_token

#<------------------ change ----------------->
# Add a new session token via dictionary
def add_session_token(db_session: Session, account_data: dict):
    new_token = SessionToken(**account_data)
    db_session.add(new_token)
    db_session.commit()
    db_session.refresh(new_token)
    return new_token

# Delete a session token (and its linked strategies)
def delete_session_token(db_session: Session, session_token_id: int):
    token_obj = get_session_token_by_id(db_session, session_token_id)
    if token_obj:
        # 1. Delete all DeployedStrategies tied to this account to prevent IntegrityError
        delete_statement = delete(DeployedStrategy).where(DeployedStrategy.session_token_id == session_token_id)
        db_session.exec(delete_statement)
        
        # 2. Now it is safe to delete the account token
        db_session.delete(token_obj)
        db_session.commit()
        
    return token_obj

#<------------------ change ----------------->