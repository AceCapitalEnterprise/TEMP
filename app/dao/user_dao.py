from sqlmodel import Session, select

from ..models.db_models import User
from ..models.create_models import UserCreate


def get_user_by_username(db_session: Session, username: str):
    statement = select(User).where(User.email == username)
    results = db_session.exec(statement)
    return results.first()


def create_user(db_session: Session, user_create: UserCreate):
    try:
        user = User.model_validate(user_create)
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)
        return user
    except Exception as e:
        repr(e)


def get_users(db_session: Session):
    statement = select(User).where(User.user_type != 'SUPER_ADMIN')
    results = db_session.exec(statement)
    return results.all()


def get_user_by_id(db_session: Session, user_id: int):
    statement = select(User).where(User.id == user_id)
    results = db_session.exec(statement)
    return results.first()

def update_user(db_session: Session, user_create: UserCreate, user_id: int):
    user = get_user_by_id(db_session, user_id)
    if user:
        user.name = user_create['name']
        user.email = user_create['email']
        user.mobile = user_create['mobile']
        db_session.commit()
        db_session.refresh(user)
    return user

def delete_user(db_session: Session, user_id: int):
    try:
        user = get_user_by_id(db_session, user_id)
        if user:
            db_session.delete(user)
            db_session.commit()
        return user
    except Exception as e:
        db_session.rollback()  # Rollback in case of an error
        raise e
