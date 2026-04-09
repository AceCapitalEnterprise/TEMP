from datetime import datetime, timedelta, timezone
from passlib.context import CryptContext

from fastapi import Request
from fastapi.param_functions import Depends
from fastapi.security import OAuth2PasswordBearer
from sqlmodel import Session
import jwt
from jwt.exceptions import InvalidTokenError

from ..dao.user_dao import get_user_by_username
from ..dao.db import get_session

SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
ALGORITHM = "HS256"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class UnauthorizedException(Exception):
    def __init__(self, name: str):
        self.name = name


def authenticate_user(db_session: Session, username: str, password: str):
    user = get_user_by_username(db_session, username)
    if not user:
        return False
    if not verify_password(password, user.password):
        return False
    return user


async def get_current_user(
    request: Request,
    db_session: Session = Depends(get_session)
):
    credentials_exception = UnauthorizedException(
        name="Unauthorized"
    )
    try:
        token = request.cookies.get("access_token")
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except InvalidTokenError:
        raise credentials_exception
    user = get_user_by_username(db_session=db_session, username=username)
    if user is None:
        raise credentials_exception
    return user


def get_password_hash(password):
    return pwd_context.hash(password)


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
