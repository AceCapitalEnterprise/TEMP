from sqlmodel import SQLModel
from .base_models import UserBase
from datetime import datetime
from typing import Optional


class UserCreate(UserBase):
    password: str


class DeployStrategyCreate(SQLModel):
    session_token_id: int
    start_hour: str
    start_minute: str
    end_hour: str
    end_minute: str
    expiry: str
    fut_expiry: str
    qty: str
    max_position:str 
    slicer:str
    portfolio_size:str
    end_month_size:str
    first_day_of_month:str
    last_day_of_month:str
    portfolio_price:str
    interval:str#change
    symbol:str#change
    strike:str#change
    auth_code:str#change
    action:str#change
    exchange:str#change
    product:str#change
    price:str#change
    right:str#change


class SessionTokenCreate(SQLModel):
    session_token_id: int
    token: str
    created_on: datetime = datetime.today()


class PasswordCreate(SQLModel):
    old_password: str
    password: str

