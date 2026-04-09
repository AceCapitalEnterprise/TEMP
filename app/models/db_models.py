from sqlmodel import Field, SQLModel, Column, TIMESTAMP, text, Relationship
from datetime import datetime
from typing import Optional

from .base_models import UserBase


class User(UserBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    password: str
    deployed_strategies: list["DeployedStrategy"] = Relationship(back_populates="user")


class Strategy(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    type: str
    description: str
    start_hour: int
    start_minute: int
    end_hour: int
    end_minute: int
    expiry: str
    fut_expiry: str
    file_name: str


class UserStrategy(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    user_id: int | None = Field(
        default=None,
        foreign_key="user.id"
    )
    strategy_id: int | None = Field(
        default=None,
        foreign_key="strategy.id"
    )


class SessionToken(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    token: str
    account_id: str
    api_key: str
    api_secret: str
    created_on: Optional[datetime] = Field(sa_column=Column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
    ))
    deployed_strategies: list["DeployedStrategy"] = Relationship(back_populates="session_token")


class DeployedStrategy(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    status: str
    start_hour: int
    start_minute: int
    end_hour: int
    end_minute: int
    expiry: str
    fut_expiry: str | None = None
    qty: int | None = None
    strategy_id: int | None = Field(
        default=None,
        foreign_key="strategy.id"
    )
    user_id: int | None = Field(
        default=None,
        foreign_key="user.id"
    )
    session_token_id: int | None = Field(
        default=None,
        foreign_key="sessiontoken.id"
    )
    user: User | None = Relationship(back_populates="deployed_strategies")
    session_token: SessionToken | None = Relationship(back_populates="deployed_strategies")
