from abc import ABC, abstractmethod

from sqlmodel import Session


class BaseStrategy(ABC):
    @abstractmethod
    def run(db_session: Session) -> None:
        pass
