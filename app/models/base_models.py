from sqlmodel import SQLModel


class UserBase(SQLModel):
    name: str
    email: str
    user_type: str
    is_active: int
    mobile: str
