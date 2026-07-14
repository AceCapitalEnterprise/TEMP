from typing import Annotated
from datetime import datetime

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..models.create_models import PasswordCreate
from ..services import auth_service
from ..models.db_models import User
from ..dao import db

router = APIRouter(tags=['change-password'])
templates = Jinja2Templates(directory="app/templates")

@router.get("/change-password", response_class=HTMLResponse)
async def get_change_password_page(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)]
):
    context = {
        "request": request,
        "base_url": request.state.base_url,  # Access base URL
        "title": "Change Password",
        "current_menu": "Password",
        "user_type": current_user.user_type
    }   
    return templates.TemplateResponse(
        "change_password.html", context  
    )


@router.post("/change-password", response_class=HTMLResponse)
async def change_password(
    request: Request,
    password_create: Annotated[PasswordCreate, Form()],
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    message = 'Incorrect current password!'
    if(auth_service.verify_password(password_create.old_password, current_user.password)):
        message = 'Password updated successfully!'
        current_user.password = auth_service.get_password_hash(password_create.password)
        db_session.add(current_user)
        db_session.commit()

    context = {
        "request": request,
        "base_url": request.state.base_url,  # Access base URL
        "title": "Change Password",
        "current_menu": "Password",
        "message": message,
        "user_type": current_user.user_type
    }   
    return templates.TemplateResponse(
        "change_password.html", context  
    )
