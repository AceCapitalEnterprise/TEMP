from datetime import timedelta
from typing import Annotated
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from fastapi.responses import RedirectResponse
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..services import auth_service
from ..dao.db import get_session

router = APIRouter(tags=['authentication'])
ACCESS_TOKEN_EXPIRE_MINUTES = 60

templates = Jinja2Templates(directory="app/templates")


@router.post("/token")
async def login_for_access_token(
    request: Request,
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    db_session: Session = Depends(get_session),
):
    user = auth_service.authenticate_user(
        db_session,
        form_data.username,
        form_data.password
    )
    if not user:
        context = {
            "request": request,
            "error_message": "Incorrect User Name or Password"
        }  

        return templates.TemplateResponse(
            "login.html",
            context
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth_service.create_access_token(
        data={"sub": user.email}, expires_delta=access_token_expires
    )

    response = RedirectResponse(url="/dashboard", status_code=303)
    response.set_cookie(key="access_token", value=access_token, httponly=True)

    return response

@router.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@router.get("/logout", response_class=RedirectResponse)
async def logout():
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(key="access_token")
    return response
