from typing import Annotated
from datetime import datetime

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse , JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..models.create_models import SessionTokenCreate
from ..services import auth_service, session_token_service
from ..models.db_models import User
from ..dao import db

router = APIRouter(tags=['account'])
templates = Jinja2Templates(directory="app/templates")

@router.get("/session-tokens", response_class=HTMLResponse)
async def get_session_tokens(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    session_token_list = session_token_service.get_all_session_tokens(db_session)

    context = {
        "request": request,
        "base_url": request.state.base_url,  # Access base URL
        "title": "Accounts",
        "current_menu": "Accounts",
        "session_tokens": session_token_list,
        "user_type": current_user.user_type
    }   
    return templates.TemplateResponse(
        "session_tokens.html", context  
    )


@router.post("/session-tokens", response_class=HTMLResponse)
async def update_session_token(
    request: Request,
    session_token_create: Annotated[SessionTokenCreate, Form()],
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    session_token_create.created_on = datetime.today()
    session_token_service.create_session_token(db_session, session_token_create)

    return RedirectResponse(url="/session-tokens", status_code=303)

#<------------------ change ----------------->

@router.post("/session-tokens/add", response_class=JSONResponse)
async def add_new_account(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    data = await request.json()
    # Set the created_on date to right now
    data["created_on"] = datetime.now()
    
    try:
        session_token_service.add_new_session_token(db_session, data)
        return JSONResponse(status_code=201, content={"message": "Account added Successfully"})
    except Exception as e:
        print(e)
        return JSONResponse(status_code=403, content={"message": "Error adding account"})

@router.delete("/session-tokens/{token_id}", response_class=JSONResponse)
async def delete_account(
    token_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    token_obj = session_token_service.delete_session_token(db_session, token_id)
    if not token_obj:
        return JSONResponse(status_code=404, content={"message": "Account not found"})
    return JSONResponse(content={"message": "Account deleted successfully"})

#<------------------ change ----------------->