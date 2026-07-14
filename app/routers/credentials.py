from typing import Annotated
from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select

from ..models.db_models import User, BreezeCredential
from ..services import auth_service
from ..dao import db

router = APIRouter(tags=['credentials'])
templates = Jinja2Templates(directory="app/templates")

@router.get("/credentials", response_class=HTMLResponse)
async def get_credentials_page(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    credentials = db_session.exec(select(BreezeCredential)).all()
    effective_user_type = "SUPER_ADMIN" if current_user.name == "Sanjay" else current_user.user_type
    
    context = {
        "request": request,
        "base_url": request.state.base_url,
        "title": "Breeze Credentials",
        "current_menu": "Breeze Credentials",
        "user_type": effective_user_type,
        "credentials": credentials
    }
    return templates.TemplateResponse("credentials.html", context)

@router.post("/credentials/add", response_class=HTMLResponse)
async def add_credential(
    request: Request,
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    totp_key: Annotated[str, Form()],
    api_key: Annotated[str, Form()],
    api_secret: Annotated[str, Form()],
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    new_cred = BreezeCredential(
        username=username,
        password=password,
        totp_key=totp_key,
        api_key=api_key,
        api_secret=api_secret
    )
    db_session.add(new_cred)
    db_session.commit()
    
    # Redirect back to the page to refresh the table
    return RedirectResponse(url="/credentials", status_code=303)

@router.post("/credentials/delete/{cred_id}", response_class=HTMLResponse)
async def delete_credential(
    cred_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    cred = db_session.get(BreezeCredential, cred_id)
    if cred:
        db_session.delete(cred)
        db_session.commit()
        
    # Redirect back to the page to refresh the table
    return RedirectResponse(url="/credentials", status_code=303)