from typing import Annotated
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..models.db_models import User
from ..services import auth_service, strategy_service
from ..dao import db

router = APIRouter(tags=['dashboard'])
templates = Jinja2Templates(directory="app/templates")


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    results = strategy_service.get_deployed_strategies_by_user_id(db_session, current_user.id)
    pt = []
    live = []
    formatted_date = datetime.now().strftime("%Y-%m-%d")
    for strategy, deployed_strategy, user_strategy in results:
        if strategy.type == 'PT':
            pt.append((strategy, deployed_strategy, strategy_service.get_pnl(strategy.file_name, formatted_date)))
        else:
            live.append((strategy, deployed_strategy, strategy_service.get_pnl(strategy.file_name, formatted_date)))

    context = {
        "request": request,
        "base_url": request.state.base_url,  # Access base URL
        "title": "Dashboard",
        "current_menu": "Dashboard",
        "pt": pt,
        "live": live,
        "user_type": current_user.user_type
    }    
    return templates.TemplateResponse(
         "dashboard.html", context
    )