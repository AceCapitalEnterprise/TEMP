from typing import Annotated
from datetime import datetime,timedelta

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session, select, update

# Make sure Strategy is imported here so we can JOIN it
from ..models.db_models import TradeNotification, Strategy, User
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

#==================================================================================
#  Trade Notification Route
#==================================================================================

@router.get("/api/notifications/today")
async def get_today_notifications(
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Perform a JOIN to get TradeNotification AND its parent Strategy
    statement = (
        select(TradeNotification, Strategy)
        .join(Strategy)
        .where(TradeNotification.user_id == current_user.id)
        .where(TradeNotification.execution_time >= today_start)
        .order_by(TradeNotification.execution_time.desc())
    )
    
    results = db_session.exec(statement).all()
    
    return [
        {
            "id": trade.id,
            "strategy_name": strategy.name,             
            "strategy_type": strategy.category,         
            "account_id": trade.account_id,             
            "time": (trade.execution_time + timedelta(hours=5, minutes=30)).strftime("%I:%M:%S %p"),
            "action": trade.action.upper(),
            "symbol": trade.symbol,
            "instrument_detail": trade.instrument_detail,
            "price": f"₹{trade.price:.2f}",
            "qty": trade.qty,
            "is_read": trade.is_read
        }
        for trade, strategy in results
    ]

@router.post("/api/notifications/mark-read")
async def mark_notifications_read(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    # Parse specific IDs sent from the frontend Intersection Observer
    payload = await request.json()
    notification_ids = payload.get("notification_ids", [])
    
    if notification_ids:
        statement = (
            update(TradeNotification)
            .where(TradeNotification.user_id == current_user.id)
            .where(TradeNotification.id.in_(notification_ids))
            .values(is_read=True)
        )
        db_session.execute(statement)
        db_session.commit()
        
    return {"status": "success"}