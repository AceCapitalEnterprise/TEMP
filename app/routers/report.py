
from typing import Annotated
import os
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..models.db_models import User
from ..services import auth_service, strategy_service, user_service
from ..dao import db
from ..config import settings

router = APIRouter(tags=['report'])
templates = Jinja2Templates(directory="app/templates")


@router.get("/reports", response_class=HTMLResponse)
async def strategies(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    user_strategies = user_service.get_user_strategies_by_user_id(db_session, current_user.id)
    results = strategy_service.get_all_strategies(db_session)

    final_data = []
    for strategy in results:
        for user_strategy in user_strategies:
            if user_strategy.strategy_id == strategy.id:
                file_path = settings.csv_folder_path + strategy.file_name
                file_exists = os.path.exists(file_path)

                pnl = strategy_service.get_pnl(strategy.file_name) if file_exists else None

                last_updated = None
                if file_exists:
                    timestamp = os.path.getmtime(file_path)
                    last_updated = datetime.fromtimestamp(timestamp)
                report_file = strategy_service.save_strategy_report(strategy.file_name)

                # final_data.append((strategy, strategy_service.get_pnl(strategy.file_name), os.path.exists(file_path)))
                final_data.append({
                    "strategy": strategy,
                    "pnl": pnl,
                    "file_exists": file_exists,
                    "last_updated": last_updated,
                    "report_file": report_file
                })

    context = {
        "request": request,
        "base_url": request.state.base_url,  # Access base URL
        "title": "Reports",
        "current_menu": "Reports",
        "strategy_list": final_data,
        "user_type": current_user.user_type
    }  
    return templates.TemplateResponse(
        "report.html",
        context
    )


@router.get("/reports/{strategy_id}/download-detail", response_class=HTMLResponse)
async def strategies(
    strategy_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    strategy = strategy_service.get_strategy_by_id(db_session, strategy_id)
    file_path = settings.csv_folder_path + strategy.file_name
    return FileResponse(path=file_path, filename=strategy.file_name, media_type='text/csv')

@router.get("/reports/{strategy_id}/download-report", response_class=HTMLResponse)
async def strategies(
    strategy_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    strategy = strategy_service.get_strategy_by_id(db_session, strategy_id)
    file_name = strategy.file_name.replace(".csv", "_report.csv")
    file_path = settings.csv_folder_path + file_name
    return FileResponse(path=file_path, filename=file_name, media_type='text/csv')
