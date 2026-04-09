
from typing import Annotated
from datetime import datetime
import sys

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse,RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..models.create_models import DeployStrategyCreate
from ..models.db_models import User
from ..services import auth_service, strategy_service, user_service, session_token_service
from ..dao import db

router = APIRouter(tags=['strategy'])
templates = Jinja2Templates(directory="app/templates")


@router.get("/strategies", response_class=HTMLResponse)
async def strategies(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    session_token_list = session_token_service.get_all_session_tokens(db_session)
    user_strategies = user_service.get_user_strategies_by_user_id(db_session, current_user.id)
    results = strategy_service.get_all_strategies(db_session)

    final_data = []
    for strategy in results:
        for user_strategy in user_strategies:
            if user_strategy.strategy_id == strategy.id:
                final_data.append((strategy, strategy_service.get_deployed_strategy_by_strategy_id(db_session, strategy.id)))
                break

    context = {
        "request": request,
        "base_url": request.state.base_url,  # Access base URL
        "title": "Strategies",
        "current_menu": "Strategies",
        "results": final_data,
        "accounts": session_token_list,
        "user_type": current_user.user_type
    }  
    return templates.TemplateResponse(
        "strategies.html",
        context
    )


@router.post("/strategies/{strategy_id}/deploy", response_class=HTMLResponse)
async def deploy_strategy(
    request: Request,
    strategy_id: int,
    strategy_data: Annotated[DeployStrategyCreate, Form()],
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    try:
        deployed_strategy = strategy_service.deploy_strategy(db_session, strategy_id, current_user.id, strategy_data)
        return RedirectResponse(url="/strategies", status_code=303)
    except Exception as ex:
        ex_type, ex_value, ex_traceback = sys.exc_info()

        context = {
            "request": request,
            "base_url": request.state.base_url,  # Access base URL
            "title": "Strategies",
            "error_message": "Failed to deploy strategy: " + str(ex_value),
            "user_type": current_user.user_type
        }  
        return templates.TemplateResponse(
            "error.html",
            context
        )


@router.post("/strategies/{strategy_id}/stop", response_class=HTMLResponse)
async def deploy_strategy(
    strategy_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    strategy_service.stop_strategy(db_session, strategy_id)
    return RedirectResponse(url="/strategies", status_code=303)


#<---------------------------- change --------------------->

# @router.post("/strategies", response_class=JSONResponse)
# async def add_strategy(
#     request: Request,
#     current_user: Annotated[User, Depends(auth_service.get_current_user)],
#     db_session: Session = Depends(db.get_session)
# ):
#     data = await request.json()
#     try:
#         strategy_service.create_strategy(db_session, data)
#         return JSONResponse(status_code=201, content={"message": "Strategy added Successfully"})
#     except Exception as e:
#         print(e)
#         return JSONResponse(status_code=403, content={"message": "Some error occurred"})
@router.post("/strategies", response_class=JSONResponse)
async def add_strategy(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    data = await request.json()
    try:
        new_strategy = strategy_service.create_strategy(db_session, data)
        
        # Auto-assign the new strategy to admin (user_id = 1)
        ADMIN_USER_ID = 1
        existing = user_service.get_user_strategies_by_user_id_and_strategy_id(
            db_session, ADMIN_USER_ID, new_strategy.id
        )
        if not existing:
            user_service.create_user_strategy(db_session, ADMIN_USER_ID, new_strategy.id)

        return JSONResponse(status_code=201, content={"message": "Strategy added Successfully"})
    except Exception as e:
        print(e)
        return JSONResponse(status_code=403, content={"message": "Some error occurred"})

@router.delete("/strategies/{strategy_id}", response_class=JSONResponse)
async def delete_strategy(
    strategy_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    strategy = strategy_service.delete_strategy(db_session, strategy_id)
    if not strategy:
        return JSONResponse(status_code=403, content={"message": "Strategy not found"})
    return JSONResponse(content={"message": "Strategy deleted successfully"})

#<---------------------------- change --------------------->