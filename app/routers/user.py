
from typing import Annotated

from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..models.db_models import User, UserStrategy
from ..services import auth_service, user_service, strategy_service
from ..dao import db

router = APIRouter(tags=['user'])
templates = Jinja2Templates(directory="app/templates")

@router.get("/users", response_class=HTMLResponse)
async def get_users(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    users_list = user_service.get_all_users(db_session)
    context = {
        "request": request,
        "base_url": request.state.base_url,  # Access base URL
        "title": "Users",
        "current_menu": "Users",
        "users_list": users_list,
        "user_type": current_user.user_type
    }   
    return templates.TemplateResponse(
        "users.html", context
       
    )

@router.post("/users", response_class=HTMLResponse)
async def deploy_strategy(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):

    data = await request.json()
    
    try:
        if data['id'] == '0' or data['id'] == "":
            del data['id']
            data['is_active'] = 1
            data['user_type'] = 'EMPLOYEE'
            newuser = user_service.create_user(db_session, data)
            return JSONResponse(status_code=201, content={"message":"A new user added Successfully"})
        else:
            data['id'] = int(data['id'])
            newuser = user_service.update_user(db_session, data,  data['id'])
            return JSONResponse(status_code=201, content={"message":"User updated Successfully"})
    except Exception as e:
         print(e)
         return JSONResponse(status_code=403, content={"message":"Some error occurred"})


@router.get("/users/{user_id}/edit", response_class=HTMLResponse)
async def get_users(   
    user_id: int,
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    user_data = user_service.get_user_by_id(db_session, user_id)

    json_data = jsonable_encoder(user_data)
    return JSONResponse(content=json_data)


@router.delete("/users/{user_id}", response_class=HTMLResponse)
async def delete_user(
    user_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)):
    user = user_service.delete_user(db_session, user_id)
    if not user:
        return JSONResponse(status_code=403, content={"message":"User not found"})
    json_data = jsonable_encoder({"message": "User deleted successfully"})
    return JSONResponse(content=json_data)


@router.get("/users/{user_id}/strategies", response_class=HTMLResponse)
async def user_strategies(
    request: Request,
    user_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    strategies = strategy_service.get_all_strategies(db_session)
    user_strategies = user_service.get_user_strategies_by_user_id(db_session, user_id)
    results = []
    for strategy in strategies:
        us: UserStrategy = None
        for user_strategy in user_strategies:
            if user_strategy.strategy_id == strategy.id:
                us = user_strategy
                break
        
        results.append((strategy, us))

    context = {
        "request": request,
        "base_url": request.state.base_url,  # Access base URL
        "title": "Users",
        "results": results,
        "user_id": user_id,
        "user_type": current_user.user_type
    }  
    return templates.TemplateResponse(
        "user-strategies.html",
        context
    )


@router.post("/users/{user_id}/assign-strategy", response_class=HTMLResponse)
async def assign_strategy(
    request: Request,
    user_id: int,
    strategy_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    user_strategy = user_service.get_user_strategies_by_user_id_and_strategy_id(db_session, user_id, strategy_id)
    if len(user_strategy) == 0:
        user_strategy = user_service.create_user_strategy(db_session, user_id, strategy_id)

    return RedirectResponse(url="/users/" + str(user_id) + "/strategies", status_code=303)


@router.post("/users/{user_id}/remove-strategy", response_class=HTMLResponse)
async def assign_strategy(
    request: Request,
    user_id: int,
    user_strategy_id: int,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    user_service.delete_user_strategy_by_id(db_session, user_strategy_id)

    return RedirectResponse(url="/users/" + str(user_id) + "/strategies", status_code=303)
