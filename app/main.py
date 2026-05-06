from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from .models import db_models
from .dao.db import create_db_and_tables, create_super_admin, insert_strategy
from .routers import auth, dashboard, strategy, user, report, session_token, change_password,greeks, rv_chart,rv_live
from .services import auth_service

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(auth.router)
app.include_router(dashboard.router)
app.include_router(strategy.router)
app.include_router(user.router)
app.include_router(report.router)
app.include_router(session_token.router)
app.include_router(change_password.router)
app.include_router(greeks.router)
app.include_router(rv_chart.router)
app.include_router(rv_live.router)

templates = Jinja2Templates(directory="app/templates")


@app.exception_handler(auth_service.UnauthorizedException)
async def unauthorized_exception_handler(request: Request, exc: auth_service.UnauthorizedException):
    return templates.TemplateResponse(
        request=request, name="login.html", context={"error_message": "Session expired. Please login again."}
    )


@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    #create_super_admin(auth_service.get_password_hash('123456'))
    #insert_strategy('ORB')
    #insert_strategy('ICICI')


# Custom Middleware to Add Base URL
class BaseURLMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Add base URL to the request scope
        request.state.base_url = request.url_for("static", path="")
        response = await call_next(request)
        return response

app.add_middleware(BaseURLMiddleware)


@app.get("/", response_class=HTMLResponse)
async def signin(request: Request):
    return templates.TemplateResponse(
        request=request, name="login.html"
    )
