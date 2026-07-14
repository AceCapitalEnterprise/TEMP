from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

# NEW IMPORTS FOR SCHEDULER AND DB CLEANUP
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor as ApsProcessExecutor, ThreadPoolExecutor as ApsThreadExecutor
from sqlmodel import Session, delete
from .models.db_models import TradeNotification
from .dao.db import engine

from .models import db_models
from .dao.db import create_db_and_tables, create_super_admin, insert_strategy
from .routers import auth, dashboard, strategy, user, report, session_token, change_password, greeks, rv_chart, rv_live, credentials ,vega_live_chart
from .services import auth_service
from .strategies.strategy_autologin import run_auto_login

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
app.include_router(credentials.router)
app.include_router(vega_live_chart.router)

templates = Jinja2Templates(directory="app/templates")


@app.exception_handler(auth_service.UnauthorizedException)
async def unauthorized_exception_handler(request: Request, exc: auth_service.UnauthorizedException):
    return templates.TemplateResponse(
        request=request, name="login.html", context={"error_message": "Session expired. Please login again."}
    )


def clear_trading_notifications():
    """Deletes all records from the TradeNotification table."""
    try:
        with Session(engine) as session:
            statement = delete(TradeNotification)
            session.exec(statement)
            session.commit()
            print("Daily Cleanup: Cleared trading notifications successfully.")
    except Exception as e:
        print(f"Daily Cleanup Failed: {e}")


@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    
    # Configure main app scheduler with dedicated system process separation capabilities
    executors = {
        'default': ApsThreadExecutor(20),
        'processpool': ApsProcessExecutor(5)
    }
    # START THE BACKGROUND SCHEDULER
    scheduler = BackgroundScheduler(executors=executors)

    # System optimization: Auto login execution routed cleanly to its own single process run
    scheduler.add_job(run_auto_login, 'cron', hour=4, minute=00, executor='processpool')
    scheduler.add_job(run_auto_login, 'cron', hour=4, minute=15, executor='processpool')

    scheduler.add_job(clear_trading_notifications, 'cron', hour=15, minute=50, executor='default')
    scheduler.start()

# Custom Middleware to Add Base URL
class BaseURLMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request.state.base_url = request.url_for("static", path="")
        response = await call_next(request)
        return response

app.add_middleware(BaseURLMiddleware)


@app.get("/", response_class=HTMLResponse)
async def signin(request: Request):
    return templates.TemplateResponse(
        request=request, name="login.html"
    )
