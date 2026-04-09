from typing import Annotated
import os
import pandas as pd

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..models.db_models import User
from ..services import auth_service
from ..dao import db
from ..config import settings

router = APIRouter(tags=['rv-live-chart'])
templates = Jinja2Templates(directory="app/templates")


@router.get("/rv-live-chart", response_class=HTMLResponse)
async def rv_live_chart_page(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    context = {
        "request": request,
        "base_url": request.state.base_url,
        "title": "RV Live Chart",
        "current_menu": "RV_Live_Chart",
        "user_type": current_user.user_type
    }
    return templates.TemplateResponse(
        "rv_live_chart.html",
        context
    )


@router.get("/api/rv-live-data")
async def get_rv_live_data(
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    try:
        # Path to RV_Live.csv
        csv_path = os.path.join(settings.csv_folder_path, "RV_Live.csv")
        
        # Check if file exists
        if not os.path.exists(csv_path):
            return JSONResponse(
                status_code=404,
                content={"error": "RV_Live.csv file not found"}
            )
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Check if required columns exist
        if 'datetime' not in df.columns or 'RV' not in df.columns:
            return JSONResponse(
                status_code=400,
                content={"error": "CSV file missing required columns (datetime, RV)"}
            )
        
        # Convert datetime column to string format
        df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Remove NaN values from RV column
        df = df.dropna(subset=['RV'])
        
        # Prepare data for chart
        labels = df['datetime'].tolist()
        values = df['RV'].tolist()
        
        return JSONResponse(content={
            "labels": labels,
            "values": values
        })
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Error reading RV_Live.csv: {str(e)}"}
        )