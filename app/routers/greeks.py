from typing import Annotated
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.param_functions import Depends
from sqlmodel import Session

from ..models.db_models import User
from ..services import auth_service
from ..dao import db
from ..config import settings

router = APIRouter(tags=['greeks'])
templates = Jinja2Templates(directory="app/templates")


@router.get("/greeks", response_class=HTMLResponse)
async def get_greeks_data(
    request: Request,
    current_user: Annotated[User, Depends(auth_service.get_current_user)],
    db_session: Session = Depends(db.get_session)
):
    # Look for Greek.csv file in the CSV folder
    csv_folder = Path(settings.csv_folder_path)
    greek_data = []
    file_exists = False
    last_updated = None
    
    # Check if Greek.csv exists
    greek_file_path = csv_folder / "Greek.csv"
    if greek_file_path.exists():
        file_exists = True
        
        # Get last modified time
        try:
            timestamp = os.path.getmtime(greek_file_path)
            last_updated = datetime.fromtimestamp(timestamp)
        except Exception as e:
            print(f"Error getting file timestamp: {e}")
        
        # Read the CSV data
        try:
            df = pd.read_csv(greek_file_path)
            # Get the last 100 rows or all rows if less than 100
            # df = df.tail(100)
            greek_data = df.to_dict('records')
        except Exception as e:
            print(f"Error reading Greek CSV: {e}")
    
    context = {
        "request": request,
        "base_url": request.state.base_url,
        "title": "Greeks Data",
        "current_menu": "Greeks",
        "file_exists": file_exists,
        "last_updated": last_updated,
        "greek_data": greek_data,
        "user_type": current_user.user_type
    }
    
    return templates.TemplateResponse(
        "greeks.html", context
    )

