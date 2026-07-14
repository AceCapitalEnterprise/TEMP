import pandas as pd
import glob
import requests
import json
import os
from datetime import datetime

# Import your settings from config.py
from ..config import settings 

# Your Dictionary to map raw filenames to clean dashboard names
STRATEGY_MAPPING = {
    "ds_directional": "DS",
    "carara_directional": "Carara",
    "rpsinghal_directional": "RPS",
    "sarlasinghal_directional": "Sarla Singhal",
    "rahulsinghal13_directional": "RS13", 
    "directional": "Ent"  
}

def push_data_to_powerbi():
    """Reads strategy CSVs and pushes live unclosed positions to Power BI."""
    
    if not settings.power_bi_push_url:
        print("Power BI Push URL is not configured.")
        return

    csv_pattern = os.path.join(settings.csv_folder_path, 'unclosed_positions_*.csv')
    csv_files = glob.glob(csv_pattern)
    all_positions = []
    
    # Format time exactly how Power BI expects it
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    for file in csv_files:
        filename = os.path.basename(file)
        
        name_without_ext = filename.replace('.csv', '') 
        opt_type = name_without_ext[-2:].upper() # Extracts 'CE' or 'PE'
        
        try:
            # Extract the raw middle part and make it lowercase to match the dictionary
            raw_strategy = name_without_ext[19:-3].lower() 
            strategy = STRATEGY_MAPPING.get(raw_strategy, raw_strategy.capitalize())
            
        except Exception:
            continue 
            
        try:
            df = pd.read_csv(file)
        except pd.errors.EmptyDataError:
            continue 
            
        for _, row in df.iterrows():
            # Calculate PnL strictly in points
            if str(row['action']).lower() == 'buy':
                current_pnl = round(float(row['ltp']) - float(row['premium']), 2)
            else:
                current_pnl = round(float(row['premium']) - float(row['ltp']), 2)

            # Build the exact payload Power BI is expecting based on our dataset schema
            record = {
                "snapshot_time": current_time,
                "strategy_name": strategy,
                "action": str(row['action']).capitalize(),
                "strike": int(row['strike']),
                "CE_or_PE": opt_type,
                "premium": round(float(row['premium']), 2),
                "trailing_sl": round(float(row['trailing_sl']), 2),
                "ltp": round(float(row['ltp']), 2),
                "pnl": current_pnl
            }
            all_positions.append(record)

    if not all_positions:
        print(f"[{datetime.now()}] PowerBI: No open positions found right now.")
        return 

    # Execute the API Push
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(settings.power_bi_push_url, data=json.dumps(all_positions), headers=headers)
        if response.status_code == 200:
            print(f"[{datetime.now()}] PowerBI: Successfully pushed {len(all_positions)} active positions.")
        else:
            print(f"[{datetime.now()}] PowerBI Push failed: {response.text}")
    except Exception as e:
         print(f"[{datetime.now()}] PowerBI API error: {e}")