import os
import csv
import time
import urllib.request
import urllib.parse
import pyotp
import logging
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import shutil  # <--- New Import --->
import gc      # <--- New Import --->

# Database & Config imports 
from sqlmodel import Session, select
from ..dao.db import engine
from ..models.db_models import BreezeCredential, Strategy, User, UserStrategy
from ..config import settings

# ==========================================
# 1) IMPLEMENT LOGGING
# ==========================================
logger = logging.getLogger("autologin")
logger.setLevel(logging.INFO)
# Avoid duplicating handlers if the module reloads
if not logger.handlers:
    fh = logging.FileHandler("autologin.log")
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def get_session_key(cred: BreezeCredential):
    logger.info(f"Generating new key for {cred.username}.....")
    
    try:
        chrome_options = webdriver.ChromeOptions()
        
        # 1. Point directly to the native Linux installations
        chrome_options.binary_location = "/usr/bin/chromium-browser" 
        service = Service("/usr/bin/chromedriver")
        
        # 2. Give each user a unique, temporary profile
        chrome_options.add_argument(f"--user-data-dir=/tmp/chrome_profile_{cred.username}")
        
        # 3. Essential Chrome Options for Linux / Headless environments
        chrome_options.add_argument('--headless=new')  
        chrome_options.add_argument('--no-sandbox')    
        chrome_options.add_argument('--disable-dev-shm-usage') 
        chrome_options.add_argument('--disable-gpu')   
        chrome_options.add_argument('--window-size=1920,1080') 

        # <------------- NEW LINES ADDED for Rectifying Error -------------------->

        chrome_options.add_argument('--disable-software-rasterizer') # Stops CPU rendering
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-background-networking')
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-client-side-phishing-detection')
        chrome_options.add_argument('--disable-default-apps')
        chrome_options.add_argument('--disable-hang-monitor')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-prompt-on-repost')
        chrome_options.add_argument('--disable-sync')
        chrome_options.add_argument('--metrics-recording-only')
        chrome_options.add_argument('--no-first-run')
        chrome_options.add_argument('--safebrowsing-disable-auto-update')

        # Limit the page load strategy so it doesn't wait for heavy assets
        chrome_options.page_load_strategy = 'eager'

        # <------------- NEW LINES ADDED for Rectifying Error -------------------->

        # Initialize the driver natively
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Navigate to the login page
        driver.get("https://api.icicidirect.com/apiuser/login?api_key=" + urllib.parse.quote_plus(cred.api_key))

        # Find the username and password input fields using their HTML attributes
        driver.find_element(By.XPATH, '//*[@id="txtuid"]').send_keys(cred.username)
        time.sleep(0.2)
        driver.find_element(By.XPATH, '//*[@id="txtPass"]').send_keys(cred.password)
        time.sleep(0.2)
        driver.find_element(By.XPATH, '//*[@id="chkssTnc"]').click()
        time.sleep(0.2)
        driver.find_element(By.XPATH, '//*[@id="btnSubmit"]').click()
        time.sleep(0.2)

        # Entering totp
        otp = pyotp.TOTP(cred.totp_key).now()

        for position, digit in zip(range(1, 7), otp):
            driver.find_element(By.XPATH, f'//*[@id="pnlOTP"]/div[2]/div[2]/div[3]/div/div[{position}]/input').send_keys(digit)
            time.sleep(0.1)

        driver.find_element(By.XPATH, '//*[@id="Button1"]').click()
        time.sleep(0.5)
        
        # Getting session key
        newurl = driver.current_url
        time.sleep(0.3)
        session_key = newurl[newurl.index('=') + 1:]
        
        logger.info(f"Key Generated for {cred.username}")
        return session_key

    except Exception as e:
        logger.error(f"Autologin failed for {cred.username}: {e}", exc_info=True)
        return None
    # finally:
    #     if 'driver' in locals():
    #         driver.quit()

    # <------------- NEW LINES ADDED  Rectifying Error -------------------->

    finally:
        # 1. Kill the Chromium Process to free RAM
        if 'driver' in locals():
            try:
                driver.quit()
                logger.info(f"Browser closed for {cred.username}")
            except Exception as e:
                logger.error(f"Error closing browser: {e}")
        
        # 2. Delete the temporary Chrome Profile to free Disk Space
        profile_path = f"/tmp/chrome_profile_{cred.username}"
        if os.path.exists(profile_path):
            try:
                shutil.rmtree(profile_path, ignore_errors=True)
                logger.info(f"Cleaned up temporary profile at {profile_path}")
            except Exception as e:
                logger.error(f"Failed to delete profile folder: {e}")
                
        # 3. Force Python to run Garbage Collection
        gc.collect()
    
    # <------------- NEW LINES ADDED For Rectifying Error -------------------->

def run_auto_login():
    # ==========================================
    # 2) FIX CSV VISIBILITY IN REPORTS
    # ==========================================
    csv_filename = "autologin_report.csv"
    
    # Save precisely where the Reports page expects strategy files
    csv_file = os.path.join(settings.csv_folder_path, csv_filename)
    if not os.path.exists(settings.csv_folder_path):
        os.makedirs(settings.csv_folder_path, exist_ok=True)
    
    with Session(engine) as session:
        # Guarantee a database link so the Reports module indexes it
        strategy = session.exec(select(Strategy).where(Strategy.file_name == csv_filename)).first()
        if not strategy:
            strategy = Strategy(
                name="Autologin Status Report",
                type="System",
                category="System",
                description="Daily Autologin tokens generator tracking",
                start_hour=0, start_minute=0, end_hour=23, end_minute=59,
                expiry="", fut_expiry="",
                file_name=csv_filename
            )
            session.add(strategy)
            session.commit()
            session.refresh(strategy)
        
        # Auto-link this report to all existing users so they can see it on the frontend
        users = session.exec(select(User)).all()
        for u in users:
            us_link = session.exec(select(UserStrategy).where(UserStrategy.user_id == u.id, UserStrategy.strategy_id == strategy.id)).first()
            if not us_link:
                session.add(UserStrategy(user_id=u.id, strategy_id=strategy.id))
        session.commit()

        # ==========================================
        # 3) REFINE TOKEN MANAGEMENT / SCHEDULER LOGIC
        # ==========================================
        today_str = datetime.now().strftime("%Y-%m-%d")
        user_tokens = {}
        
        # Read file but ONLY keep successful tokens from TODAY (b. Overwrite, Don't Append)
        if os.path.exists(csv_file):
            with open(csv_file, mode='r') as file:
                reader = csv.reader(file)
                headers = next(reader, None)
                for row in reader:
                    if len(row) >= 3:
                        u, t, dt_str = row[0], row[1], row[2]
                        if dt_str.startswith(today_str):
                            user_tokens[u] = (t, dt_str)

        credentials = session.exec(select(BreezeCredential)).all()
        
        for cred in credentials:
            # a. Daily Limit: Skip if token is already registered successfully today
            if cred.username in user_tokens:
                logger.info(f"Token already successfully generated today for {cred.username}. Skipping.")
                continue
            
            logger.info(f"Starting session token generation for: {cred.username}")
            session_token = get_session_key(cred)
            
            if session_token:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                user_tokens[cred.username] = (session_token, current_time)
                logger.info(f"Session token saved for {cred.username}")
            
        # Overwrite file entirely instead of appending, leaving only fresh tokens
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Strategy Name", "Session Token", "Date and Time"])
            for u, (t, dt_str) in user_tokens.items():
                writer.writerow([u, t, dt_str])