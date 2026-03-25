from fastapi import FastAPI, HTTPException, Depends, Form
import Configuration_Details_Scripts.Utils as utils
import uvicorn
import shutil
import os
import sys
import json
import mysql.connector
import logging
import time
from Configuration_Details_Scripts.Logger import setup_file_logger, setup_db_logging, setup_audit_logging, log_audit_event
from Configuration_Details_Scripts.Utils import sendmail
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime
from typing import Optional
from Data_Cleaning.Silver_Cleaner import SilverCleaner
from Data_Cleaning.Config.Cleaning_Config import CLEANING_CONFIG



import configparser
config = configparser.ConfigParser() 
config.read("Configuration_Details_Scripts/Config.ini") 

# Initialize loggers
file_logger = setup_file_logger()  # Initialize file logger
db_logger = setup_db_logging()   # Initialize DB logger
audit_logger = setup_audit_logging()

app = FastAPI()


# database connection
url = utils.url
host = utils.host
user = utils.user
password = utils.password
database = utils.database
security = HTTPBearer()

# Get the security token from config
security_token = config.get("security", "token")

# Helper function to validate token
async def validate_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != security_token:
        raise HTTPException(status_code=403, detail="Invalid or missing token")

def _get_latest_timestamp_folder(base_path: str) -> Optional[str]:
    """Finds the latest timestamped folder in a given base path."""
    try:
        subfolders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
        # Assuming timestamp format is "YYYY-MM-DD_HH-MM-SS" which sorts lexicographically
        subfolders.sort(reverse=True)
        return subfolders[0] if subfolders else None
    except Exception as e:
        file_logger.error(f"Error getting latest timestamp folder from {base_path}: {e}")
        return None

db_logger.info("New ETL Iteration Initiated")
file_logger.info("------------------------- New ETL Iteration -----------------------\n")
sendmail("Collection_Intelligence_System ETL pipeline has started", "Stage: Initialization - Pipeline Execution Initiated")
# ------------------------------------------------------------------------------------------------------------------------------------------
# API Endpoint to process all tasks (fetch data, validate data, transform data)
# ------------------------------------------------------------------------------------------------------------------------------------------
# API Endpoints for Medallion Architecture

@app.post("/Data_Fetching")
async def Data_Fetching(
    credentials: HTTPAuthorizationCredentials = Depends(validate_token),
    load_type: str = Form(..., description="Load type: 'full' or 'incremental'"),
    run_timestamp: Optional[str] = Form(None, description="Optional timestamp (yyyy-mm-dd_HH-MM-SS). If not provided, it will be generated.")
):
    try:
        if load_type not in ["full", "incremental"]:
            raise HTTPException(status_code=400, detail="Invalid load type. Use 'full' or 'incremental'.")

        if not run_timestamp or run_timestamp == "string":
            run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        else:
            # Validate format
            try:
                datetime.strptime(run_timestamp, "%Y-%m-%d_%H-%M-%S")
            except ValueError:
                # If invalid format, log warning and generate new one, or raise error. 
                # For robustness, let's generate a new one but log it.
                db_logger.warning(f"Invalid timestamp format provided: {run_timestamp}. Generating new one.")
                run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        db_logger.info(f"Starting Bronze Layer Ingestion for timestamp: {run_timestamp} with load_type: {load_type}")
        
        # Initialize Bronze Loader
        from Data_Ingestion.Bronze_Loader import BronzeLoader
        loader = BronzeLoader()
        
        # Execute Ingestion for All Configured Entities
        results = loader.Run_All(run_timestamp, load_type)
        
        # Log Summary
        file_logger.info(f"Bronze Ingestion Summary: {json.dumps(results, indent=2)}")
        
        # check for any failures
        failed_entities = [k for k, v in results.items() if "Failed" in v]
        if failed_entities:
            db_logger.error(f"Bronze Layer Ingestion Finished with Errors: {failed_entities}")
            # Optional: Decide if you want to raise 500 or return 200 with error details
            # return {"status": "Partial Success", "run_timestamp": run_timestamp, "failures": failed_entities}

        return {"status": "Success", "message": "Bronze Layer Ingestion Completed", "run_timestamp": run_timestamp, "details": results}

    except Exception as e:
        db_logger.error(f"Bronze Layer Failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/Data_Cleaning")
async def Data_Cleaning(
    credentials: HTTPAuthorizationCredentials = Depends(validate_token),
    run_timestamp: Optional[str] = Form(None, description="Optional timestamp. If not provided, next folder will be processed.")
):
    try:
        if run_timestamp == "string":
            run_timestamp = None
        cleaner = SilverCleaner()
        results = {}
        
        # Iterate over all entities defined in Cleaning Config
        for entity_name in CLEANING_CONFIG.keys():
            try:
                success = cleaner.Clean_Entity(entity_name, run_timestamp)
                results[entity_name] = "Success" if success else "Failed/Skipped"
            except Exception as e:
                results[entity_name] = f"Error: {str(e)}"
                db_logger.error(f"Cleaning failed for {entity_name}: {e}")

        # Check for failures
        failed_entities = [k for k, v in results.items() if "Error" in v or "Failed" in v]
        if failed_entities:
            return {"status": "Partial Success", "details": results, "failures": failed_entities}

        return {"status": "Success", "message": "Silver Layer Cleaning Completed", "details": results}

    except Exception as e:
        db_logger.error(f"Silver Layer Cleaning Failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/Data_Loading")
async def Data_Loading(
    credentials: HTTPAuthorizationCredentials = Depends(validate_token),
    run_timestamp: Optional[str] = Form(None, description="Optional timestamp of the Silver run to load.")
):
    try:
        if run_timestamp is None:
            # Note: Gold Spark load reads from the cumulative Valid Silver directories
            # defined in individual entity configs (e.g., Silver/User/Valid).
            file_logger.info("Triggering Gold Layer load for all valid Silver data.")
            run_timestamp = "CUMULATIVE_LOAD"

        # Execute Spark Orchestrator as a separate process for isolation and environment handling
        import subprocess
        
        # Determining the python executable (using the same one running this API)
        python_exe = sys.executable
        
        # Command: python -m Data_Loading.Main_Gold_Spark
        command = [python_exe, "-m", "Data_Loading.Main_Gold_Spark"]
        
        file_logger.info(f"Triggering Gold Layer Spark Load: {' '.join(command)}")
        
        result = subprocess.run(command, capture_output=True, text=True, cwd=os.getcwd())
        
        if result.returncode != 0:
            error_msg = f"Spark Job Failed. Return Code: {result.returncode}\nStderr: {result.stderr}\nStdout: {result.stdout}"
            file_logger.error(error_msg)
            raise Exception(error_msg)
            
        file_logger.info(f"Gold Layer Spark Load Success. Stdout: {result.stdout}")
        
        return {"status": "Success", "message": "Gold Layer Loading Completed (Spark)", "processed_timestamp": run_timestamp, "details": result.stdout}
    except Exception as e:
        db_logger.error(f"Gold Layer Failed for timestamp {run_timestamp}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/Run_E2E_Pipeline")
async def Run_E2E_Pipeline(
    credentials: HTTPAuthorizationCredentials = Depends(validate_token),
    load_type: str = Form("incremental", description="Load type: 'full' or 'incremental'"),
):
    """
    Orchestrates the entire Medallion Architecture flow:
    Bronze (Fetching) -> Silver (Cleaning) -> Gold (Loading)
    """
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    db_logger.info(f"Starting End-to-End Pipeline Execution. Timestamp: {run_timestamp}")
    file_logger.info(f"--- STARTING E2E PIPELINE: {run_timestamp} ---")
    
    try:
        # 1. Data Fetching (Bronze)
        db_logger.info(f"E2E Step 1/3: Data Fetching ({load_type})")
        fetching_result = await Data_Fetching(credentials=credentials, load_type=load_type, run_timestamp=run_timestamp)
        if fetching_result.get("status") != "Success":
             raise Exception(f"E2E Pipeline failed at Data Fetching: {fetching_result.get('details')}")

        # 2. Data Cleaning (Silver)
        db_logger.info("E2E Step 2/3: Data Cleaning")
        cleaning_result = await Data_Cleaning(credentials=credentials, run_timestamp=run_timestamp)
        if cleaning_result.get("status") != "Success":
             # Note: Silver returns Partial Success if some entities fail. We stop E2E on any failure.
             raise Exception(f"E2E Pipeline failed at Data Cleaning. Details: {cleaning_result}")

        # 3. Data Loading (Gold)
        db_logger.info("E2E Step 3/3: Data Loading")
        loading_result = await Data_Loading(credentials=credentials, run_timestamp=run_timestamp)
        # Data_Loading doesn't return a status dict in the same way, it raises Exception if failed, otherwise returns 200 message.
        
        db_logger.info(f"End-to-End Pipeline Completed Successfully for timestamp: {run_timestamp}")
        file_logger.info(f"--- E2E PIPELINE SUCCESS: {run_timestamp} ---")
        
        sendmail(f"E2E Pipeline Success - {run_timestamp}", "The end-to-end data pipeline completed successfully across all layers (Bronze, Silver, Gold).")
        
        return {
            "status": "Success",
            "message": "End-to-End Pipeline Completed Successfully",
            "timestamp": run_timestamp,
            "summary": {
                "fetching": fetching_result.get("status"),
                "cleaning": cleaning_result.get("status"),
                "loading": "Success"
            }
        }

    except Exception as e:
        error_msg = f"End-to-End Pipeline Failed: {str(e)}"
        db_logger.error(error_msg)
        file_logger.error(f"--- E2E PIPELINE FAILED: {run_timestamp} ---\n{error_msg}")
        
        # Send alert email
        sendmail(f"CRITICAL: E2E Pipeline Failed - {run_timestamp}", error_msg)
        
        raise HTTPException(status_code=500, detail=error_msg)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)