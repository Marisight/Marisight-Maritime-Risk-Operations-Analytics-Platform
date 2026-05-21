import os, io, sys, time, logging
import pandas as pd
from datetime import datetime

from vessel_scraper import VesselFinderScraper, OUTPUT_COLUMNS
from data_quality   import validate
from base_scraper   import BlockedException 

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPE_DELAY = float(os.getenv("SCRAPE_DELAY", "4.0"))
MAX_WORKERS  = int(os.getenv("MAX_WORKERS",  "2")) 
TEST_MODE    = os.getenv("TEST_MODE",  "false").lower() == "true"
TEST_LIMIT   = int(os.getenv("TEST_LIMIT",  "5"))
S3_BUCKET    = os.getenv("S3_BUCKET",  "marisight-bronze")
S3_PREFIX    = os.getenv("S3_PREFIX",  "vessels")
AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")

logger = logging.getLogger() 
def _setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def _is_lambda() -> bool:
    return bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))

def _save(df: pd.DataFrame, s3_key: str, local_path: str):
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[OUTPUT_COLUMNS]

    if _is_lambda():
        import boto3
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        boto3.client("s3", region_name=AWS_REGION).put_object(
            Bucket=S3_BUCKET, Key=s3_key,
            Body=buf.getvalue(), ContentType="text/csv",
        )
        logger.info(f"Uploaded → s3://{S3_BUCKET}/{s3_key}")
    else:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        df.to_csv(local_path, index=False, encoding="utf-8")
        logger.info(f"Saved → {local_path}")

def _run():
    t0  = time.time()
    now = datetime.now()
    ds, yr, mo = now.strftime("%Y-%m-%d"), now.strftime("%Y"), now.strftime("%m")
    fname = f"vessels_{ds}.csv"

    logger.info(f"Starting Pipeline | Mode: {'TEST' if TEST_MODE else 'PROD'} | Workers: {MAX_WORKERS}")

    scraper = VesselFinderScraper(
        delay=SCRAPE_DELAY,
        test_mode=TEST_MODE,
        test_limit=TEST_LIMIT,
        max_workers=MAX_WORKERS,
    )
    vessels = scraper.collect()

    if not vessels:
        logger.error("No vessels collected")
        return {"statusCode": 500, "body": "No vessels"}

    df = pd.DataFrame(vessels)
    
    # ── Validate ────────────────────────────────────────────────────────
    ok, msg = validate(df)

    good_s3 = f"{S3_PREFIX}/year={yr}/month={mo}/{fname}"
    quar_s3 = f"quarantine/vessels/year={yr}/month={mo}/failed_{fname}"

    if ok:
        _save(df, s3_key=good_s3, local_path=f"output/{good_s3}")
        return {"statusCode": 200, "body": f"Success: {len(df)} vessels"}
    else:
        logger.error(f"Validation failed: {msg}")
        _save(df, s3_key=quar_s3, local_path=f"output/{quar_s3}")
        return {"statusCode": 500, "body": f"Validation failed: {msg}"}

# ── Lambda Entry ──────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    _setup_logging()
    try:
        return _run()
    except BlockedException as e:
        logger.error(f"EXECUTION ABORTED (BLOCKED): {e}")
        return {"statusCode": 429, "body": "Blocked by target site"}
    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}")
        raise e 

if __name__ == "__main__":
    _setup_logging()
    result = _run()
    print(f"Final result: {result}")