import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "name", "type", "year_built", "gross_tonnage", "deadweight",
    "length(m)", "beam(m)", "detail_link",
    "departure_date", "last_port_country", "last_port_name",
    "arrival_date", "destination_port_country", "destination_port_name",
    "destination_port_lat", "destination_port_lon",
    "reported_status", "report_date",
]

MIN_VESSELS = 50  


def validate(df: pd.DataFrame) -> tuple[bool, str]:
    logger.info("── Data Quality Check ──────────────────────────")

    # Fatal: 
    if df.empty:
        msg = "FATAL: DataFrame empty — scraping returned nothing"
        logger.error(msg)
        _sns(" Pipeline Failed", msg)
        return False, msg

    # Fatal: NULL
    if "name" in df.columns and df["name"].isnull().all():
        msg = "FATAL: All vessel names NULL — HTML structure changed"
        logger.error(msg)
        _sns(" Scraper Broken", msg)
        return False, msg

    # Fatal: Schema drift
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        msg = f"FATAL: Schema drift — missing columns: {missing}"
        logger.error(msg)
        _sns(" Schema Drift", msg)
        return False, msg

    if len(df) < MIN_VESSELS:
        w = f"Only {len(df)} vessels (expected {MIN_VESSELS}+) — possible blocking or site change"
        logger.warning(w)
        _sns(" Low Vessel Count", w)

    for col in ["name", "type", "destination_port_name", "arrival_date", "reported_status"]:
        if col in df.columns:
            n = df[col].isnull().sum()
            if n:
                logger.warning(f"  {col}: {n}/{len(df)} missing ({n/len(df)*100:.0f}%)")

    logger.info(f"Quality check passed  {len(df)} vessels | {len(df.columns)} columns")
    return True, "OK"


def _sns(subject: str, body: str):
    arn = os.getenv("SNS_TOPIC_ARN", "")
    if not arn:
        return
    try:
        import boto3
        boto3.client("sns", region_name=os.getenv("AWS_REGION", "us-east-1")).publish(
            TopicArn=arn, Subject=subject, Message=body
        )
    except Exception as e:
        logger.error(f"SNS failed: {e}")