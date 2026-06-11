import asyncio
import websockets
import json
import pandas as pd
import os
import httpx
import logging
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.ERROR)

WS_URL = "wss://www.seismicportal.eu/standing_order/websocket"
API_URL = "https://www.seismicportal.eu/fdsnws/event/1/query?format=json"

DB_USER = "alaa"
DB_PASS = "1234"
DB_NAME = "seismic_data"
DB_HOST = "postgres_db"
DB_PORT = "5432"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

COLUMNS_ORDER = [
    "source_id", "source_catalog", "lastupdate", "time", 
    "flynn_region", "lat", "lon", "depth", "evtype", 
    "auth", "mag", "magtype", "unid", "action"
]

def update_database(action, unid, new_row):
    try:
        with engine.begin() as conn:
            table_exists = conn.execute(
                text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'seismic_events')")
            ).scalar()

            if not table_exists:
                new_row[COLUMNS_ORDER].to_sql(
                    "seismic_events", engine, if_exists="append", index=False
                )
                print(f"[DATABASE] Table 'seismic_events' created.")
                return

            if action in ["update", "delete"]:
                conn.execute(
                    text("DELETE FROM seismic_events WHERE unid = :unid"),
                    {"unid": unid},
                )

            if action in ["create", "update"]:
                check = conn.execute(
                    text("SELECT COUNT(*) FROM seismic_events WHERE unid = :unid"),
                    {"unid": unid},
                ).scalar()
                if check == 0:
                    new_row[COLUMNS_ORDER].to_sql(
                        "seismic_events", engine, if_exists="append", index=False
                    )
    except Exception as e:
        print(f"[DATABASE ERROR] {e}")

async def fetch_backfill():
    try:
        with engine.connect() as conn:
            last_time = conn.execute(text("SELECT MAX(time) FROM seismic_events")).scalar()
            
        if last_time:
            start_time = pd.to_datetime(last_time).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            start_time = (datetime.now(timezone.utc) - pd.Timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
    except:
        start_time = (datetime.now(timezone.utc) - pd.Timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")

    print(f"[BACKFILL] Checking missed events since {start_time}...")

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.get(f"{API_URL}&starttime={start_time}")
            if response.status_code == 200:
                events = response.json().get("features", [])
                for f in events:
                    props = f["properties"]
                    event = {
                        "unid": f["id"],
                        "action": "create",
                        **{k: props.get(k) for k in COLUMNS_ORDER if k not in ["unid", "action"]},
                    }
                    update_database("create", event["unid"], pd.DataFrame([event]))
                print(f"[BACKFILL] Recovered {len(events)} events.")
        except Exception as e:
            print(f"[BACKFILL] Error: {e}")

async def start_stream():
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                async for message in ws:
                    data = json.loads(message)
                    event_data = data.get("data", {})
                    if event_data:
                        props = event_data.get("properties", {})
                        action = data.get("action", "create")
                        unid = event_data.get("id")
                        event = {
                            "unid": unid,
                            "action": action,
                            **{k: props.get(k) for k in COLUMNS_ORDER if k not in ["unid", "action"]},
                        }
                        update_database(action.lower(), unid, pd.DataFrame([event]))
                        print(f"[STREAM] {action.upper()} processed for {unid}")
        except Exception as e:
            print(f"[STREAM] Error: {e}. Reconnecting in 10s")
            await asyncio.sleep(10)

async def main():
    print("Starting Seismic Ingestion System")
    try:
        with engine.connect() as conn:
            print("[SYSTEM] Successfully connected to Postgres.")
    except Exception as e:
        print(f"[SYSTEM ERROR] Could not connect to Postgres: {e}")
        return 

    await fetch_backfill()
    print("[SYSTEM] Switching to Live Stream mode.")
    await start_stream()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Stopped.")