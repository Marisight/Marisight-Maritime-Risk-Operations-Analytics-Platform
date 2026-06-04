"""
gold_port_recommendations.py
─────────────────────────────────────────────────────────────────────────────
Smart Port Recommendation Engine — MariSight Gold Layer
Triggered daily by Airflow after silver_vessels refresh.

Logic
─────
1. Pull active vessels (Under way / Sailing) from GOLD_VESSELS
2. Pull scored port candidates from GOLD_PORTS
3. Pull seismic risk (last 7 days) from GOLD_SEISMIC_PORT_PROXIMITY
4. Hard-exclude ports with MAX_RISK_SCORE >= SEISMIC_HARD_CUTOFF
5. Filter by vessel dimensions (length, beam)
6. Filter by vessel-type facility requirement (container, tanker, etc.)
7. Score & rank remaining ports per vessel → top N
8. DELETE today's rows, INSERT fresh results → GOLD.GOLD_PORT_RECOMMENDATIONS

Dependencies
────────────
  pip install "snowflake-connector-python[pandas]" pandas numpy

Environment variables (or Airflow connections)
──────────────────────────────────────────────
  SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT
  SNOWFLAKE_WAREHOUSE   
  SNOWFLAKE_ROLE        
"""

import logging
import os
from datetime import date

import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────
SF_CONN = dict(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    database="PROJECT_DB",
    schema="GOLD",
    role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
)

TARGET_DB     = "PROJECT_DB"
TARGET_SCHEMA = "GOLD"
TARGET_TABLE  = "GOLD_PORT_RECOMMENDATIONS"

TOP_N               = 3
SEISMIC_HARD_CUTOFF = 70    # ports with 7-day max risk score >= this are excluded
SEISMIC_LOOKBACK_DAYS = 7

# Score weights — must sum to 1.0
W_SUPPLY        = 0.30
W_COMMUNICATION = 0.20
W_SAFETY        = 0.25
W_DEPTH         = 0.25

ACTIVE_STATUSES = ("Under way", "Sailing")


# ── SQL Queries ───────────────────────────────────────────────────────────────
# One row per vessel (latest report), active status only
Q_VESSELS = """
SELECT
    NAME,
    TYPE,
    COALESCE(LENGTH_M, 0)  AS LENGTH_M,
    COALESCE(BEAM_M,   0)  AS BEAM_M,
    REPORTED_STATUS,
    DESTINATION_PORT_NAME,
    DESTINATION_PORT_COUNTRY,
    REPORT_DATE
FROM PROJECT_DB.GOLD.GOLD_VESSELS
WHERE REPORTED_STATUS IN ('Under way', 'Sailing')
  AND NAME IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY REPORT_DATE DESC) = 1
"""

# Port candidates — valid coordinates only, pre-scored columns already exist
Q_PORTS = """
SELECT
    WORLD_PORT_INDEX_NUMBER,
    MAIN_PORT_NAME,
    COUNTRY_CODE,
    REGION_NAME,
    WORLD_WATER_BODY,
    HARBOR_SIZE,
    PORT_DEPTH_CLASS,
    LATITUDE,
    LONGITUDE,
    COALESCE(MAXIMUM_VESSEL_LENGTH_M, 9999) AS MAXIMUM_VESSEL_LENGTH_M,
    COALESCE(MAXIMUM_VESSEL_BEAM_M,    999) AS MAXIMUM_VESSEL_BEAM_M,
    COALESCE(SUPPLY_SCORE,               0) AS SUPPLY_SCORE,
    COALESCE(COMMUNICATION_SCORE,        0) AS COMMUNICATION_SCORE,
    SHELTER_AFFORDED,
    PORT_SECURITY,
    FACILITIES_CONTAINER,
    FACILITIES_RO_RO,
    FACILITIES_LIQUID_BULK,
    FACILITIES_SOLID_BULK,
    FACILITIES_OIL_TERMINAL
FROM PROJECT_DB.GOLD.GOLD_PORTS
WHERE IS_VALID_COORDINATES = TRUE
"""

# Seismic risk aggregated per port, last N days
# Joins GOLD_SEISMIC_EVENTS for the timestamp (GOLD_SEISMIC_PORT_PROXIMITY has no date)
Q_SEISMIC = f"""
SELECT
    p.PORT_NAME                       AS MAIN_PORT_NAME,
    MAX(p.RISK_SCORE)                 AS MAX_RISK_SCORE,
    COUNT(p.EVENT_ID)                 AS SEISMIC_EVENT_COUNT_7D
FROM PROJECT_DB.GOLD.GOLD_SEISMIC_PORT_PROXIMITY p
JOIN PROJECT_DB.GOLD.GOLD_SEISMIC_EVENTS         e ON p.EVENT_ID = e.EVENT_ID
WHERE e.EARTHQUAKE_TIME >= DATEADD(DAY, -{SEISMIC_LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
GROUP BY p.PORT_NAME
"""


# ── Helpers ───────────────────────────────────────────────────────────────────
def _yn(series: pd.Series) -> pd.Series:
    """Normalise Yes / No / NaN strings → 1 / 0."""
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map({"yes": 1, "y": 1, "true": 1})
        .fillna(0)
        .astype(int)
    )


def safety_score(df: pd.DataFrame) -> pd.Series:
    """
    Base 50 + shelter quality (max 30) + port security (20) → capped at 100.
    Mirrors the prototype logic; computed here because it is not in GOLD_PORTS.
    """
    shelter_bonus = (
        df["SHELTER_AFFORDED"]
        .astype(str)
        .str.strip()
        .str.lower()
        .map({"excellent": 30, "good": 20, "fair": 10})
        .fillna(0)
    )
    security_bonus = _yn(df["PORT_SECURITY"]) * 20
    return (50 + shelter_bonus + security_bonus).clip(upper=100)


def depth_score(df: pd.DataFrame) -> pd.Series:
    """
    Convert PORT_DEPTH_CLASS to a numeric score (0-100).
    Adjust the map below if your gold model uses different labels.
    Unrecognised values fall back to 50 (neutral).
    """
    return (
        df["PORT_DEPTH_CLASS"]
        .map({"Deep": 100, "Medium": 65, "Shallow": 30})
        .fillna(50)
    )


# Vessel-type keyword → required port facility column
FACILITY_REQUIREMENTS: dict[str, str] = {
    "container":   "FACILITIES_CONTAINER",
    "tanker":      "FACILITIES_OIL_TERMINAL",
    "oil":         "FACILITIES_OIL_TERMINAL",
    "bulk":        "FACILITIES_SOLID_BULK",
    "liquid bulk": "FACILITIES_LIQUID_BULK",
    "ro-ro":       "FACILITIES_RO_RO",
    "ro ro":       "FACILITIES_RO_RO",
    "car carrier": "FACILITIES_RO_RO",
    "vehicle":     "FACILITIES_RO_RO",
}


def required_facility(vessel_type: str) -> str | None:
    """Return the GOLD_PORTS column that must equal 'Yes', or None (no restriction)."""
    vt = str(vessel_type).lower()
    for keyword, col in FACILITY_REQUIREMENTS.items():
        if keyword in vt:
            return col
    return None


# ── Core ──────────────────────────────────────────────────────────────────────
def load_data(cur) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    def fetch(q: str, label: str) -> pd.DataFrame:
        log.info("Fetching %s …", label)
        cur.execute(q)
        df = cur.fetch_pandas_all()
        log.info("  → %d rows", len(df))
        return df

    vessels = fetch(Q_VESSELS,  "active vessels")
    ports   = fetch(Q_PORTS,    "ports")
    seismic = fetch(Q_SEISMIC,  "seismic risk (7d)")
    return vessels, ports, seismic


def prepare_ports(
    ports: pd.DataFrame,
    seismic: pd.DataFrame,
) -> pd.DataFrame:
    """Merge seismic risk, hard-exclude dangerous ports, pre-compute scores."""
    # Merge seismic (left join → ports with no seismic activity get risk = 0)
    ports = ports.merge(seismic, on="MAIN_PORT_NAME", how="left")
    ports["MAX_RISK_SCORE"]         = ports["MAX_RISK_SCORE"].fillna(0.0)
    ports["SEISMIC_EVENT_COUNT_7D"] = ports["SEISMIC_EVENT_COUNT_7D"].fillna(0).astype(int)

    # Hard exclusion
    before = len(ports)
    ports = ports[ports["MAX_RISK_SCORE"] < SEISMIC_HARD_CUTOFF].copy()
    log.info(
        "Seismic hard cutoff (>= %.0f): excluded %d ports, %d remain",
        SEISMIC_HARD_CUTOFF,
        before - len(ports),
        len(ports),
    )

    # Pre-compute scores once (vectorised — avoids repeating this per vessel)
    ports["_safety_score"] = safety_score(ports)
    ports["_depth_score"]  = depth_score(ports)
    ports["_seismic_pen"]  = (ports["MAX_RISK_SCORE"] / SEISMIC_HARD_CUTOFF) * 30

    ports["_composite"] = (
        ports["SUPPLY_SCORE"]      * W_SUPPLY
        + ports["COMMUNICATION_SCORE"] * W_COMMUNICATION
        + ports["_safety_score"]   * W_SAFETY
        + ports["_depth_score"]    * W_DEPTH
    ) - ports["_seismic_pen"]

    return ports


def recommend(vessels: pd.DataFrame, ports: pd.DataFrame) -> pd.DataFrame:
    """
    For each active vessel, filter + rank ports and return top-N rows.
    The loop is over vessels (~200 rows); all port scoring is vectorised.
    """
    run_date = date.today()
    rows = []

    # Deduplicate vessels by name (should already be 1-per-vessel from SQL)
    for _, v in vessels.drop_duplicates(subset=["NAME"]).iterrows():
        v_name   = str(v["NAME"])
        v_type   = str(v["TYPE"])
        v_length = float(v["LENGTH_M"])
        v_beam   = float(v["BEAM_M"])

        # ── Step 1: dimension filter ──────────────────────────────────────────
        candidates = ports[
            (ports["MAXIMUM_VESSEL_LENGTH_M"] >= v_length) &
            (ports["MAXIMUM_VESSEL_BEAM_M"]   >= v_beam)
        ]

        # ── Step 2: facility filter ───────────────────────────────────────────
        fac_col = required_facility(v_type)
        if fac_col and fac_col in candidates.columns:
            candidates = candidates[_yn(candidates[fac_col]) == 1]

        if candidates.empty:
            log.warning("No qualifying ports for %s (%s) — skipping", v_name, v_type)
            continue

        # ── Step 3: rank & select top N ───────────────────────────────────────
        top = candidates.nlargest(TOP_N, "_composite").reset_index(drop=True)

        for rank, (_, p) in enumerate(top.iterrows(), start=1):
            rows.append(
                {
                    "RUN_DATE":                 run_date,
                    "VESSEL_NAME":              v_name,
                    "VESSEL_TYPE":              v_type,
                    "VESSEL_LENGTH_M":          v_length,
                    "VESSEL_BEAM_M":            v_beam,
                    "REPORTED_STATUS":          str(v["REPORTED_STATUS"]),
                    "RANK":                     rank,
                    "RECOMMENDED_PORT_NAME":    str(p["MAIN_PORT_NAME"]),
                    "COUNTRY_CODE":             str(p["COUNTRY_CODE"]),
                    "REGION_NAME":              str(p["REGION_NAME"]),
                    "WORLD_PORT_INDEX_NUMBER":  p["WORLD_PORT_INDEX_NUMBER"],
                    "HARBOR_SIZE":              p["HARBOR_SIZE"],
                    "PORT_DEPTH_CLASS":         p["PORT_DEPTH_CLASS"],
                    "LATITUDE":                 float(p["LATITUDE"]),
                    "LONGITUDE":                float(p["LONGITUDE"]),
                    "COMPOSITE_SCORE":          round(float(p["_composite"]),      2),
                    "SUPPLY_SCORE":             round(float(p["SUPPLY_SCORE"]),    2),
                    "COMMUNICATION_SCORE":      round(float(p["COMMUNICATION_SCORE"]), 2),
                    "SAFETY_SCORE":             round(float(p["_safety_score"]),   2),
                    "DEPTH_SCORE":              round(float(p["_depth_score"]),    2),
                    "SEISMIC_RISK_SCORE":       round(float(p["MAX_RISK_SCORE"]), 2),
                    "SEISMIC_PENALTY":          round(float(p["_seismic_pen"]),    2),
                    "SEISMIC_EVENT_COUNT_7D":   int(p["SEISMIC_EVENT_COUNT_7D"]),
                }
            )

    if not rows:
        log.warning("No recommendations produced.")
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out.columns = [c.upper() for c in out.columns]   # Snowflake expects uppercase
    log.info(
        "Produced %d recommendation rows (%d vessels × up to %d ports)",
        len(out),
        out["VESSEL_NAME"].nunique(),
        TOP_N,
    )
    return out


def write_results(conn, cur, df: pd.DataFrame, run_date: date) -> None:
    """DELETE today's existing rows, then INSERT fresh batch."""
    delete_sql = (
        f"DELETE FROM {TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE} "
        f"WHERE RUN_DATE = '{run_date}'"
    )
    cur.execute(delete_sql)
    log.info("Deleted existing rows for %s", run_date)

    success, n_chunks, n_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=TARGET_TABLE,
        database=TARGET_DB,
        schema=TARGET_SCHEMA,
        overwrite=False,
    )
    log.info(
        "Wrote %d rows in %d chunk(s) — success=%s",
        n_rows, n_chunks, success,
    )
    if not success:
        raise RuntimeError("write_pandas reported failure — check Snowflake logs.")


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    run_date = date.today()
    log.info("=== Port Recommendation Run: %s ===", run_date)

    conn = snowflake.connector.connect(**SF_CONN)
    cur  = conn.cursor()

    try:
        vessels, ports, seismic = load_data(cur)

        if vessels.empty:
            log.info("No active vessels found. Nothing to recommend.")
            return

        ports = prepare_ports(ports, seismic)
        out   = recommend(vessels, ports)

        if out.empty:
            log.info("Empty output — no writes performed.")
            return

        write_results(conn, cur, out, run_date)

    finally:
        cur.close()
        conn.close()

    log.info("=== Done ===")


if __name__ == "__main__":
    main()