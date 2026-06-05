"""
my_System_revised_fixed.py
─────────────────────────────────────────────────────────────────────────────
Smart Port Recommendation Engine — MariSight Gold Layer  (v2, fixed)
Triggered daily by Airflow after silver_vessels + gold_ports refresh.

Fixes applied vs. my_System_revised.py
────────────────────────────────────────
  1. distance_score() moved ABOVE recommend() — was referenced before definition.
  2. DESTINATION_PORT_LAT/LON renamed → DESTINATION_PORT_LAT/DESTINATION_PORT_LON
     to match actual GOLD_VESSELS column names (inherited from SILVER_VESSELS).
  3. Duplicate `write_pandas` import removed.

Dependencies
────────────
  pip install "snowflake-connector-python[pandas]" pandas numpy python-dotenv

Environment variables (or Airflow connections)
──────────────────────────────────────────────
  SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT
  SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
"""

import logging
import os
from datetime import date
from math import atan2, cos, exp, radians, sin, sqrt

import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

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

TOP_N                 = 3
SEISMIC_HARD_CUTOFF   = 90   # ports with 14-day max risk score >= this are excluded
SEISMIC_LOOKBACK_DAYS = 14

# Score weights — must sum to 1.0
# AFTER
W_DISTANCE       = 0.00
W_SUPPLY         = 0.30
W_INFRASTRUCTURE = 0.25
W_SEISMIC        = 0.30
W_COMMUNICATION  = 0.15


# W_DISTANCE       = 0.25
# W_SUPPLY         = 0.25
# W_INFRASTRUCTURE = 0.20
# W_SEISMIC        = 0.25
# W_COMMUNICATION  = 0.05

ACTIVE_STATUSES = ("Under way", "Sailing")


# ── SQL Queries ───────────────────────────────────────────────────────────────

# FIX #2: DESTINATION_PORT_LAT / DESTINATION_PORT_LON — correct column names from GOLD_VESSELS
Q_VESSELS = """
SELECT
    NAME,
    TYPE,
    COALESCE(LENGTH_M, 0) AS LENGTH_M,
    COALESCE(BEAM_M,   0) AS BEAM_M,
    REPORTED_STATUS,
    DESTINATION_PORT_NAME,
    DESTINATION_PORT_COUNTRY,
    DESTINATION_PORT_LAT,
    DESTINATION_PORT_LON,
    REPORT_DATE
FROM PROJECT_DB.GOLD.GOLD_VESSELS
WHERE REPORTED_STATUS IN ('Under way', 'Sailing')
  AND NAME IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NAME
    ORDER BY REPORT_DATE DESC
) = 1
"""

Q_PORTS = """
SELECT
    p.WORLD_PORT_INDEX_NUMBER,
    p.MAIN_PORT_NAME,
    p.COUNTRY_CODE,
    p.REGION_NAME,
    p.WORLD_WATER_BODY,
    p.HARBOR_SIZE,
    p.PORT_DEPTH_CLASS,
    p.LATITUDE,
    p.LONGITUDE,
    COALESCE(p.MAXIMUM_VESSEL_LENGTH_M, 9999) AS MAXIMUM_VESSEL_LENGTH_M,
    COALESCE(p.MAXIMUM_VESSEL_BEAM_M,    999) AS MAXIMUM_VESSEL_BEAM_M,
    COALESCE(p.SUPPLY_SCORE,               0) AS SUPPLY_SCORE,
    COALESCE(p.COMMUNICATION_SCORE,        0) AS COMMUNICATION_SCORE,
    p.SHELTER_AFFORDED,
    p.PORT_SECURITY,
    COALESCE(f.HAS_CONTAINER_FACILITY,    0) AS HAS_CONTAINER_FACILITY,
    COALESCE(f.HAS_RO_RO_FACILITY,        0) AS HAS_RO_RO_FACILITY,
    COALESCE(f.HAS_LIQUID_BULK_FACILITY,  0) AS HAS_LIQUID_BULK_FACILITY,
    COALESCE(f.HAS_SOLID_BULK_FACILITY,   0) AS HAS_SOLID_BULK_FACILITY,
    COALESCE(f.HAS_OIL_TERMINAL_FACILITY, 0) AS HAS_OIL_TERMINAL_FACILITY,
    f.PORT_PRIMARY_TYPE
FROM PROJECT_DB.GOLD.GOLD_PORTS p
LEFT JOIN PROJECT_DB.GOLD.GOLD_PORT_INFRASTRUCTURE_FACILITIES f
    ON p.WORLD_PORT_INDEX_NUMBER = f.WORLD_PORT_INDEX_NUMBER
WHERE p.IS_VALID_COORDINATES = TRUE
"""

Q_SEISMIC = f"""
SELECT
    p.PORT_NAME       AS MAIN_PORT_NAME,
    MAX(p.RISK_SCORE) AS MAX_RISK_SCORE
FROM PROJECT_DB.GOLD.GOLD_SEISMIC_PORT_PROXIMITY p
JOIN PROJECT_DB.GOLD.GOLD_SEISMIC_EVENTS         e
    ON p.EVENT_ID = e.EVENT_ID
WHERE e.EARTHQUAKE_TIME >= DATEADD(DAY, -{SEISMIC_LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
GROUP BY p.PORT_NAME
"""


# ── Helper functions ──────────────────────────────────────────────────────────

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
    """Base 50 + shelter quality (max 30) + port security (20) → capped at 100."""
    shelter_bonus = (
        df["SHELTER_AFFORDED"]
        .astype(str).str.strip().str.lower()
        .map({"excellent": 30, "good": 20, "fair": 10})
        .fillna(0)
    )
    security_bonus = _yn(df["PORT_SECURITY"]) * 20
    return (50 + shelter_bonus + security_bonus).clip(upper=100)


def depth_score(df: pd.DataFrame) -> pd.Series:
    """Convert PORT_DEPTH_CLASS → numeric score (0-100). Unknown → 50 (neutral)."""
    return (
        df["PORT_DEPTH_CLASS"]
        .map({"Deep": 100, "Medium": 65, "Shallow": 30})
        .fillna(50)
    )


def harbor_size_score(series: pd.Series) -> pd.Series:
    return (
        series.astype(str).str.strip()
        .map({"Large": 100, "Medium": 70, "Small": 40, "Very Small": 20, "Unknown": 50})
        .fillna(50)
    )


def infrastructure_score(df: pd.DataFrame) -> pd.Series:
    """Depth (60%) + harbor size (40%) composite."""
    return (
        depth_score(df) * 0.60
        + harbor_size_score(df["HARBOR_SIZE"]) * 0.40
    )


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points on Earth (km)."""
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


# FIX #1: distance_score() defined HERE — before recommend() calls it.
def distance_score(distance_km: pd.Series) -> pd.Series:
    """
    Continuous exponential decay score (0-100).
    0 km   → 100  |  250 km → ~78  |  500 km → ~61  |  1000 km → ~37
    Decay constant = 2000 km (gentler than the original 1000 km constant,
    so mid-range alternatives aren't penalised too harshly).
    """
    return 100 * distance_km.apply(lambda d: exp(-d / 2000))


# Vessel-type keyword → required pre-computed binary facility column
FACILITY_REQUIREMENTS: dict[str, str] = {
    "container":   "HAS_CONTAINER_FACILITY",
    "tanker":      "HAS_OIL_TERMINAL_FACILITY",
    "oil":         "HAS_OIL_TERMINAL_FACILITY",
    "bulk":        "HAS_SOLID_BULK_FACILITY",
    "liquid bulk": "HAS_LIQUID_BULK_FACILITY",
    "ro-ro":       "HAS_RO_RO_FACILITY",
    "ro ro":       "HAS_RO_RO_FACILITY",
    "car carrier": "HAS_RO_RO_FACILITY",
    "vehicle":     "HAS_RO_RO_FACILITY",
}


def required_facility(vessel_type: str) -> str | None:
    """Return the GOLD_PORTS column that must equal 1, or None (no restriction)."""
    vt = str(vessel_type).lower()
    for keyword, col in FACILITY_REQUIREMENTS.items():
        if keyword in vt:
            return col
    return None


# ── Core pipeline ─────────────────────────────────────────────────────────────

def load_data(cur) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    def fetch(q: str, label: str) -> pd.DataFrame:
        log.info("Fetching %s …", label)
        cur.execute(q)
        df = cur.fetch_pandas_all()
        log.info("  → %d rows", len(df))
        return df

    vessels = fetch(Q_VESSELS, "active vessels")
    ports   = fetch(Q_PORTS,   "ports")
    seismic = fetch(Q_SEISMIC, "seismic risk (14d)")
    return vessels, ports, seismic


def prepare_ports(ports: pd.DataFrame, seismic: pd.DataFrame) -> pd.DataFrame:
    """Merge seismic risk, hard-exclude dangerous ports, pre-compute reusable scores."""
    # ports = ports.merge(seismic, on="MAIN_PORT_NAME", how="left")
  
    # AFTER
    seismic["MAIN_PORT_NAME"] = seismic["MAIN_PORT_NAME"].str.strip().str.upper()
    ports["_port_name_key"]   = ports["MAIN_PORT_NAME"].str.strip().str.upper()
    ports = ports.merge(
        seismic.rename(columns={"MAIN_PORT_NAME": "_port_name_key"}),
        on="_port_name_key",
        how="left",
    ).drop(columns=["_port_name_key"])
    

    ports["MAX_RISK_SCORE"] = ports["MAX_RISK_SCORE"].fillna(0.0)

    before = len(ports)
    ports  = ports[ports["MAX_RISK_SCORE"] < SEISMIC_HARD_CUTOFF].copy()
    log.info(
        "Seismic hard cutoff (>= %.0f): excluded %d ports, %d remain",
        SEISMIC_HARD_CUTOFF, before - len(ports), len(ports),
    )

    # Pre-compute static scores once (vectorised — avoids repeating per vessel)
    ports["_safety_score"]         = safety_score(ports)
    ports["_depth_score"]          = depth_score(ports)
    ports["_infrastructure_score"] = infrastructure_score(ports)
    ports["_seismic_safety_score"] = (100 - ports["MAX_RISK_SCORE"]).clip(lower=0)

    return ports


def recommend(vessels: pd.DataFrame, ports: pd.DataFrame) -> pd.DataFrame:
    """For each active vessel, filter + rank ports and return top-N rows."""
    run_date = date.today()
    rows = []

    for _, v in vessels.drop_duplicates(subset=["NAME"]).iterrows():
        v_name   = str(v["NAME"])
        v_type   = str(v["TYPE"])
        v_length = float(v["LENGTH_M"])
        v_beam   = float(v["BEAM_M"])

        # Step 1 — Dimension filter
        candidates = ports[
            (ports["MAXIMUM_VESSEL_LENGTH_M"] >= v_length) &
            (ports["MAXIMUM_VESSEL_BEAM_M"]   >= v_beam)
        ]

        # Step 2 — Facility filter
        fac_col = required_facility(v_type)
        if fac_col and fac_col in candidates.columns:
            candidates = candidates[candidates[fac_col] == 1]

        if candidates.empty:
            log.warning("No qualifying ports for %s (%s) — skipping", v_name, v_type)
            continue

        candidates = candidates.copy()

        # Step 3 — Distance scoring (FIX #2: correct column names)
        dest_lat = v["DESTINATION_PORT_LAT"]
        dest_lon = v["DESTINATION_PORT_LON"]

        if pd.notna(dest_lat) and pd.notna(dest_lon):
            candidates["_distance_km"] = candidates.apply(
                lambda p: haversine_km(
                    float(dest_lat), float(dest_lon),
                    float(p["LATITUDE"]), float(p["LONGITUDE"]),
                ),
                axis=1,
            )
            candidates["_distance_score"] = distance_score(candidates["_distance_km"])
            has_dest_coords = True
        else:
            candidates["_distance_km"]    = np.nan
            candidates["_distance_score"] = np.nan

        # Step 4 — Final composite score
        # AFTER
        candidates["_final_score"] = (
            candidates["SUPPLY_SCORE"]            * W_SUPPLY
            + candidates["_infrastructure_score"] * W_INFRASTRUCTURE
            + candidates["_seismic_safety_score"] * W_SEISMIC
            + candidates["COMMUNICATION_SCORE"]   * W_COMMUNICATION
        )


        # candidates["_final_score"] = (
        #     candidates["_distance_score"]       * W_DISTANCE
        #     + candidates["SUPPLY_SCORE"]        * W_SUPPLY
        #     + candidates["_infrastructure_score"] * W_INFRASTRUCTURE
        #     + candidates["_seismic_safety_score"] * W_SEISMIC
        #     + candidates["COMMUNICATION_SCORE"] * W_COMMUNICATION
        # )

        # Step 5 — Rank & select top N
        top = candidates.nlargest(TOP_N, "_final_score").reset_index(drop=True)

        for rank, (_, p) in enumerate(top.iterrows(), start=1):
            rows.append({
                "RUN_DATE":                run_date,
                "VESSEL_NAME":             v_name,
                "VESSEL_TYPE":             v_type,
                "VESSEL_LENGTH_M":         v_length,
                "VESSEL_BEAM_M":           v_beam,
                "REPORTED_STATUS":         str(v["REPORTED_STATUS"]),
                "RANK":                    rank,
                "RECOMMENDED_PORT_NAME":   str(p["MAIN_PORT_NAME"]),
                "COUNTRY_CODE":            str(p["COUNTRY_CODE"]),
                "REGION_NAME":             str(p["REGION_NAME"]),
                "WORLD_PORT_INDEX_NUMBER": p["WORLD_PORT_INDEX_NUMBER"],
                "HARBOR_SIZE":             p["HARBOR_SIZE"],
                "PORT_DEPTH_CLASS":        p["PORT_DEPTH_CLASS"],
                "LATITUDE":                float(p["LATITUDE"]),
                "LONGITUDE":               float(p["LONGITUDE"]),
                "FINAL_SCORE":             round(float(p["_final_score"]), 2),
                "DISTANCE_KM":             (
                    None if pd.isna(p["_distance_km"])
                    else round(float(p["_distance_km"]), 2)
                ),
                "DISTANCE_SCORE":          round(float(p["_distance_score"]), 2),
                "INFRASTRUCTURE_SCORE":    round(float(p["_infrastructure_score"]), 2),
                "SEISMIC_SAFETY_SCORE":    round(float(p["_seismic_safety_score"]), 2),
                "SUPPLY_SCORE":            round(float(p["SUPPLY_SCORE"]), 2),
                "COMMUNICATION_SCORE":     round(float(p["COMMUNICATION_SCORE"]), 2),
                "SAFETY_SCORE":            round(float(p["_safety_score"]), 2),
                "DEPTH_SCORE":             round(float(p["_depth_score"]), 2),
                "SEISMIC_RISK_SCORE":      round(float(p["MAX_RISK_SCORE"]), 2),
            })

    if not rows:
        log.warning("No recommendations produced.")
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out.columns = [c.upper() for c in out.columns]
    log.info(
        "Produced %d recommendation rows (%d vessels × up to %d ports)",
        len(out), out["VESSEL_NAME"].nunique(), TOP_N,
    )
    return out


def write_results(conn, cur, df: pd.DataFrame, run_date: date) -> None:
    """DELETE today's existing rows, then INSERT fresh batch."""
    cur.execute(
        f"DELETE FROM PROJECT_DB.GOLD.GOLD_PORT_RECOMMENDATIONS_V2 "
        f"WHERE RUN_DATE = '{run_date}'"
    )
    log.info("Deleted existing rows for %s", run_date)

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="GOLD_PORT_RECOMMENDATIONS_V2",
        database="PROJECT_DB",
        schema="GOLD",
        overwrite=True,
    )
    if not success:
        raise RuntimeError("write_pandas reported failure — check Snowflake logs.")
    log.info("Inserted %d rows in %d chunk(s)", nrows, nchunks)


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

        ports   = prepare_ports(ports, seismic)
        results = recommend(vessels, ports)

        if results.empty:
            log.info("Empty output — no writes performed.")
            return

        write_results(conn, cur, results, run_date)

    finally:
        cur.close()
        conn.close()

    log.info("=== Done ===")


if __name__ == "__main__":
    main()