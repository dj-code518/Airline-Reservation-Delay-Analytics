
import os
import logging
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np
import psycopg2
import snowflake.connector
from sqlalchemy import create_engine
import requests
from tenacity import retry, stop_after_attempt, wait_exponential


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AirlineETL")



class Config:
    # Source Systems
    BTS_API_URL    = "https://api.bts.gov/airline-data"
    FAA_API_URL    = "https://soa.smext.faa.gov/asws/api/airport/delays"
    RAW_DATA_PATH  = Path("data/raw")
    STAGE_PATH     = Path("data/stage")
    ARCHIVE_PATH   = Path("data/archive")

    # Snowflake
    SF_ACCOUNT     = os.getenv("SF_ACCOUNT",  "xy12345.us-east-1")
    SF_USER        = os.getenv("SF_USER",     "etl_svc")
    SF_PASSWORD    = os.getenv("SF_PASSWORD", "")
    SF_ROLE        = "ETL_ROLE"
    SF_WAREHOUSE   = "ETL_WH"
    SF_DATABASE    = "AIRLINE_DW"
    SF_SCHEMA      = "PUBLIC"

    # ETL Params
    BATCH_SIZE     = 50_000
    LOOKBACK_DAYS  = 3   # Re-process last N days for late arrivals



class Extractor:

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cfg.RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
    def extract_bts_flights(self, flight_date: str) -> pd.DataFrame:
        """Extract BTS On-Time Performance data via API."""
        logger.info(f"Extracting BTS flights for {flight_date}")
        params = {"date": flight_date, "fields": "all", "limit": 100_000}
        try:
            resp = requests.get(self.cfg.BTS_API_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            df = pd.DataFrame(data["records"])
            logger.info(f"  → {len(df):,} records extracted from BTS API")
            return df
        except requests.RequestException as e:
            logger.error(f"BTS API error: {e}")
            return self._load_csv_fallback(flight_date)

    def _load_csv_fallback(self, flight_date: str) -> pd.DataFrame:
        """Load from local BTS CSV files (monthly bulk downloads)."""
        pattern = f"bts_otd_{flight_date[:7].replace('-','_')}*.csv"
        files = list(self.cfg.RAW_DATA_PATH.glob(pattern))
        if not files:
            raise FileNotFoundError(f"No BTS CSV for {flight_date}")
        df = pd.concat([pd.read_csv(f, low_memory=False) for f in files])
        # Filter to specific date
        df = df[df["FlightDate"] == flight_date]
        logger.info(f"  → {len(df):,} records from CSV fallback")
        return df

    def extract_faa_delays(self, airport_codes: list[str]) -> pd.DataFrame:
        """Extract real-time FAA delay info."""
        records = []
        for code in airport_codes:
            try:
                resp = requests.get(
                    f"{self.cfg.FAA_API_URL}/{code}", timeout=15
                )
                if resp.ok:
                    records.append({"airport": code, **resp.json()})
            except Exception as e:
                logger.warning(f"FAA delay fetch failed for {code}: {e}")
        return pd.DataFrame(records)

    def extract_weather(self, stations: list[str], date: str) -> pd.DataFrame:
        """Extract NOAA weather data for airport stations."""
        logger.info(f"Extracting weather for {len(stations)} stations")
        # NOAA Climate Data Online API
        headers = {"token": os.getenv("NOAA_API_TOKEN", "")}
        params = {
            "datasetid": "GHCND",
            "stationid": ",".join(stations),
            "startdate": date,
            "enddate": date,
            "datatypeid": "AWND,PRCP,SNOW,TMAX,TMIN,WDF2",
            "limit": 1000,
        }
        resp = requests.get(
            "https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
            headers=headers, params=params, timeout=30
        )
        return pd.DataFrame(resp.json().get("results", []))


class Transformer:

    BTS_COLUMN_MAP = {
        "FlightDate":          "flight_date",
        "Carrier":             "airline_code",
        "TailNum":             "tail_number",
        "FlightNum":           "flight_number",
        "OriginAirportID":     "origin_iata",
        "DestAirportID":       "dest_iata",
        "CRSDepTime":          "sched_dep_time",
        "DepTime":             "actual_dep_time",
        "DepDelay":            "dep_delay_min",
        "CRSArrTime":          "sched_arr_time",
        "ArrTime":             "actual_arr_time",
        "ArrDelay":            "arr_delay_min",
        "Cancelled":           "is_cancelled",
        "CancellationCode":    "cancellation_code",
        "Diverted":            "is_diverted",
        "CRSElapsedTime":      "elapsed_time_min",
        "Distance":            "distance_miles",
        "CarrierDelay":        "carrier_delay_min",
        "WeatherDelay":        "weather_delay_min",
        "NASDelay":            "nas_delay_min",
        "SecurityDelay":       "security_delay_min",
        "LateAircraftDelay":   "late_aircraft_delay",
    }

    def transform_flights(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Transforming {len(df):,} flight records")

        # 1. Rename columns
        df = df.rename(columns=self.BTS_COLUMN_MAP)
        df = df[[c for c in self.BTS_COLUMN_MAP.values() if c in df.columns]]

        # 2. Parse dates & times
        df["flight_date"]      = pd.to_datetime(df["flight_date"], format="%Y-%m-%d")
        df["date_key"]         = df["flight_date"].dt.strftime("%Y%m%d").astype(int)
        df["sched_dep_time"]   = self._parse_hhmm(df["sched_dep_time"])
        df["sched_arr_time"]   = self._parse_hhmm(df["sched_arr_time"])
        df["actual_dep_time"]  = self._parse_hhmm(df["actual_dep_time"])
        df["actual_arr_time"]  = self._parse_hhmm(df["actual_arr_time"])

        # 3. Numeric coercions
        delay_cols = [
            "dep_delay_min", "arr_delay_min", "carrier_delay_min",
            "weather_delay_min", "nas_delay_min", "security_delay_min",
            "late_aircraft_delay", "elapsed_time_min", "distance_miles",
        ]
        for col in delay_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        # 4. Boolean flags
        df["is_cancelled"] = df["is_cancelled"].astype(float).fillna(0).astype(bool)
        df["is_diverted"]  = df["is_diverted"].astype(float).fillna(0).astype(bool)

        # 5. Data quality checks
        df = self._apply_dq_rules(df)

        # 6. Add audit columns
        df["etl_load_ts"]   = datetime.utcnow()
        df["etl_source"]    = "BTS_OTP"
        df["record_hash"]   = df.apply(
            lambda r: hashlib.md5(
                f"{r.get('flight_number')}{r.get('flight_date')}{r.get('tail_number')}".encode()
            ).hexdigest(), axis=1
        )

        logger.info(f"  → {len(df):,} records after transformation")
        return df

    def _parse_hhmm(self, series: pd.Series) -> pd.Series:
        """Convert HHMM integer (e.g. 1430) → time string '14:30'."""
        def _fmt(v):
            try:
                v = int(float(v))
                if v < 0 or v > 2400:
                    return None
                return f"{v // 100:02d}:{v % 100:02d}"
            except (ValueError, TypeError):
                return None
        return series.apply(_fmt)

    def _apply_dq_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data quality validation rules."""
        initial = len(df)
        issues = {}

        # Rule 1: Drop records without flight identifiers
        mask = df["flight_number"].notna() & df["airline_code"].notna()
        issues["missing_flight_id"] = (~mask).sum()
        df = df[mask]

        # Rule 2: Delay cannot exceed 1440 min (24 hours)
        for col in ["dep_delay_min", "arr_delay_min"]:
            if col in df.columns:
                bad = df[col].abs() > 1440
                issues[f"{col}_out_of_range"] = bad.sum()
                df.loc[bad, col] = np.nan

        # Rule 3: Cancelled flights should not have actual times
        cancelled = df["is_cancelled"]
        df.loc[cancelled, ["actual_dep_time", "actual_arr_time"]] = None

        logger.info(f"  DQ Issues: {issues}")
        logger.info(f"  DQ: {initial - len(df):,} records dropped")
        return df

    def build_date_dimension(self, start: str, end: str) -> pd.DataFrame:
        """Generate dim_date rows for a date range."""
        dates = pd.date_range(start=start, end=end, freq="D")
        df = pd.DataFrame({"full_date": dates})
        df["date_key"]    = df["full_date"].dt.strftime("%Y%m%d").astype(int)
        df["day_of_week"] = df["full_date"].dt.dayofweek + 1  # 1=Mon
        df["day_name"]    = df["full_date"].dt.day_name()
        df["day_of_month"]= df["full_date"].dt.day
        df["month_num"]   = df["full_date"].dt.month
        df["month_name"]  = df["full_date"].dt.month_name()
        df["quarter"]     = df["full_date"].dt.quarter
        df["year"]        = df["full_date"].dt.year
        df["is_weekend"]  = df["day_of_week"].isin([6, 7])
        df["season"]      = df["month_num"].map({
            12:"Winter", 1:"Winter", 2:"Winter",
            3:"Spring",  4:"Spring", 5:"Spring",
            6:"Summer",  7:"Summer", 8:"Summer",
            9:"Fall",    10:"Fall",  11:"Fall"
        })
        return df


class SnowflakeLoader:

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    def _get_conn(self):
        if self._conn is None or self._conn.is_closed():
            self._conn = snowflake.connector.connect(
                account=self.cfg.SF_ACCOUNT,
                user=self.cfg.SF_USER,
                password=self.cfg.SF_PASSWORD,
                role=self.cfg.SF_ROLE,
                warehouse=self.cfg.SF_WAREHOUSE,
                database=self.cfg.SF_DATABASE,
                schema=self.cfg.SF_SCHEMA,
                session_parameters={"QUERY_TAG": "ETL_PIPELINE"},
            )
        return self._conn

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "append",
        merge_keys: Optional[list] = None,
    ) -> int:
        """Load a DataFrame into Snowflake via internal stage."""
        conn = self._get_conn()
        stage_file = self.cfg.STAGE_PATH / f"{table_name}_{datetime.now():%Y%m%d_%H%M%S}.parquet"
        self.cfg.STAGE_PATH.mkdir(parents=True, exist_ok=True)

        # Write to Parquet for efficient staging
        df.to_parquet(stage_file, index=False, compression="snappy")
        logger.info(f"Staged {len(df):,} rows → {stage_file.name}")

        try:
            cur = conn.cursor()
            # PUT file to Snowflake internal stage
            cur.execute(f"PUT file://{stage_file.absolute()} @%{table_name} OVERWRITE=TRUE")
            # COPY INTO table
            cur.execute(f"""
                COPY INTO {table_name}
                FROM @%{table_name}/{stage_file.name}
                FILE_FORMAT = (TYPE='PARQUET' SNAPPY_COMPRESSION=TRUE)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = TRUE
            """)
            rows_loaded = cur.fetchone()[3] if cur.rowcount else 0
            logger.info(f"  ✓ Loaded {rows_loaded:,} rows into {table_name}")
            return rows_loaded

        finally:
            if stage_file.exists():
                stage_file.unlink()

    def upsert_dimension(
        self,
        df: pd.DataFrame,
        target_table: str,
        merge_key: str,
        scd_type: int = 1,
    ):
        """SCD Type 1 or Type 2 dimension upsert."""
        conn = self._get_conn()
        temp_table = f"TEMP_{target_table}_{datetime.now():%H%M%S}"

        # Load staging table
        self.load_dataframe(df, temp_table, mode="replace")

        if scd_type == 1:
            # Simple overwrite of changed attributes
            conn.cursor().execute(f"""
                MERGE INTO {target_table} tgt
                USING {temp_table} src ON tgt.{merge_key} = src.{merge_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        elif scd_type == 2:
            # Close old record, insert new record
            conn.cursor().execute(f"""
                MERGE INTO {target_table} tgt
                USING {temp_table} src ON tgt.{merge_key} = src.{merge_key}
                    AND tgt.is_current = TRUE
                WHEN MATCHED AND (tgt.checksum != src.checksum) THEN
                    UPDATE SET tgt.expiry_date = CURRENT_DATE, tgt.is_current = FALSE
                WHEN NOT MATCHED THEN
                    INSERT * VALUES (src.*, CURRENT_DATE, '9999-12-31', TRUE)
            """)

        conn.cursor().execute(f"DROP TABLE IF EXISTS {temp_table}")
        logger.info(f"  ✓ SCD{scd_type} upsert complete on {target_table}")

    def close(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()


class AirlineETLPipeline:

    def __init__(self):
        self.cfg         = Config()
        self.extractor   = Extractor(self.cfg)
        self.transformer = Transformer()
        self.loader      = SnowflakeLoader(self.cfg)

    def run_daily(self, process_date: Optional[str] = None):
        """Full daily ETL run."""
        process_date = process_date or (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        logger.info(f"{'='*60}")
        logger.info(f"Starting Daily ETL Run | Date: {process_date}")
        logger.info(f"{'='*60}")

        start_time = datetime.now()
        run_meta = {"status": "running", "process_date": process_date}

        try:
            # ── EXTRACT ──────────────
            flights_raw = self.extractor.extract_bts_flights(process_date)

            # ── TRANSFORM ────────────
            flights_clean = self.transformer.transform_flights(flights_raw)

            # ── LOAD ─────────────────
            rows = self.loader.load_dataframe(flights_clean, "FACT_FLIGHT", mode="append")

            # ── POST-LOAD REFRESH ────
            self._refresh_aggregates()

            elapsed = (datetime.now() - start_time).total_seconds()
            run_meta.update({"status": "success", "rows_loaded": rows, "elapsed_sec": elapsed})
            logger.info(f"✓ ETL Complete | {rows:,} rows | {elapsed:.1f}s")

        except Exception as e:
            run_meta.update({"status": "failed", "error": str(e)})
            logger.error(f"✗ ETL Failed: {e}", exc_info=True)
            raise

        finally:
            self._log_run(run_meta)
            self.loader.close()

    def _refresh_aggregates(self):
        """Refresh materialized views / dynamic tables."""
        conn = self.loader._get_conn()
        views = ["MV_DAILY_AIRLINE_PERFORMANCE", "MV_ROUTE_SUMMARY"]
        for v in views:
            try:
                conn.cursor().execute(f"ALTER DYNAMIC TABLE {v} REFRESH")
                logger.info(f"  ↻ Refreshed {v}")
            except Exception as e:
                logger.warning(f"  ⚠ Could not refresh {v}: {e}")

    def _log_run(self, meta: dict):
        try:
            conn = self.loader._get_conn()
            conn.cursor().execute("""
                INSERT INTO ETL_RUN_LOG (run_ts, process_date, status, rows_loaded, elapsed_sec, error_msg)
                VALUES (CURRENT_TIMESTAMP, %(process_date)s, %(status)s,
                        %(rows_loaded)s, %(elapsed_sec)s, %(error)s)
            """, {
                "process_date": meta.get("process_date"),
                "status":       meta.get("status"),
                "rows_loaded":  meta.get("rows_loaded", 0),
                "elapsed_sec":  meta.get("elapsed_sec", 0),
                "error":        meta.get("error"),
            })
        except Exception:
            pass  # Don't fail the pipeline on logging errors



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Airline ETL Pipeline")
    parser.add_argument("--date", help="Process date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                        help="Backfill date range")
    args = parser.parse_args()

    pipeline = AirlineETLPipeline()

    if args.backfill:
        dates = pd.date_range(args.backfill[0], args.backfill[1])
        for d in dates:
            pipeline.run_daily(d.strftime("%Y-%m-%d"))
    else:
        pipeline.run_daily(args.date)
