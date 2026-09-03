"""
AdMob → BigQuery  (PRODUCTION v6)
==========================================================
⚠️  YE FILE CHAARO REPOS MEIN BILKUL EK JAISI LAGTI HAI.
    Publisher ka farq sirf ADMOB_PUBLISHER_ID env var se aata hai:
        pub-5972202469838280   pub-5036550218341905
        pub-4905254875899379   pub-9688592152492531

VERSION HISTORY:
  v3 FINAL: Original (no publisher_id, wiped other accounts' data)
  v4:       publisher_id everywhere · publisher-scoped DELETEs · auto schema
            migration · DELETE retry + schema verification · hard fail on
            missing publisher_id
  v5 (2026-07-26) — FACT TABLE DATA-LOSS GUARDS:
            🔴 sync_one_day mein DELETE ab SAB SE AAKHIR mein hai.
            🛡️ GUARD 1: koi batch 403 → us din ko haath hi mat lagao
            🛡️ GUARD 2: teeno report khali → bhi haath mat lagao

  v6 (2026-09-03) — DIMENSION DATA-LOSS FIX:
            🔴 BUG 1: v5 ka fix SIRF fact table pe laga tha. sync_dims mein
               `delete_dim_for_publisher(...)` abhi bhi fetch se PEHLE thi.
               Run #214 mein adUnits pe 401 aaya — DELETE ho chuki thi, load
               kabhi nahi hua. pub-5972202469838280 ka poora
               admob_ad_units_dim UD GAYA.
               → Ab: PEHLE saara fetch, PHIR staging + atomic swap
                 (BEGIN/COMMIT TRANSACTION). Bilkul Mintegral loader jaisa.

            🔴 BUG 2: with_retry 401 pe turant `raise` karta tha
               (`status < 500 and status != 429`). Ek transient auth blip =
               table gaya. → Ab 401 pe credentials refresh karke retry.

            🔴 BUG 3: sync_dims try/finally se BAHAR tha, is liye dimension
               failure kabhi admob_sync_log mein nahi likhi jaati thi.
               Run #214 ki koi row hi nahi hai.
               → Ab dono sync() aur backfill() mein try ke ANDAR.

            🔴 BUG 4: _verify_publisher_id_column() False de to DELETE skip
               ho jati thi — magar load_rows phir bhi WRITE_APPEND karta tha
               → duplicate dim rows (joins 2x fan out kar rahe the).
               → Ab dims kabhi append nahi hote, sirf atomic swap.

            🛡️ GUARD 3: fetch mein pehle se maujood rows ka SHRINK_PCT se
               kam aaye to swap se inkaar — partial fetch se wipe nahi hoga.

REQUIRED ENV VARS:
   ADMOB_PUBLISHER_ID    = pub-XXXXXXXXXXXXXXXX  (ya accounts/pub-XXXX)
   GCP_PROJECT_ID        = terafort
   BQ_DATASET_ID         = Admob (default)
   BQ_LOCATION           = US (default)
   OAUTH_CLIENT_ID       = <OAuth client for this AdMob account>
   OAUTH_CLIENT_SECRET   = <OAuth secret>
   OAUTH_REFRESH_TOKEN   = <Refresh token for THIS publisher>
   GCP_CREDENTIALS_JSON  = <Service account JSON>

OPTIONAL ENV VARS:
   DIM_SHRINK_PCT        = 0.5   (refuse dim swap if new rows < 50% of old)

USAGE:
   python admob_full_sync.py --days 3
   python admob_full_sync.py --backfill-start 2026-07-19 --backfill-end 2026-07-26

   # v6.1 — sab kuch isi EK file mein. Koi alag helper script nahi.
   python admob_full_sync.py --plan-chunks     # backfill ko chunks mein tode
   python admob_full_sync.py --dim-health      # dimension tables khaali to nahi?

DATA MODEL:
   Money fields: INT64 MICROS — divide by 1,000,000 for USD
   Partition  : report_date (DAY)
   Cluster    : data_source, app_id, country_code, ad_format

   ⚠️  admob_unified_fact mein TEEN grain ek saath hain (data_source):
         admob_network         → AdMob ki apni demand (SUBSET)
         admob_network_adtype  → wahi data, ad_type breakdown (SUBSET, DUPLICATE)
         admob_mediation       → poori mediated revenue (YEHI TOTAL HAI)
       Bina `WHERE data_source = 'admob_mediation'` ke SUM karne se
       revenue double/triple count hoti hai.
"""

import os
import sys
import json
import time
import argparse
import socket
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

socket.setdefaulttimeout(180)

# =============================================================================
# CONFIG
# =============================================================================

PROJECT_ID          = os.environ.get("GCP_PROJECT_ID", "").strip()
DATASET_ID          = os.environ.get("BQ_DATASET_ID", "Admob").strip()
BQ_LOCATION         = os.environ.get("BQ_LOCATION", "US").strip()
ADMOB_PUBLISHER_ID  = os.environ.get("ADMOB_PUBLISHER_ID", "").strip()
ADMOB_CURRENCY      = os.environ.get("ADMOB_REPORT_CURRENCY", "USD").strip()
CLIENT_ID           = os.environ.get("OAUTH_CLIENT_ID", "").strip()
CLIENT_SECRET       = os.environ.get("OAUTH_CLIENT_SECRET", "").strip()
REFRESH_TOKEN       = os.environ.get("OAUTH_REFRESH_TOKEN", "").strip()
BQ_CREDENTIALS_JSON = os.environ.get("GCP_CREDENTIALS_JSON", "").strip()

FACT_TABLE   = "admob_unified_fact"
DIM_ACCOUNT  = "admob_account_dim"
DIM_APPS     = "admob_apps_dim"
DIM_AD_UNITS = "admob_ad_units_dim"
LOG_TABLE    = "admob_sync_log"

MAX_RETRIES    = 4
RETRY_BACKOFF  = 8
ROW_LIMIT_WARN = 90000

# 🛡️ v6 GUARD 3: agar naya fetch purane rows ka is ratio se kam ho → swap mat karo
try:
    DIM_SHRINK_PCT = float(os.environ.get("DIM_SHRINK_PCT", "0.5"))
except ValueError:
    DIM_SHRINK_PCT = 0.5

# App batch sizes per report type — proven safe vs 100K limit
BATCH_NETWORK         = 5
BATCH_NETWORK_ADTYPE  = 3
BATCH_MEDIATION       = 2

# Pause after ALTER TABLE to let BQ metadata cache refresh
SCHEMA_CACHE_WAIT_SEC = 3

# v6: module-level creds handle so with_retry can refresh IN PLACE.
# AuthorizedHttp holds a reference to this same object, so refreshing it
# fixes the already-built discovery client — no rebuild needed.
_CREDS: Optional[Credentials] = None

# =============================================================================
# SCHEMAS — publisher_id added to all tables that need attribution
# =============================================================================

UNIFIED_FACT_SCHEMA = [
    bigquery.SchemaField("report_date",               "DATE"),
    bigquery.SchemaField("data_source",               "STRING"),
    bigquery.SchemaField("run_id",                    "STRING"),
    bigquery.SchemaField("sync_timestamp",            "TIMESTAMP"),
    bigquery.SchemaField("publisher_id",              "STRING"),
    bigquery.SchemaField("app_id",                    "STRING"),
    bigquery.SchemaField("app_name",                  "STRING"),
    bigquery.SchemaField("platform",                  "STRING"),
    bigquery.SchemaField("ad_unit_id",                "STRING"),
    bigquery.SchemaField("ad_unit_name",              "STRING"),
    bigquery.SchemaField("ad_format",                 "STRING"),
    bigquery.SchemaField("ad_type",                   "STRING"),
    bigquery.SchemaField("country_code",              "STRING"),
    bigquery.SchemaField("country_name",              "STRING"),
    bigquery.SchemaField("ad_source_id",              "STRING"),
    bigquery.SchemaField("ad_source_name",            "STRING"),
    bigquery.SchemaField("mediation_group_id",        "STRING"),
    bigquery.SchemaField("mediation_group_name",      "STRING"),
    bigquery.SchemaField("impressions",               "INT64"),
    bigquery.SchemaField("clicks",                    "INT64"),
    bigquery.SchemaField("ctr",                       "FLOAT64"),
    bigquery.SchemaField("estimated_earnings_micros", "INT64"),
    bigquery.SchemaField("ecpm_micros",               "FLOAT64"),
    bigquery.SchemaField("ad_requests",               "INT64"),
    bigquery.SchemaField("matched_requests",          "INT64"),
    bigquery.SchemaField("fill_rate",                 "FLOAT64"),
    bigquery.SchemaField("match_rate",                "FLOAT64"),
    bigquery.SchemaField("show_rate",                 "FLOAT64"),
    bigquery.SchemaField("observed_ecpm_micros",      "FLOAT64"),
]

ACCOUNT_SCHEMA = [
    bigquery.SchemaField("account_resource_name", "STRING"),
    bigquery.SchemaField("publisher_id",          "STRING"),
    bigquery.SchemaField("reporting_time_zone",   "STRING"),
    bigquery.SchemaField("currency_code",         "STRING"),
    bigquery.SchemaField("sync_timestamp",        "TIMESTAMP"),
]

APPS_SCHEMA = [
    bigquery.SchemaField("app_resource_name",   "STRING"),
    bigquery.SchemaField("app_id",              "STRING"),
    bigquery.SchemaField("publisher_id",        "STRING"),
    bigquery.SchemaField("platform",            "STRING"),
    bigquery.SchemaField("manual_display_name", "STRING"),
    bigquery.SchemaField("store_app_id",        "STRING"),
    bigquery.SchemaField("store_display_name",  "STRING"),
    bigquery.SchemaField("app_approval_state",  "STRING"),
    bigquery.SchemaField("sync_timestamp",      "TIMESTAMP"),
]

AD_UNITS_SCHEMA = [
    bigquery.SchemaField("ad_unit_resource_name", "STRING"),
    bigquery.SchemaField("ad_unit_id",            "STRING"),
    bigquery.SchemaField("publisher_id",          "STRING"),
    bigquery.SchemaField("app_id",                "STRING"),
    bigquery.SchemaField("ad_unit_display_name",  "STRING"),
    bigquery.SchemaField("ad_format",             "STRING"),
    bigquery.SchemaField("ad_types",              "STRING", mode="REPEATED"),
    bigquery.SchemaField("sync_timestamp",        "TIMESTAMP"),
]

SYNC_LOG_SCHEMA = [
    bigquery.SchemaField("run_id",              "STRING"),
    bigquery.SchemaField("publisher_id",        "STRING"),
    bigquery.SchemaField("run_type",            "STRING"),
    bigquery.SchemaField("start_date",          "DATE"),
    bigquery.SchemaField("end_date",            "DATE"),
    bigquery.SchemaField("status",              "STRING"),
    bigquery.SchemaField("network_rows",        "INT64"),
    bigquery.SchemaField("network_adtype_rows", "INT64"),
    bigquery.SchemaField("mediation_rows",      "INT64"),
    bigquery.SchemaField("total_rows",          "INT64"),
    bigquery.SchemaField("error_message",       "STRING"),
    bigquery.SchemaField("duration_seconds",    "FLOAT64"),
    bigquery.SchemaField("sync_timestamp",      "TIMESTAMP"),
]

# =============================================================================
# VALIDATION
# =============================================================================

def validate_config() -> bool:
    required = {
        "GCP_PROJECT_ID":       PROJECT_ID,
        "BQ_DATASET_ID":        DATASET_ID,
        "ADMOB_PUBLISHER_ID":   ADMOB_PUBLISHER_ID,
        "OAUTH_CLIENT_ID":      CLIENT_ID,
        "OAUTH_CLIENT_SECRET":  CLIENT_SECRET,
        "OAUTH_REFRESH_TOKEN":  REFRESH_TOKEN,
        "GCP_CREDENTIALS_JSON": BQ_CREDENTIALS_JSON,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"❌ ERROR: Missing env vars: {', '.join(missing)}")
        return False

    pid = normalize_publisher_id(ADMOB_PUBLISHER_ID)
    if not pid.startswith("pub-") or len(pid) < 10:
        print(f"❌ ERROR: ADMOB_PUBLISHER_ID looks invalid: '{pid}'")
        print(f"   Expected format: pub-XXXXXXXXXXXXXXXX")
        return False
    return True

# =============================================================================
# AUTH
# =============================================================================

def get_fresh_credentials() -> Credentials:
    """v6: creds ko module level pe rakhta hai taake with_retry 401 pe
    in-place refresh kar sake."""
    global _CREDS
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[
            "https://www.googleapis.com/auth/admob.readonly",
            "https://www.googleapis.com/auth/admob.report",
        ],
    )
    creds.refresh(Request())
    _CREDS = creds
    print(f"  Token refreshed ✅")
    return creds

def get_v1(creds):
    return build("admob", "v1", credentials=creds, cache_discovery=False)

def get_bq_client() -> bigquery.Client:
    info  = json.loads(BQ_CREDENTIALS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds, location=BQ_LOCATION)

# =============================================================================
# RETRY   🔴 v6: 401 ab retry hota hai (credential refresh ke saath)
# =============================================================================

def with_retry(fn, label="call", auth_retry=True):
    """v6: 401 ("missing required authentication credential") ab fatal nahi.

    v5 mein `if status < 500 and status != 429: raise` tha — 401 seedha
    upar chala jata tha. Run #214 mein wahi hua: adUnits pe 401, aur us se
    pehle DELETE ho chuki thi.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except HttpError as e:
            status = e.resp.status
            if status == 401 and auth_retry and _CREDS is not None and attempt < MAX_RETRIES:
                print(f"  [{label}] 401 received — forcing credential refresh …")
                try:
                    _CREDS.refresh(Request())
                    print(f"  [{label}] credentials refreshed ✅")
                except Exception as refresh_err:
                    print(f"  [{label}] credential refresh FAILED: {refresh_err}")
                last_err = e
            elif status < 500 and status != 429:
                raise
            else:
                last_err = e
        except Exception as e:
            last_err = e
        wait = RETRY_BACKOFF * attempt
        print(f"  [{label}] attempt {attempt}/{MAX_RETRIES} failed — retrying in {wait}s …")
        time.sleep(wait)
    raise last_err

# =============================================================================
# HELPERS
# =============================================================================

def utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()

def run_id_now() -> str:
    return datetime.utcnow().strftime("%Y%m%d%H%M%S")

def to_api_date(d: date) -> Dict[str, int]:
    return {"year": d.year, "month": d.month, "day": d.day}

def dim_val(dims, key):
    return dims.get(key, {}).get("value")

def dim_lbl(dims, key):
    return dims.get(key, {}).get("displayLabel") or dims.get(key, {}).get("value")

def metric_val(m: Optional[Dict]) -> Optional[Any]:
    if not m:
        return None
    for k in ("microsValue", "integerValue"):
        if k in m and m[k] not in (None, ""):
            return int(m[k])
    for k in ("doubleValue", "decimalValue"):
        if k in m and m[k] not in (None, ""):
            return float(m[k])
    if "value" in m and m["value"] not in (None, ""):
        raw = m["value"]
        try:
            return float(raw) if "." in str(raw) else int(raw)
        except Exception:
            return raw
    return None

def parse_date_from_dims(dims) -> Optional[str]:
    raw = dim_val(dims, "DATE")
    if not raw or len(raw) != 8:
        return None
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"

def safe_fill_rate(matched, requests):
    try:
        if requests and int(requests) > 0:
            return round(int(matched) / int(requests), 6)
    except Exception:
        pass
    return None

def safe_ecpm(earnings_micros, impressions):
    try:
        if impressions and int(impressions) > 0:
            return round(int(earnings_micros) / int(impressions) * 1000, 2)
    except Exception:
        pass
    return None

def paginate(callable_, items_key: str, label: str = "paginate") -> List[Dict]:
    results, page_token = [], None
    while True:
        resp = with_retry(
            lambda pt=page_token: callable_(pageToken=pt).execute() if pt else callable_().execute(),
            label=label
        )
        results.extend(resp.get(items_key, []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results

def make_batches(items: List, batch_size: int) -> List[List]:
    return [items[i:i+batch_size] for i in range(0, len(items), batch_size)]

def normalize_publisher_id(raw_or_resource: str) -> str:
    if not raw_or_resource:
        return ""
    return raw_or_resource.replace("accounts/", "").strip()

# =============================================================================
# BIGQUERY OPS
# =============================================================================

def ensure_dataset(bq: bigquery.Client):
    ds_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        bq.get_dataset(ds_id)
        print(f"  Dataset exists: {ds_id}")
    except NotFound:
        ds = bigquery.Dataset(ds_id)
        ds.location = BQ_LOCATION
        bq.create_dataset(ds)
        print(f"  Created dataset: {ds_id}")

def migrate_table_schema(bq: bigquery.Client, table_name: str, expected_schema):
    """Auto-add missing columns to existing table (schema drift across accounts)."""
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    try:
        table = bq.get_table(tid)
    except NotFound:
        return  # Table doesn't exist — will be created fresh

    existing_columns = {f.name for f in table.schema}
    migrations_run = 0

    for field in expected_schema:
        if field.name in existing_columns:
            continue
        if field.mode == "REPEATED" or field.field_type == "RECORD":
            print(f"  ⚠️  Cannot auto-migrate {table_name}.{field.name} "
                  f"({field.field_type}/{field.mode}) — needs manual ALTER")
            continue
        alter_sql = (f"ALTER TABLE `{tid}` "
                     f"ADD COLUMN IF NOT EXISTS {field.name} {field.field_type}")
        try:
            bq.query(alter_sql).result()
            print(f"  Migrated {table_name}: added {field.name} ({field.field_type})")
            migrations_run += 1
        except Exception as e:
            print(f"  ⚠️  Migration failed for {table_name}.{field.name}: {e}")

    if migrations_run > 0:
        print(f"  ⏱️  Waiting {SCHEMA_CACHE_WAIT_SEC}s for {table_name} schema cache …")
        time.sleep(SCHEMA_CACHE_WAIT_SEC)
        bq.get_table(tid)

def ensure_table(bq: bigquery.Client, name: str, schema, is_fact=False, quiet=False):
    tid = f"{PROJECT_ID}.{DATASET_ID}.{name}"
    try:
        bq.get_table(tid)
        if not quiet:
            print(f"  Table exists: {name}")
        migrate_table_schema(bq, name, schema)
    except NotFound:
        t = bigquery.Table(tid, schema=schema)
        if is_fact:
            t.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="report_date"
            )
            t.clustering_fields = ["data_source", "app_id", "country_code", "ad_format"]
        bq.create_table(t)
        print(f"  Created table: {name}")

def load_rows(bq: bigquery.Client, table: str, schema, rows: List[Dict],
              disposition=bigquery.WriteDisposition.WRITE_APPEND,
              quiet=False) -> int:
    if not rows:
        if not quiet:
            print(f"  No rows for {table}")
        return 0
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    cfg = bigquery.LoadJobConfig(schema=schema, write_disposition=disposition)

    def _load():
        job = bq.load_table_from_json(rows, tid, job_config=cfg)
        job.result()
        return len(rows)

    n = with_retry(_load, label=table, auth_retry=False)
    if not quiet:
        print(f"  Loaded {n:,} rows → {table}")
    return n

def count_dim_rows(bq: bigquery.Client, table: str, publisher_id: str) -> int:
    """Existing row count for this publisher. -1 if table/column unavailable."""
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    try:
        job = bq.query(
            f"SELECT COUNT(*) AS n FROM `{tid}` WHERE publisher_id = @pub",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pub", "STRING", publisher_id)
            ])
        )
        return list(job.result())[0]["n"]
    except Exception:
        return -1

def _verify_publisher_id_column(bq: bigquery.Client, table: str) -> bool:
    """Confirm publisher_id column exists (guards BQ metadata cache staleness)."""
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    try:
        table_ref = bq.get_table(tid)
        column_names = {f.name for f in table_ref.schema}
        if 'publisher_id' not in column_names:
            print(f"  ⚠️  {table} missing publisher_id column in cached schema")
            return False
        return True
    except NotFound:
        print(f"  ⚠️  {table} not found")
        return False
    except Exception as e:
        print(f"  ⚠️  Could not verify {table} schema: {e}")
        return True  # Proceed anyway — don't block on transient errors

def delete_range_for_publisher(bq: bigquery.Client, table: str, publisher_id: str,
                               start: date, end: date):
    """Delete ONLY this publisher's fact rows for the range. Never touches others."""
    if not publisher_id:
        raise ValueError("delete_range_for_publisher: publisher_id is required (empty)")

    if not _verify_publisher_id_column(bq, table):
        raise RuntimeError(
            f"{table}: publisher_id column missing — refusing to DELETE. "
            f"v5 mein yahan sirf skip hota tha aur load phir bhi APPEND karta tha "
            f"(duplicate rows). Schema migrate karke dobara chalao."
        )

    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"

    def _do_delete():
        return bq.query(
            f"""
            DELETE FROM `{tid}`
            WHERE report_date BETWEEN @start AND @end
              AND publisher_id = @pub
            """,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", str(start)),
                bigquery.ScalarQueryParameter("end",   "DATE", str(end)),
                bigquery.ScalarQueryParameter("pub",   "STRING", publisher_id),
            ])
        ).result()

    with_retry(_do_delete, label=f"delete_range_{table}", auth_retry=False)
    print(f"  Deleted {table} for {publisher_id}: {start} → {end}")

# =============================================================================
# 🔴 v6 CORE FIX — ATOMIC DIMENSION SWAP
# =============================================================================

def swap_dim_for_publisher(bq: bigquery.Client, table: str, schema,
                           publisher_id: str, rows: List[Dict],
                           min_rows: int = 1):
    """Fetch-first, non-destructive dimension replace.

    v5 tak: DELETE → fetch → load.  Fetch fail = data hamesha ke liye gaya.
    v6:     fetch (caller) → staging → BEGIN/COMMIT TRANSACTION swap.

    Agar rows khali ya shaq-e-qabil kam hain to RuntimeError — purana data
    chhua tak nahi jata.
    """
    if not publisher_id:
        raise ValueError("swap_dim_for_publisher: publisher_id is required (empty)")

    # ── 🛡️ GUARD A: khali / bohot kam rows → swap se inkaar ──────────────
    if len(rows) < min_rows:
        raise RuntimeError(
            f"{table}/{publisher_id}: fetch returned {len(rows)} rows "
            f"(minimum {min_rows}) — refusing to swap. Existing data preserved."
        )

    # ── 🛡️ GUARD B (v6): purane ke muqable bohot simat gaya → inkaar ──────
    existing = count_dim_rows(bq, table, publisher_id)
    if existing > 0 and len(rows) < existing * DIM_SHRINK_PCT:
        raise RuntimeError(
            f"{table}/{publisher_id}: fetch returned {len(rows):,} rows but "
            f"{existing:,} already exist (< {DIM_SHRINK_PCT:.0%}). Looks like a "
            f"partial fetch — refusing to swap. Set DIM_SHRINK_PCT=0 to override."
        )

    stg  = f"_stg_{table}"
    tid  = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    sid  = f"{PROJECT_ID}.{DATASET_ID}.{stg}"
    cols = ", ".join(f.name for f in schema)

    ensure_table(bq, stg, schema, quiet=True)
    load_rows(bq, stg, schema, rows,
              disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, quiet=True)

    sql = f"""
    BEGIN TRANSACTION;
      DELETE FROM `{tid}` WHERE publisher_id = @pub;
      INSERT INTO `{tid}` ({cols}) SELECT {cols} FROM `{sid}`;
    COMMIT TRANSACTION;
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("pub", "STRING", publisher_id)
    ])

    with_retry(lambda: bq.query(sql, job_config=cfg).result(),
               label=f"swap_{table}", auth_retry=False)

    print(f"  ✅ {len(rows):,} rows → {table}  ({publisher_id})  [atomic swap]")

def write_log(bq, run_id, publisher_id, run_type, start, end, status, totals, error, duration):
    row = [{
        "run_id":              run_id,
        "publisher_id":        publisher_id,
        "run_type":            run_type,
        "start_date":          str(start),
        "end_date":            str(end),
        "status":              status,
        "network_rows":        totals.get("network", 0),
        "network_adtype_rows": totals.get("network_adtype", 0),
        "mediation_rows":      totals.get("mediation", 0),
        "total_rows":          sum(totals.values()),
        "error_message":       (error[:9000] if error else None),
        "duration_seconds":    round(duration, 2),
        "sync_timestamp":      utc_now(),
    }]
    try:
        load_rows(bq, LOG_TABLE, SYNC_LOG_SCHEMA, row)
    except Exception as e:
        print(f"  WARNING: sync_log write failed: {e}")

# =============================================================================
# DIMENSION SYNC   🔴 v6: PEHLE SAARA FETCH, PHIR SWAP
# =============================================================================

def sync_dims(v1, bq: bigquery.Client, account: str, publisher_id: str) -> List[str]:
    """v6: koi bhi DELETE tab tak nahi hoti jab tak TEENO dimensions ka
    poora data haath mein na aa jaye."""
    ts = utc_now()

    # ══════════ PHASE 1 — FETCH (kuch bhi destructive nahi) ══════════
    print("  Fetching account …")
    acc = with_retry(lambda: v1.accounts().get(name=account).execute(), label="account")

    print("  Fetching apps …")
    apps = paginate(
        lambda pageToken=None: v1.accounts().apps().list(parent=account, pageToken=pageToken),
        "apps", label="apps.list"
    )

    print("  Fetching ad units …")
    units = paginate(
        lambda pageToken=None: v1.accounts().adUnits().list(parent=account, pageToken=pageToken),
        "adUnits", label="adUnits.list"
    )

    # ══════════ PHASE 2 — TRANSFORM ══════════
    account_rows = [{
        "account_resource_name": acc.get("name"),
        "publisher_id":          publisher_id,
        "reporting_time_zone":   acc.get("reportingTimeZone"),
        "currency_code":         acc.get("currencyCode"),
        "sync_timestamp":        ts,
    }]

    app_rows, app_ids = [], []
    for a in apps:
        mi = a.get("manualAppInfo", {})
        li = a.get("linkedAppInfo", {})
        app_id = a.get("appId")
        if app_id:
            app_ids.append(app_id)
        app_rows.append({
            "app_resource_name":   a.get("name"),
            "app_id":              app_id,
            "publisher_id":        publisher_id,
            "platform":            a.get("platform"),
            "manual_display_name": mi.get("displayName"),
            "store_app_id":        li.get("appStoreId"),
            "store_display_name":  li.get("displayName"),
            "app_approval_state":  a.get("appApprovalState"),
            "sync_timestamp":      ts,
        })

    unit_rows = [{
        "ad_unit_resource_name": u.get("name"),
        "ad_unit_id":            u.get("adUnitId"),
        "publisher_id":          publisher_id,
        "app_id":                u.get("appId"),
        "ad_unit_display_name":  u.get("displayName"),
        "ad_format":             u.get("adFormat"),
        "ad_types":              u.get("adTypes", []),
        "sync_timestamp":        ts,
    } for u in units]

    # ══════════ PHASE 3 — ATOMIC SWAP (ab mehfooz) ══════════
    swap_dim_for_publisher(bq, DIM_ACCOUNT,  ACCOUNT_SCHEMA,  publisher_id, account_rows)
    swap_dim_for_publisher(bq, DIM_APPS,     APPS_SCHEMA,     publisher_id, app_rows)
    swap_dim_for_publisher(bq, DIM_AD_UNITS, AD_UNITS_SCHEMA, publisher_id, unit_rows)

    print(f"  Publisher {publisher_id}: {len(app_ids)} apps, {len(unit_rows)} ad units")
    return app_ids

# =============================================================================
# REPORT SPECS
# =============================================================================

def _base_spec(start: date, end: date) -> Dict:
    return {
        "dateRange": {"startDate": to_api_date(start), "endDate": to_api_date(end)},
        "localizationSettings": {"currencyCode": ADMOB_CURRENCY},
    }

def _app_filter(app_ids: List[str]) -> Dict:
    return {"dimension": "APP", "matchesAny": {"values": app_ids}}

def network_spec(start, end, app_ids):
    spec = _base_spec(start, end)
    spec["dimensions"]       = ["DATE", "APP", "AD_UNIT", "COUNTRY", "FORMAT", "PLATFORM"]
    spec["metrics"]          = ["AD_REQUESTS", "MATCHED_REQUESTS", "MATCH_RATE",
                                "IMPRESSIONS", "CLICKS", "IMPRESSION_CTR",
                                "IMPRESSION_RPM", "ESTIMATED_EARNINGS", "SHOW_RATE"]
    spec["dimensionFilters"] = [_app_filter(app_ids)]
    return {"reportSpec": spec}

def network_adtype_spec(start, end, app_ids):
    spec = _base_spec(start, end)
    spec["dimensions"]       = ["DATE", "APP", "AD_UNIT", "AD_TYPE",
                                "COUNTRY", "FORMAT", "PLATFORM"]
    spec["metrics"]          = ["MATCHED_REQUESTS", "IMPRESSIONS", "CLICKS",
                                "IMPRESSION_CTR", "ESTIMATED_EARNINGS", "SHOW_RATE"]
    spec["dimensionFilters"] = [_app_filter(app_ids)]
    return {"reportSpec": spec}

def mediation_spec(start, end, app_ids):
    spec = _base_spec(start, end)
    spec["dimensions"]       = ["DATE", "APP", "AD_UNIT", "AD_SOURCE",
                                "MEDIATION_GROUP", "COUNTRY", "FORMAT", "PLATFORM"]
    spec["metrics"]          = ["AD_REQUESTS", "MATCHED_REQUESTS", "MATCH_RATE",
                                "IMPRESSIONS", "CLICKS", "IMPRESSION_CTR",
                                "ESTIMATED_EARNINGS", "OBSERVED_ECPM"]
    spec["dimensionFilters"] = [_app_filter(app_ids)]
    return {"reportSpec": spec}

# =============================================================================
# FETCHERS
# =============================================================================

def fetch_network(v1, account, body):
    return with_retry(
        lambda: v1.accounts().networkReport().generate(parent=account, body=body).execute(),
        label="networkReport"
    )

def fetch_mediation_report(v1, account, body):
    return with_retry(
        lambda: v1.accounts().mediationReport().generate(parent=account, body=body).execute(),
        label="mediationReport"
    )

# =============================================================================
# PARSERS
# =============================================================================

def _empty_row(source: str, ts: str, run_id: str, publisher_id: str) -> Dict:
    return {
        "data_source": source, "run_id": run_id, "sync_timestamp": ts,
        "publisher_id": publisher_id,
        "app_id": None, "app_name": None, "platform": None,
        "ad_unit_id": None, "ad_unit_name": None,
        "ad_format": None, "ad_type": None,
        "country_code": None, "country_name": None,
        "ad_source_id": None, "ad_source_name": None,
        "mediation_group_id": None, "mediation_group_name": None,
        "impressions": None, "clicks": None, "ctr": None,
        "estimated_earnings_micros": None, "ecpm_micros": None,
        "ad_requests": None, "matched_requests": None,
        "fill_rate": None, "match_rate": None, "show_rate": None,
        "observed_ecpm_micros": None,
    }

def _set_base_dims(row, dims):
    row.update({
        "app_id":       dim_val(dims, "APP"),
        "app_name":     dim_lbl(dims, "APP"),
        "platform":     dim_lbl(dims, "PLATFORM"),
        "ad_unit_id":   dim_val(dims, "AD_UNIT"),
        "ad_unit_name": dim_lbl(dims, "AD_UNIT"),
        "ad_format":    dim_lbl(dims, "FORMAT"),
        "country_code": dim_val(dims, "COUNTRY"),
        "country_name": dim_lbl(dims, "COUNTRY"),
    })

def parse_network_rows(report, run_id, publisher_id):
    ts, rows = utc_now(), []
    for item in report:
        rd = item.get("row")
        if not rd:
            continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date_from_dims(dims)
        if not dt:
            continue
        row = _empty_row("admob_network", ts, run_id, publisher_id)
        row["report_date"] = dt
        _set_base_dims(row, dims)
        imp  = metric_val(mets.get("IMPRESSIONS"))
        earn = metric_val(mets.get("ESTIMATED_EARNINGS"))
        req  = metric_val(mets.get("AD_REQUESTS"))
        mat  = metric_val(mets.get("MATCHED_REQUESTS"))
        row.update({
            "impressions":               imp,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earn,
            "ecpm_micros":               metric_val(mets.get("IMPRESSION_RPM")) or safe_ecpm(earn, imp),
            "ad_requests":               req,
            "matched_requests":          mat,
            "fill_rate":                 safe_fill_rate(mat, req),
            "match_rate":                metric_val(mets.get("MATCH_RATE")),
            "show_rate":                 metric_val(mets.get("SHOW_RATE")),
        })
        rows.append(row)
    return rows

def parse_network_adtype_rows(report, run_id, publisher_id):
    ts, rows = utc_now(), []
    for item in report:
        rd = item.get("row")
        if not rd:
            continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date_from_dims(dims)
        if not dt:
            continue
        row = _empty_row("admob_network_adtype", ts, run_id, publisher_id)
        row["report_date"] = dt
        _set_base_dims(row, dims)
        imp  = metric_val(mets.get("IMPRESSIONS"))
        earn = metric_val(mets.get("ESTIMATED_EARNINGS"))
        mat  = metric_val(mets.get("MATCHED_REQUESTS"))
        row.update({
            "ad_type":                   dim_lbl(dims, "AD_TYPE"),
            "impressions":               imp,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earn,
            "ecpm_micros":               safe_ecpm(earn, imp),
            "matched_requests":          mat,
            "show_rate":                 metric_val(mets.get("SHOW_RATE")),
        })
        rows.append(row)
    return rows

def parse_mediation_rows(report, run_id, publisher_id):
    ts, rows = utc_now(), []
    for item in report:
        rd = item.get("row")
        if not rd:
            continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date_from_dims(dims)
        if not dt:
            continue
        row = _empty_row("admob_mediation", ts, run_id, publisher_id)
        row["report_date"] = dt
        _set_base_dims(row, dims)
        imp  = metric_val(mets.get("IMPRESSIONS"))
        earn = metric_val(mets.get("ESTIMATED_EARNINGS"))
        req  = metric_val(mets.get("AD_REQUESTS"))
        mat  = metric_val(mets.get("MATCHED_REQUESTS"))
        row.update({
            "ad_source_id":              dim_val(dims, "AD_SOURCE"),
            "ad_source_name":            dim_lbl(dims, "AD_SOURCE"),
            "mediation_group_id":        dim_val(dims, "MEDIATION_GROUP"),
            "mediation_group_name":      dim_lbl(dims, "MEDIATION_GROUP"),
            "impressions":               imp,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earn,
            "ecpm_micros":               safe_ecpm(earn, imp),
            "ad_requests":               req,
            "matched_requests":          mat,
            "fill_rate":                 safe_fill_rate(mat, req),
            "match_rate":                metric_val(mets.get("MATCH_RATE")),
            "observed_ecpm_micros":      metric_val(mets.get("OBSERVED_ECPM")),
        })
        rows.append(row)
    return rows

# =============================================================================
# BATCHED FETCH   🛡️ v5: (rows, skipped) deta hai
# =============================================================================

def fetch_batched(v1, account, app_ids, start, end, run_id, publisher_id,
                  spec_fn, parse_fn, fetch_fn, batch_size, label) -> Tuple[List[Dict], int]:
    """403 batches ki GINTI wapas karta hai taake caller adhoore data pe
    DELETE na kare."""
    all_rows: List[Dict] = []
    skipped  = 0
    batches  = make_batches(app_ids, batch_size)
    total_b  = len(batches)

    for i, batch in enumerate(batches, 1):
        body = spec_fn(start, end, batch)
        try:
            report = fetch_fn(v1, account, body)
            rows   = parse_fn(report, run_id, publisher_id)
            if len(rows) >= ROW_LIMIT_WARN:
                print(f"  ⚠️  {label} batch {i}/{total_b}: {len(rows):,} rows — near 100K limit!")
            all_rows.extend(rows)
        except HttpError as e:
            if e.resp.status == 403:
                print(f"  WARNING: {label} batch {i}/{total_b} skipped — 403")
                skipped += 1
            else:
                raise

    return all_rows, skipped

# =============================================================================
# ACCOUNT RESOLUTION
# =============================================================================

def get_account_name(v1) -> str:
    """HARD FAILS if ADMOB_PUBLISHER_ID is empty — never auto-picks accounts[0]."""
    if not ADMOB_PUBLISHER_ID:
        raise ValueError(
            "ADMOB_PUBLISHER_ID env var is required. "
            "Refusing to auto-pick from accounts.list() to prevent wrong account."
        )
    raw = ADMOB_PUBLISHER_ID.strip()
    return raw if raw.startswith("accounts/") else f"accounts/{raw}"

# =============================================================================
# TABLES
# =============================================================================

def ensure_all_tables(bq: bigquery.Client):
    print("Ensuring tables …")
    ensure_dataset(bq)
    ensure_table(bq, FACT_TABLE,   UNIFIED_FACT_SCHEMA, is_fact=True)
    ensure_table(bq, DIM_ACCOUNT,  ACCOUNT_SCHEMA)
    ensure_table(bq, DIM_APPS,     APPS_SCHEMA)
    ensure_table(bq, DIM_AD_UNITS, AD_UNITS_SCHEMA)
    ensure_table(bq, LOG_TABLE,    SYNC_LOG_SCHEMA)

# =============================================================================
# SYNC ONE DAY   (v5 guards — unchanged, they work)
# =============================================================================

def sync_one_day(v1, bq, account, app_ids, publisher_id, day, run_id) -> Dict[str, int]:
    """PEHLE poora data haath mein lo — TAB purana hatao."""
    totals = {"network": 0, "network_adtype": 0, "mediation": 0}

    print(f"  [{day}] Fetching network for {publisher_id} …")
    net_rows, net_skip = fetch_batched(
        v1, account, app_ids, day, day, run_id, publisher_id,
        network_spec, parse_network_rows, fetch_network,
        BATCH_NETWORK, "network"
    )

    print(f"  [{day}] Fetching network_adtype for {publisher_id} …")
    nat_rows, nat_skip = fetch_batched(
        v1, account, app_ids, day, day, run_id, publisher_id,
        network_adtype_spec, parse_network_adtype_rows, fetch_network,
        BATCH_NETWORK_ADTYPE, "network_adtype"
    )

    print(f"  [{day}] Fetching mediation for {publisher_id} …")
    try:
        med_rows, med_skip = fetch_batched(
            v1, account, app_ids, day, day, run_id, publisher_id,
            mediation_spec, parse_mediation_rows, fetch_mediation_report,
            BATCH_MEDIATION, "mediation"
        )
    except HttpError as e:
        if e.resp.status == 403:
            print(f"  [{day}] WARNING: Mediation skipped — 403 (no access)")
            med_rows, med_skip = [], 0
        else:
            raise

    # ── 🛡️ GUARD 1: koi bhi batch 403 hua → us din ko haath mat lagao ──
    skipped = net_skip + nat_skip + med_skip
    if skipped:
        raise RuntimeError(
            f"[{day}] {skipped} batch(es) returned 403 — data is INCOMPLETE. "
            f"Refusing to delete+reload this day; existing rows preserved. "
            f"Fix AdMob API access for {publisher_id} and re-run."
        )

    # ── 🛡️ GUARD 2: teeno report bilkul khali → bhi haath mat lagao ──
    if not (net_rows or nat_rows or med_rows):
        print(f"  [{day}] ⚠️  API returned 0 rows across ALL 3 reports — "
              f"SKIPPING delete+load entirely (existing data preserved). "
              f"Next run will retry.")
        return totals

    # ── ab mehfooz: poora data haath mein hai, TAB purana hatao ──
    delete_range_for_publisher(bq, FACT_TABLE, publisher_id, day, day)

    totals["network"]        = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, net_rows)
    totals["network_adtype"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, nat_rows)
    totals["mediation"]      = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, med_rows)

    return totals

# =============================================================================
# SYNC + BACKFILL   🔴 v6: sync_dims ab try/finally ke ANDAR
# =============================================================================

def _run(run_type: str, start_date: date, end_date: date, sleep_between: int):
    publisher_id = normalize_publisher_id(ADMOB_PUBLISHER_ID)
    rid          = run_id_now()
    t0           = time.time()

    print(f"\n=== AdMob {run_type.title()} v6 | publisher={publisher_id} | run_id={rid} ===")
    print(f"  Date range : {start_date} → {end_date}")

    creds   = get_fresh_credentials()
    v1      = get_v1(creds)
    bq      = get_bq_client()
    account = get_account_name(v1)
    print(f"  Account    : {account}")

    ensure_all_tables(bq)

    grand = {"network": 0, "network_adtype": 0, "mediation": 0}
    error, status = None, "SUCCESS"
    total_days = (end_date - start_date).days + 1
    done = 0

    try:
        # 🔴 v6: dimensions ab try ke ANDAR — failure ab log hoti hai
        print("\nSyncing dimensions …")
        app_ids = sync_dims(v1, bq, account, publisher_id)

        if not app_ids:
            raise RuntimeError(
                f"{publisher_id}: 0 apps returned — refusing to run reports "
                f"(app filter would be empty)."
            )

        cur = start_date
        while cur <= end_date:
            done += 1
            print(f"\n--- Day {done}/{total_days}: {cur} ---")
            creds = get_fresh_credentials()
            v1    = get_v1(creds)
            t = sync_one_day(v1, bq, account, app_ids, publisher_id, cur, rid)
            for k in grand:
                grand[k] += t.get(k, 0)
            cur += timedelta(days=1)
            time.sleep(sleep_between)
    except Exception as e:
        status, error = "FAILED", str(e)
        raise
    finally:
        write_log(bq, rid, publisher_id, run_type, start_date, end_date,
                  status, grand, error, time.time() - t0)

    print(f"\n=== {run_type.title()} complete for {publisher_id} ===")
    print(json.dumps(grand, indent=2))

def sync(days_back: int = 3):
    end_date   = datetime.utcnow().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)
    _run("sync", start_date, end_date, sleep_between=1)

def backfill(start_str: str, end_str: str):
    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date   = datetime.strptime(end_str,   "%Y-%m-%d").date()
    if start_date > end_date:
        raise ValueError(f"backfill-start ({start_date}) is after backfill-end ({end_date})")
    _run("backfill", start_date, end_date, sleep_between=2)

# =============================================================================
# 🆕 v6.1 — CHUNK PLANNER   (pehle alag plan_chunks.py thi)
# =============================================================================

# Daily mode mein bhi matrix KHAALI nahi ho sakta — GitHub khaali array par
# workflow-level error deta hai chahe job ka `if:` false ho (run #110:
# 0 seconds, "workflow graph cannot be shown"). Is liye placeholder.
_CHUNK_PLACEHOLDER = [{"idx": 0, "start": "1970-01-01", "end": "1970-01-01"}]


def _emit_gh_output(mode: str, chunks: List[Dict]) -> None:
    line_mode   = f"mode={mode}"
    line_chunks = f"chunks={json.dumps(chunks, separators=(',', ':'))}"
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(line_mode + "\n")
            fh.write(line_chunks + "\n")
    print(line_mode)
    print(line_chunks)


def plan_chunks() -> int:
    """Backfill range ko chhote chunks mein todta hai.

    KYUN: run #107 (Admob_4 / pub-9688) 60-min timeout par cancel hui.
          Maap: 06-05 → 06-21 = 17 din, ~57 min = ~3.2 min/din.
          90 din = ~290 min — single job mein kabhi poora nahi hoga.
          Chunks mein har job ~50 min, aur chunk 4 fail ho to sirf
          chunk 4 dobara chalao.

    Env: BF_START · BF_END · CHUNK_DAYS (default 15)
    Writes to GITHUB_OUTPUT:  mode=daily|backfill  ·  chunks=<json>
    """
    start_raw = (os.environ.get("BF_START") or "").strip()
    end_raw   = (os.environ.get("BF_END")   or "").strip()

    if not start_raw or not end_raw:
        print("Mode: DAILY SYNC (no backfill dates given)")
        _emit_gh_output("daily", _CHUNK_PLACEHOLDER)
        return 0

    try:
        start = datetime.strptime(start_raw, "%Y-%m-%d").date()
        end   = datetime.strptime(end_raw,   "%Y-%m-%d").date()
    except ValueError as e:
        print(f"❌ ERROR: bad date format ({e}). Use YYYY-MM-DD.")
        return 1

    if start > end:
        print(f"❌ ERROR: backfill-start {start} is after backfill-end {end}")
        return 1

    try:
        size = max(1, int((os.environ.get("CHUNK_DAYS") or "15").strip()))
    except ValueError:
        size = 15

    chunks, cur, idx = [], start, 0
    while cur <= end:
        idx += 1
        stop = min(cur + timedelta(days=size - 1), end)
        chunks.append({"idx": idx, "start": cur.isoformat(), "end": stop.isoformat()})
        cur = stop + timedelta(days=1)

    total_days = (end - start).days + 1
    print(f"Mode: BACKFILL {start} → {end} ({total_days} days) "
          f"in {len(chunks)} chunk(s) of {size}")
    for c in chunks:
        span = (datetime.strptime(c["end"], "%Y-%m-%d").date()
                - datetime.strptime(c["start"], "%Y-%m-%d").date()).days + 1
        print(f"   chunk {c['idx']}: {c['start']} → {c['end']}  "
              f"({span}d, ~{round(span * 3.2)} min)")

    _emit_gh_output("backfill", chunks)
    return 0

# =============================================================================
# 🆕 v6.1 — DIMENSION HEALTH CHECK   (pehle alag dim_health.py thi)
# =============================================================================

def dim_health() -> int:
    """Har run ke BAAD chalta hai. Job ko FAIL karta hai agar is publisher
    ki koi dimension table khaali ho — chahe sync "SUCCESS" keh chuki ho.

    KYUN: run #214 mein admob_ad_units_dim se pub-5972202469838280 ki SAARI
          5,713 rows ud gayi thin (DELETE chali, adUnits par 401 aaya, load
          kabhi hua hi nahi). Table din bhar khaali padi rahi aur kisi ko
          pata nahi chala.
    """
    publisher_id = normalize_publisher_id(ADMOB_PUBLISHER_ID)
    if not publisher_id:
        print("❌ ERROR: ADMOB_PUBLISHER_ID missing — cannot run health check")
        return 1

    bq = get_bq_client()
    print(f"\n── dimension health: {PROJECT_ID}.{DATASET_ID} / {publisher_id} " + "─" * 15)

    expected = {DIM_ACCOUNT: 1, DIM_APPS: 1, DIM_AD_UNITS: 1}
    failed   = []

    for table, minimum in expected.items():
        tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
        try:
            row = list(bq.query(
                f"SELECT COUNT(*) AS n, "
                f"CAST(MAX(sync_timestamp) AS STRING) AS last_sync "
                f"FROM `{tid}` WHERE publisher_id = @p",
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("p", "STRING", publisher_id)
                ])
            ).result())[0]
            n, last_sync = row["n"], row["last_sync"]
            mark = "OK   " if n >= minimum else "EMPTY"
            print(f"   [{mark}] {table:<22} {n:>8,} rows   last_sync={last_sync}")
            if n < minimum:
                failed.append(table)
        except Exception as e:
            print(f"   [ERROR] {table:<22} {type(e).__name__}: {e}")
            failed.append(table)

    # Fact freshness — dims bhari hon magar fact purani ho to bhi masla hai
    try:
        row = list(bq.query(
            f"SELECT CAST(MAX(report_date) AS STRING) AS last_dt, "
            f"DATE_DIFF(CURRENT_DATE(), MAX(report_date), DAY) AS stale "
            f"FROM `{PROJECT_ID}.{DATASET_ID}.{FACT_TABLE}` WHERE publisher_id = @p",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("p", "STRING", publisher_id)
            ])
        ).result())[0]
        stale = row["stale"]
        mark = "OK   " if stale is not None and stale <= 4 else "STALE"
        print(f"   [{mark}] {FACT_TABLE:<22} last={row['last_dt']} ({stale}d old)")
    except Exception as e:
        print(f"   [ERROR] {FACT_TABLE:<22} {type(e).__name__}: {e}")

    if failed:
        print("\n:: DIMENSION TABLE EMPTY FOR THIS PUBLISHER ::")
        print(f"   Affected: {', '.join(failed)}")
        print("   Ye #214 wali shakl hai: DELETE chali, fetch mara, load nahi hua.")
        print("   → Workflow dobara chalao. Agar phir bhi khaali rahe to AdMob API")
        print("     is account ke liye apps/adUnits list refuse kar rahi hai.")
        return 1

    print("\n   Dimension health OK.")
    return 0

# =============================================================================
# CLI
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="AdMob → BigQuery sync v6.1")
    p.add_argument("--days",           type=int, default=3)
    p.add_argument("--backfill-start", type=str)
    p.add_argument("--backfill-end",   type=str)
    # 🆕 v6.1: ye dono modes pehle alag .py files mein the
    p.add_argument("--plan-chunks", action="store_true",
                   help="Backfill range ko chunks mein tode (GITHUB_OUTPUT likhta hai)")
    p.add_argument("--dim-health",  action="store_true",
                   help="Dimension tables khaali to nahi — exit 1 agar hain")
    # Kept for backwards compatibility with existing GitHub Actions YAML
    p.add_argument("--chunk",          type=int, default=1)
    p.add_argument("--chunk-days",     type=int, default=1)
    p.add_argument("--enable-campaign", action="store_true")
    p.add_argument("--enable-campaign-beta", action="store_true")
    args = p.parse_args()

    # --plan-chunks ko koi credential nahi chahiye — validate_config se pehle
    if args.plan_chunks:
        sys.exit(plan_chunks())

    if not validate_config():
        sys.exit(1)

    if args.dim_health:
        try:
            sys.exit(dim_health())
        except Exception as e:
            print(f"FATAL ERROR (dim-health): {e}")
            sys.exit(1)

    try:
        if args.backfill_start and args.backfill_end:
            backfill(args.backfill_start, args.backfill_end)
        else:
            sync(args.days)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
