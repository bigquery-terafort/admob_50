"""
AdMob → BigQuery  (ACCOUNT 2)
=====================================================
Identical to v3 FINAL but with:
- publisher_id column added to all writes
- Publisher-scoped DELETE (won't wipe account 1 or 3 data)
- Uses ACCOUNT 2's OAuth credentials via env vars
"""

import os
import sys
import json
import time
import argparse
import socket
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery
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

BATCH_NETWORK         = 5
BATCH_NETWORK_ADTYPE  = 3
BATCH_MEDIATION       = 2

# =============================================================================
# SCHEMAS
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
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        return False
    return True

# =============================================================================
# AUTH
# =============================================================================

def get_fresh_credentials() -> Credentials:
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
# RETRY
# =============================================================================

def with_retry(fn, label="call"):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except HttpError as e:
            if e.resp.status < 500 and e.resp.status != 429:
                raise
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

def paginate(callable_, items_key: str) -> List[Dict]:
    results, page_token = [], None
    while True:
        resp = with_retry(
            lambda pt=page_token: callable_(pageToken=pt).execute() if pt else callable_().execute()
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
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = BQ_LOCATION
        bq.create_dataset(ds)
        print(f"  Created dataset: {ds_id}")

def ensure_table(bq: bigquery.Client, name: str, schema, is_fact=False):
    tid = f"{PROJECT_ID}.{DATASET_ID}.{name}"
    try:
        bq.get_table(tid)
        print(f"  Table exists: {name}")
    except Exception:
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
              disposition=bigquery.WriteDisposition.WRITE_APPEND) -> int:
    if not rows:
        print(f"  No rows for {table}")
        return 0
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    cfg = bigquery.LoadJobConfig(schema=schema, write_disposition=disposition)

    def _load():
        job = bq.load_table_from_json(rows, tid, job_config=cfg)
        job.result()
        return len(rows)

    n = with_retry(_load, label=table)
    print(f"  Loaded {n:,} rows → {table}")
    return n

def delete_range_for_publisher(bq: bigquery.Client, table: str, publisher_id: str,
                                start: date, end: date):
    """Delete ONLY this publisher's rows. Won't touch account 1 or 3 data."""
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    bq.query(
        f"""
        DELETE FROM `{tid}`
        WHERE report_date BETWEEN '{start}' AND '{end}'
          AND publisher_id = '{publisher_id}'
        """
    ).result()
    print(f"  Deleted {table} for {publisher_id}: {start} → {end}")

def delete_dim_for_publisher(bq: bigquery.Client, table: str, publisher_id: str):
    """Delete ONLY this publisher's dim rows."""
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    bq.query(
        f"""
        DELETE FROM `{tid}`
        WHERE publisher_id = '{publisher_id}'
        """
    ).result()
    print(f"  Deleted dim {table} for {publisher_id}")

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
        "error_message":       error,
        "duration_seconds":    round(duration, 2),
        "sync_timestamp":      utc_now(),
    }]
    try:
        load_rows(bq, LOG_TABLE, SYNC_LOG_SCHEMA, row)
    except Exception as e:
        print(f"  WARNING: sync_log write failed: {e}")

# =============================================================================
# DIMENSION SYNC (publisher-scoped)
# =============================================================================

def sync_dims(v1, bq: bigquery.Client, account: str, publisher_id: str) -> List[str]:
    ts = utc_now()

    acc = with_retry(lambda: v1.accounts().get(name=account).execute())
    bq.query(
        f"""
        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{DIM_ACCOUNT}`
        WHERE publisher_id = '{publisher_id}'
        """
    ).result()
    load_rows(bq, DIM_ACCOUNT, ACCOUNT_SCHEMA, [{
        "account_resource_name": acc.get("name"),
        "publisher_id":          publisher_id,
        "reporting_time_zone":   acc.get("reportingTimeZone"),
        "currency_code":         acc.get("currencyCode"),
        "sync_timestamp":        ts,
    }])

    delete_dim_for_publisher(bq, DIM_APPS, publisher_id)
    apps = paginate(
        lambda pageToken=None: v1.accounts().apps().list(parent=account, pageToken=pageToken),
        "apps"
    )
    app_rows = []
    app_ids  = []
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
    load_rows(bq, DIM_APPS, APPS_SCHEMA, app_rows)

    delete_dim_for_publisher(bq, DIM_AD_UNITS, publisher_id)
    units = paginate(
        lambda pageToken=None: v1.accounts().adUnits().list(parent=account, pageToken=pageToken),
        "adUnits"
    )
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
    load_rows(bq, DIM_AD_UNITS, AD_UNITS_SCHEMA, unit_rows)

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
    return {
        "dimension": "APP",
        "matchesAny": {"values": app_ids}
    }

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
        lambda: v1.accounts().networkReport().generate(parent=account, body=body).execute()
    )

def fetch_mediation_report(v1, account, body):
    return with_retry(
        lambda: v1.accounts().mediationReport().generate(parent=account, body=body).execute()
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
        if not rd: continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date_from_dims(dims)
        if not dt: continue
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
        if not rd: continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date_from_dims(dims)
        if not dt: continue
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
        if not rd: continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date_from_dims(dims)
        if not dt: continue
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
# BATCHED FETCH
# =============================================================================

def fetch_batched(v1, account, app_ids, start, end, run_id, publisher_id,
                  spec_fn, parse_fn, fetch_fn, batch_size, label):
    all_rows = []
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
            else:
                raise

    return all_rows

# =============================================================================
# ACCOUNT
# =============================================================================

def get_account_name(v1) -> str:
    if ADMOB_PUBLISHER_ID:
        raw = ADMOB_PUBLISHER_ID.strip()
        return raw if raw.startswith("accounts/") else f"accounts/{raw}"
    resp = with_retry(lambda: v1.accounts().list().execute())
    accounts = resp.get("account", []) or resp.get("accounts", [])
    if not accounts:
        raise ValueError("No AdMob accounts found.")
    return accounts[0]["name"]

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
# SYNC ONE DAY
# =============================================================================

def sync_one_day(v1, bq, account, app_ids, publisher_id, day, run_id) -> Dict[str, int]:
    totals = {"network": 0, "network_adtype": 0, "mediation": 0}

    delete_range_for_publisher(bq, FACT_TABLE, publisher_id, day, day)

    print(f"  [{day}] Fetching network for {publisher_id} …")
    net_rows = fetch_batched(
        v1, account, app_ids, day, day, run_id, publisher_id,
        network_spec, parse_network_rows, fetch_network,
        BATCH_NETWORK, "network"
    )
    totals["network"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, net_rows)

    print(f"  [{day}] Fetching network_adtype for {publisher_id} …")
    nat_rows = fetch_batched(
        v1, account, app_ids, day, day, run_id, publisher_id,
        network_adtype_spec, parse_network_adtype_rows, fetch_network,
        BATCH_NETWORK_ADTYPE, "network_adtype"
    )
    totals["network_adtype"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, nat_rows)

    print(f"  [{day}] Fetching mediation for {publisher_id} …")
    try:
        med_rows = fetch_batched(
            v1, account, app_ids, day, day, run_id, publisher_id,
            mediation_spec, parse_mediation_rows, fetch_mediation_report,
            BATCH_MEDIATION, "mediation"
        )
        totals["mediation"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, med_rows)
    except HttpError as e:
        if e.resp.status == 403:
            print(f"  [{day}] WARNING: Mediation skipped — 403 (no access)")
        else:
            raise

    return totals

# =============================================================================
# SYNC + BACKFILL
# =============================================================================

def sync(days_back: int = 3):
    publisher_id = normalize_publisher_id(ADMOB_PUBLISHER_ID)
    rid          = run_id_now()
    t0           = time.time()
    end_date     = datetime.utcnow().date() - timedelta(days=1)
    start_date   = end_date - timedelta(days=days_back - 1)

    print(f"\n=== AdMob Sync Account 2 | publisher={publisher_id} | run_id={rid} ===")
    print(f"  Date range : {start_date} → {end_date}")

    creds   = get_fresh_credentials()
    v1      = get_v1(creds)
    bq      = get_bq_client()
    account = get_account_name(v1)
    print(f"  Account    : {account}")

    ensure_all_tables(bq)

    print("\nSyncing dimensions …")
    app_ids = sync_dims(v1, bq, account, publisher_id)

    grand = {"network": 0, "network_adtype": 0, "mediation": 0}
    error, status = None, "SUCCESS"

    try:
        cur = start_date
        while cur <= end_date:
            creds = get_fresh_credentials()
            v1    = get_v1(creds)
            t = sync_one_day(v1, bq, account, app_ids, publisher_id, cur, rid)
            for k in grand:
                grand[k] += t.get(k, 0)
            cur += timedelta(days=1)
            time.sleep(1)
    except Exception as e:
        status, error = "FAILED", str(e)
        raise
    finally:
        write_log(bq, rid, publisher_id, "sync", start_date, end_date,
                  status, grand, error, time.time()-t0)

    print(f"\n=== Sync complete for {publisher_id} ===")
    print(json.dumps(grand, indent=2))

def backfill(start_str: str, end_str: str):
    publisher_id = normalize_publisher_id(ADMOB_PUBLISHER_ID)
    rid          = run_id_now()
    t0           = time.time()
    start_date   = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date     = datetime.strptime(end_str,   "%Y-%m-%d").date()

    print(f"\n=== AdMob Backfill Account 2 | publisher={publisher_id} | run_id={rid} ===")
    print(f"  Date range : {start_date} → {end_date}")

    creds   = get_fresh_credentials()
    v1      = get_v1(creds)
    bq      = get_bq_client()
    account = get_account_name(v1)
    print(f"  Account    : {account}")

    ensure_all_tables(bq)

    print("\nSyncing dimensions …")
    app_ids = sync_dims(v1, bq, account, publisher_id)

    grand = {"network": 0, "network_adtype": 0, "mediation": 0}
    error, status = None, "SUCCESS"
    cur   = start_date
    total_days = (end_date - start_date).days + 1
    done  = 0

    try:
        while cur <= end_date:
            done += 1
            print(f"\n--- Day {done}/{total_days}: {cur} ---")

            creds = get_fresh_credentials()
            v1    = get_v1(creds)

            t = sync_one_day(v1, bq, account, app_ids, publisher_id, cur, rid)
            for k in grand:
                grand[k] += t.get(k, 0)

            cur += timedelta(days=1)
            time.sleep(2)

    except Exception as e:
        status, error = "FAILED", str(e)
        raise
    finally:
        write_log(bq, rid, publisher_id, "backfill", start_date, end_date,
                  status, grand, error, time.time()-t0)

    print(f"\n=== Backfill complete for {publisher_id} ===")
    print(json.dumps(grand, indent=2))

# =============================================================================
# CLI
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="AdMob → BigQuery sync (Account 2)")
    p.add_argument("--days",           type=int, default=3)
    p.add_argument("--backfill-start", type=str)
    p.add_argument("--backfill-end",   type=str)
    p.add_argument("--chunk",          type=int, default=1)
    p.add_argument("--chunk-days",     type=int, default=1)
    p.add_argument("--enable-campaign", action="store_true")
    p.add_argument("--enable-campaign-beta", action="store_true")
    args = p.parse_args()

    if not validate_config():
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
