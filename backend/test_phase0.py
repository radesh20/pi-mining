"""
Phase 0 Diagnostic — run this from your backend/ directory:

    cd backend
    python test_phase0.py

What it does:
  1. Connects to Celonis using your .env credentials
  2. Hits every table in ACTIVITY_TABLES one by one
  3. Reports: row count, columns found, activity name, case_link column, vendor resolution
  4. Shows a final summary so you know exactly what the union will look like
  5. Does NOT write any cache or modify anything — read-only

Requirements: your .env must be present in backend/ (same place you run uvicorn from)
"""

import os
import sys
import json
import warnings
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: load .env and add app to path
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent / ".env"
    print(f"Looking for .env at: {env_path}")
    print(f"File exists: {env_path.exists()}")
    load_dotenv(env_path, override=True)
    print("✓ .env loaded\n")
except ImportError:
    print("⚠  python-dotenv not installed — reading os.environ directly\n")

sys.path.insert(0, str(Path(__file__).parent))

# ---------------------------------------------------------------------------
# Read config values — mirrors config.py alias logic exactly
# config.py uses: CELONIS_BASE_URL → fallback CELONIS_URL
#                 CELONIS_API_TOKEN → fallback CELONIS_API_KEY
#                 ACTIVITY_TABLE (singular) from .env → parsed into list
# ---------------------------------------------------------------------------
CELONIS_BASE_URL    = os.getenv("CELONIS_BASE_URL", os.getenv("CELONIS_URL", ""))
CELONIS_API_TOKEN   = os.getenv("CELONIS_API_TOKEN", os.getenv("CELONIS_API_KEY", ""))
CELONIS_KEY_TYPE    = os.getenv("CELONIS_KEY_TYPE", "BEARER")
DATA_POOL_ID        = os.getenv("CELONIS_DATA_POOL_ID", "")
DATA_MODEL_ID       = os.getenv("CELONIS_DATA_MODEL_ID", "")
ACTIVITY_TABLES_RAW = os.getenv("ACTIVITY_TABLE", "")   # singular — matches your .env
MAX_ROWS            = int(os.getenv("CELONIS_EVENT_LOG_MAX_ROWS", "500") or 500)

ACTIVITY_TABLES = [t.strip() for t in ACTIVITY_TABLES_RAW.split(",") if t.strip()]

print("=" * 65)
print("CONFIG CHECK")
print("=" * 65)
print(f"  CELONIS_BASE_URL  : {'✓ set' if CELONIS_BASE_URL else '✗ MISSING'}")
print(f"  CELONIS_API_TOKEN : {'✓ set' if CELONIS_API_TOKEN else '✗ MISSING'}")
print(f"  DATA_POOL_ID      : {'✓ set' if DATA_POOL_ID else '✗ MISSING'}")
print(f"  DATA_MODEL_ID     : {'✓ set' if DATA_MODEL_ID else '✗ MISSING'}")
print(f"  ACTIVITY_TABLES   : {len(ACTIVITY_TABLES)} tables configured")
print(f"  MAX_ROWS per table: {MAX_ROWS}")
print()

if not all([CELONIS_BASE_URL, CELONIS_API_TOKEN, DATA_POOL_ID, DATA_MODEL_ID]):
    print("✗ Missing required env vars. Check your .env file.")
    sys.exit(1)

if not ACTIVITY_TABLES:
    print("✗ ACTIVITY_TABLES is empty in .env. Add the t_e_custom_* table names.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Connect to Celonis
# ---------------------------------------------------------------------------
print("=" * 65)
print("CONNECTING TO CELONIS")
print("=" * 65)

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        from pycelonis import get_celonis
    celonis = get_celonis(
        base_url=CELONIS_BASE_URL,
        api_token=CELONIS_API_TOKEN,
        key_type=CELONIS_KEY_TYPE,
    )
    print("  ✓ SDK connection OK")
except Exception as e:
    print(f"  ✗ Connection failed: {e}")
    sys.exit(1)

try:
    data_pool  = celonis.data_integration.get_data_pool(DATA_POOL_ID)
    data_model = data_pool.get_data_model(DATA_MODEL_ID)
    print(f"  ✓ Data model loaded: {data_model.name}")
except Exception as e:
    print(f"  ✗ Data pool/model load failed: {e}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Column alias detection (mirrors celonis_service.py logic)
# ---------------------------------------------------------------------------
TIMESTAMP_ALIASES = [
    "EVENTTIME", "TIMESTAMP", "TIME", "CREATEDATE",
    "AEDAT", "BEDAT", "BUDAT", "BLDAT", "CPUDT",
    "PSTNG_DATE", "CREATED_AT", "CHANGED_AT",
]
ACTIVITY_ALIASES = [
    "ACTIVITYEN", "ACTIVITY_EN", "ACTIVITY", "ACTIVIT",
    "STEP", "STEP_NAME", "PROCESS_STEP",
]
CASE_LINK_ALIASES = [
    "AccountingDocumentHeader_ID", "PurchasingDocumentHeader_ID",
    "PurchasingDocumentItem_ID", "VimHeader_ID", "VimWorkitem_ID",
    "GoodsReceiptHeader_ID", "GoodsReceiptItem_ID",
    "ServiceEntrySheet_ID", "ServiceEntrySheetItem_ID",
    "InvoiceVerificationDocument_ID", "PaymentDocument_ID",
    "RequisitionHeader_ID", "RequisitionItem_ID",
    "OutlineAgreementHeader_ID", "OutlineAgreementItem_ID",
    "ContractHeader_ID", "ContractItem_ID",
    "CASEKEY", "EBELN", "BELNR", "MBLNR", "ID", "CASE_ID", "OBJECT_ID",
]

def find_col(cols, aliases):
    cols_upper = {c.upper(): c for c in cols}
    for a in aliases:
        if a.upper() in cols_upper:
            return cols_upper[a.upper()]
    return None

def activity_from_table(name):
    import re
    stem = name.replace("t_e_custom_", "").replace("T_E_CUSTOM_", "")
    stem = re.sub(r"([a-z])([A-Z])", r"\1 \2", stem)
    return stem.replace("_", " ").title().strip() or name

# ---------------------------------------------------------------------------
# Test each table
# ---------------------------------------------------------------------------
print()
print("=" * 65)
print(f"TESTING {len(ACTIVITY_TABLES)} ACTIVITY TABLES")
print("=" * 65)

results = []

for table_name in ACTIVITY_TABLES:
    print(f"\n  ▶ {table_name}")

    # Get columns
    try:
        table_obj = data_model.get_tables().find(table_name)
        if not table_obj:
            print(f"    ✗ TABLE NOT FOUND in data model")
            results.append({
                "table": table_name, "status": "NOT_FOUND",
                "rows": 0, "ts_col": None, "act_col": None, "case_col": None,
            })
            continue
        cols = [c.name for c in table_obj.get_columns()]
        print(f"    columns ({len(cols)}): {', '.join(cols[:12])}{'...' if len(cols) > 12 else ''}")
    except Exception as e:
        print(f"    ✗ Could not read columns: {e}")
        results.append({
            "table": table_name, "status": "COLUMN_ERROR",
            "rows": 0, "ts_col": None, "act_col": None, "case_col": None,
        })
        continue

    ts_col       = find_col(cols, TIMESTAMP_ALIASES)
    act_col      = find_col(cols, ACTIVITY_ALIASES)
    case_lnk_col = find_col(cols, CASE_LINK_ALIASES)

    print(f"    timestamp  : {ts_col or '✗ NOT FOUND'}")
    print(f"    activity   : {act_col or f'✗ not found → will use {activity_from_table(table_name)} (from table name)'}")
    print(f"    case_link  : {case_lnk_col or '✗ NOT FOUND — vendor resolution will be UNKNOWN for this table'}")

    if not ts_col:
        print(f"    ⚠  SKIPPED — no timestamp column")
        results.append({
            "table": table_name, "status": "NO_TIMESTAMP",
            "rows": 0, "ts_col": None, "act_col": act_col, "case_col": case_lnk_col,
        })
        continue

    # Fetch a small sample to confirm rows exist
    try:
        from pycelonis.pql import PQL, PQLColumn

        # Celonis PQL uses alias name (without t_ prefix) even though
        # get_tables() returns the full storage name with t_ prefix.
        pql_name = table_name[2:] if table_name.startswith("t_") else table_name

        pq = PQL()
        pq += PQLColumn(query=f'"{pql_name}"."{ts_col}"', name="ts")
        if case_lnk_col:
            pq += PQLColumn(query=f'"{pql_name}"."{case_lnk_col}"', name="case_link")
        if act_col:
            pq += PQLColumn(query=f'"{pql_name}"."{act_col}"', name="activity")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            sample_df = data_model.export_data_frame(pq)

        row_count = len(sample_df)
        print(f"    ✓ {row_count} rows returned")

        if row_count > 0 and case_lnk_col:
            unique_cases = sample_df["case_link"].nunique() if "case_link" in sample_df.columns else "?"
            print(f"    unique case_link values: {unique_cases}")

        if row_count > 0:
            print(f"    sample timestamps: {list(sample_df['ts'].dropna().head(3).astype(str))}")

        results.append({
            "table": table_name,
            "status": "OK" if row_count > 0 else "EMPTY",
            "rows": row_count,
            "ts_col": ts_col,
            "act_col": act_col or f"[derived: {activity_from_table(table_name)}]",
            "case_col": case_lnk_col,
        })

    except Exception as e:
        print(f"    ✗ PQL query failed: {e}")
        results.append({
            "table": table_name, "status": "QUERY_ERROR",
            "rows": 0, "ts_col": ts_col, "act_col": act_col, "case_col": case_lnk_col,
            "error": str(e),
        })

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
print()
print("=" * 65)
print("SUMMARY")
print("=" * 65)

ok        = [r for r in results if r["status"] == "OK"]
empty     = [r for r in results if r["status"] == "EMPTY"]
no_ts     = [r for r in results if r["status"] == "NO_TIMESTAMP"]
not_found = [r for r in results if r["status"] == "NOT_FOUND"]
errors    = [r for r in results if r["status"] in ("QUERY_ERROR", "COLUMN_ERROR")]
no_case   = [r for r in ok if not r["case_col"]]

total_rows = sum(r["rows"] for r in ok)

print(f"\n  ✓ Tables with data        : {len(ok)}/{len(results)}")
print(f"  ✓ Total rows (all tables) : {total_rows}")
print(f"  ⚠  Empty tables           : {len(empty)}")
print(f"  ✗ Tables not in model     : {len(not_found)}")
print(f"  ✗ No timestamp column     : {len(no_ts)}")
print(f"  ✗ Query errors            : {len(errors)}")
print(f"  ⚠  No case_link (vendor=UNKNOWN): {len(no_case)}")

if ok:
    print(f"\n  Tables contributing to union:")
    for r in ok:
        bridge_status = "✓ vendor resolvable" if r["case_col"] else "⚠  vendor=UNKNOWN"
        print(f"    {r['table']:<50} {r['rows']:>6} rows  {bridge_status}")

if not_found:
    print(f"\n  Tables NOT FOUND in data model (remove from .env or fix name):")
    for r in not_found:
        print(f"    ✗ {r['table']}")

if no_ts:
    print(f"\n  Tables skipped (no timestamp):")
    for r in no_ts:
        print(f"    ⚠  {r['table']}")

if no_case:
    print(f"\n  Tables with no case_link — these will have vendor_id=UNKNOWN:")
    print(f"  (This is OK for the demo — events still show up, just no vendor filter)")
    for r in no_case:
        print(f"    ⚠  {r['table']}")

if errors:
    print(f"\n  Tables with errors:")
    for r in errors:
        print(f"    ✗ {r['table']}: {r.get('error', r['status'])}")

print()
if len(ok) == 0:
    print("✗ PHASE 0 NOT READY — no tables returned data. Check errors above.")
elif len(ok) < len(results) * 0.5:
    print("⚠  PHASE 0 PARTIAL — fewer than half the tables are working.")
    print("   The app will run but some process steps will be missing.")
    print("   Check NOT_FOUND tables — likely a name mismatch in .env.")
else:
    print(f"✓ PHASE 0 LOOKS GOOD — {len(ok)} tables will feed the union.")
    print(f"  Expected bot response: ~{total_rows} events, not 35 cases.")
    print()
    print("  Next: delete warm_cache.json (or event_log_cache.json) and")
    print("  restart the backend to trigger a fresh Celonis extraction.")

print()