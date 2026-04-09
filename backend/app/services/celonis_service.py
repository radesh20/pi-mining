import logging
import math
import threading
import warnings
import time
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.config import settings

logger = logging.getLogger(__name__)


class CelonisConnectionError(Exception):
    pass


class CelonisService:
    """
    Celonis data extraction service for object-centric P2P models.
    """

    _warned_missing_activity_table = False
    _warned_missing_case_table = False
    _shared_lock = threading.Lock()
    _shared_celonis = None
    _shared_data_pool = None
    _shared_data_model = None
    _shared_initialized = False
    _shared_tables_loaded_at = 0.0
    _shared_table_names: List[str] = []
    _shared_table_columns: Dict[str, List[str]] = {}

    # ── NEW: cache discovered sources so we don't re-scan on every call ──
    _shared_event_source: Optional[Dict[str, Any]] = None
    _shared_case_attr_source: Optional[Dict[str, Any]] = None

    @classmethod
    def reset_shared_connection(cls) -> None:
        with cls._shared_lock:
            cls._shared_celonis = None
            cls._shared_data_pool = None
            cls._shared_data_model = None
            cls._shared_initialized = False
            cls._shared_tables_loaded_at = 0.0
            cls._shared_table_names = []
            cls._shared_table_columns = {}
            cls._shared_event_source = None
            cls._shared_case_attr_source = None
            cls._warned_missing_activity_table = False
            cls._warned_missing_case_table = False
        logger.info("CelonisService shared connection and schema caches have been reset.")

    def clear_instance_caches(self) -> None:
        self._event_log_cache = None
        self._case_attributes_cache = None
        self._vendor_mapping_cache = None
        self._event_with_vendor_cache = None
        self._variants_cache = None
        self._throughput_cache = None
        self._activity_freq_cache = None
        self._resource_mapping_cache = None
        self._case_durations_cache = None
        self._vendor_stats_cache = None
        self._table_data_cache = {}
        logger.info("CelonisService instance caches cleared.")

    def __init__(self):
        self.celonis = None
        self.data_pool = None
        self.data_model = None

        self.activity_table = settings.ACTIVITY_TABLE
        self.activity_tables = settings.ACTIVITY_TABLES
        self.case_col = settings.CASE_COLUMN
        self.activity_col = settings.ACTIVITY_COLUMN
        self.timestamp_col = settings.TIMESTAMP_COLUMN
        self.resource_col = settings.RESOURCE_COLUMN
        self.resource_role_col = settings.RESOURCE_ROLE_COLUMN
        self.document_col = "EBELN"
        self.transaction_col = "TRANSACTIONCODE"

        self.case_table = settings.CASE_TABLE
        self.case_table_doc_col = settings.CASE_TABLE_DOC_COLUMN
        self.vendor_col = settings.VENDOR_ID_COLUMN
        self.payment_terms_col = settings.PAYMENT_TERMS_COLUMN
        self.currency_col = settings.CURRENCY_COLUMN
        self.amount_col = settings.AMOUNT_COLUMN

        self._event_log_cache = None
        self._case_attributes_cache = None
        self._vendor_mapping_cache = None
        self._event_with_vendor_cache = None
        self._variants_cache = None
        self._throughput_cache = None
        self._activity_freq_cache = None
        self._resource_mapping_cache = None
        self._case_durations_cache = None
        self._vendor_stats_cache = None
        self._table_data_cache: Dict[str, pd.DataFrame] = {}

        self._connect()
        self._normalize_configured_tables()

    # ─────────────────────────────────────────────────────────────────────
    # CONNECTION
    # ─────────────────────────────────────────────────────────────────────
    def _build_ocpm_event_log(self) -> pd.DataFrame:
        """
        Build an event log from ALL activity-like OCPM tables instead of one table.
        This is the main fix for the 140-events problem.
        """
        table_names = self._get_table_names()

        activity_aliases = [
            self.activity_col,
            "ACTIVITYEN", "ACTIVITY", "ACTIVITY_NAME",
            "STEP", "STATUS", "TYPE",
            "EVENTTYPE", "PROCESSSTEP"
        ]
        timestamp_aliases = [
            self.timestamp_col,
            "EVENTTIME", "EVENT_TIME",
            "TIMESTAMP", "TIME", "DATETIME",
            "CREATEDAT", "CREATED_AT",
            "POSTINGDATE", "POSTING_DATE",
            "DOCUMENTDATE", "DOCUMENT_DATE",
            "ERDAT", "BUDAT", "BLDAT",
            "ZFBDT"
        ]
        case_aliases = [
            self.case_col,
            "CASEKEY", "_CASE_KEY", "CASE_KEY",
            "CASE_ID", "CASEID",
            "PROCESSINSTANCEID", "PROCESS_INSTANCE_ID",
            "BELNR", "EBELN", "VBELN", "AUFNR",
            "ACCOUNTINGDOCUMENTHEADER_ID",
            "PURCHASINGDOCUMENTHEADER_ID",
            "APINVOICE_ID",
            "VIMHEADER_ID",
            "ID",
        ]
        resource_aliases = [
            self.resource_col,
            "USERNAME", "USER_NAME", "USER",
            "RESOURCE", "UNAME", "USNAM",
            "AGENT", "AGENTNAME", "PERFORMER",
        ]
        role_aliases = [
            self.resource_role_col,
            "USERTYPE", "USER_TYPE",
            "ROLE", "RESOURCE_ROLE", "CLASS",
        ]
        document_aliases = [
            self.document_col,
            "EBELN", "DOCUMENT_NUMBER", "DOCUMENTNUMBER",
            "PO_NUMBER", "PONUMBER", "BELNR", "VBELN",
        ]
        tcode_aliases = [
            self.transaction_col,
            "TRANSACTIONCODE", "TRANSACTION_CODE", "TCODE", "T_CODE",
        ]

        candidate_tables = []
        for table_name in table_names:
            cols = self._table_columns_safe(table_name)
            if not cols:
                continue

            if table_name.startswith("t_e_custom_"):
                candidate_tables.append({
                    "table": table_name,
                    "columns": cols,
                    "activity": table_name,
                    "timestamp": self._find_col_by_aliases(cols, timestamp_aliases),
                    "case_id": self._find_col_by_aliases(cols, case_aliases),
                    "resource": self._find_col_by_aliases(cols, resource_aliases),
                    "resource_role": self._find_col_by_aliases(cols, role_aliases),
                    "document_number": self._find_col_by_aliases(cols, document_aliases),
                    "transaction_code": self._find_col_by_aliases(cols, tcode_aliases),
                })
                continue

            activity_col = self._find_col_by_aliases(cols, activity_aliases)
            timestamp_col = self._find_col_by_aliases(cols, timestamp_aliases)

            if timestamp_col:
                if not activity_col:
                    activity_col = "UNKNOWN_ACTIVITY"
                candidate_tables.append({
                    "table": table_name,
                    "columns": cols,
                    "activity": activity_col,
                    "timestamp": timestamp_col,
                    "case_id": self._find_col_by_aliases(cols, case_aliases),
                    "resource": self._find_col_by_aliases(cols, resource_aliases),
                    "resource_role": self._find_col_by_aliases(cols, role_aliases),
                    "document_number": self._find_col_by_aliases(cols, document_aliases),
                    "transaction_code": self._find_col_by_aliases(cols, tcode_aliases),
                })

        logger.info("🔥 USING RELAXED OCPM MULTI-TABLE LOGIC")
        logger.info(f"Tables scanned: {table_names}")
        logger.info(f"Activity-like tables found: {[c['table'] for c in candidate_tables]}")
        logger.info(f"🔥 USING EVENT TABLES: {[t['table'] for t in candidate_tables]}")

        if not candidate_tables:
            raise Exception("No activity-like tables found across the OCPM model.")

        frames = []
        tables_used = []
        per_table_row_counts = {}
        for candidate in candidate_tables:
            table_name = candidate["table"]

            selected_cols = []
            for key in ["case_id", "activity", "timestamp", "resource", "resource_role", "document_number", "transaction_code"]:
                col = candidate.get(key)
                if col and col not in selected_cols and col not in [table_name, "UNKNOWN_ACTIVITY"]:
                    selected_cols.append(col)

            # fallback: if case_id not found, try the first *_ID column
            if not candidate.get("case_id"):
                id_like = [c for c in candidate["columns"] if str(c).upper().endswith("_ID")]
                if id_like:
                    candidate["case_id"] = id_like[0]
                    if id_like[0] not in selected_cols:
                        selected_cols.append(id_like[0])

            if not selected_cols:
                continue

            raw_df = self.get_table_data(
                table_name=table_name,
                columns=selected_cols,
                use_cache=False,
            )

            logger.info(f"Extracting from {table_name}, rows={len(raw_df)}")

            if raw_df.empty:
                continue

            rename_map = {}
            for std_col in ["case_id", "activity", "timestamp", "resource", "resource_role", "document_number", "transaction_code"]:
                src = candidate.get(std_col)
                if src and src in raw_df.columns:
                    rename_map[src] = std_col

            df = raw_df.rename(columns=rename_map).copy()

            for col in ["case_id", "activity", "timestamp", "resource", "resource_role", "document_number", "transaction_code"]:
                if col not in df.columns:
                    df[col] = None

            if candidate.get("activity") == "UNKNOWN_ACTIVITY":
                df["activity"] = "UNKNOWN_ACTIVITY"
            elif candidate.get("activity") == table_name:
                df["activity"] = table_name.replace("t_e_custom_", "").replace("_", " ")

            df["source_table"] = table_name
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

            # keep only rows that really look like events
            df = df[df["activity"].notna() & df["timestamp"].notna()].copy()

            if not df.empty:
                valid_df = df[[
                    "case_id", "activity", "timestamp",
                    "resource", "resource_role",
                    "document_number", "transaction_code",
                    "source_table"
                ]]
                frames.append(valid_df)
                tables_used.append(table_name)
                per_table_row_counts[table_name] = len(valid_df)

        if not frames:
            raise Exception("No event rows could be built from activity-like OCPM tables.")

        event_log = pd.concat(frames, ignore_index=True)

        # fallback case id
        event_log["case_id"] = event_log["case_id"].fillna(event_log["document_number"])
        event_log["case_id"] = event_log["case_id"].fillna(event_log["source_table"])

        event_log = event_log.drop_duplicates().sort_values(
            ["case_id", "timestamp", "activity"],
            na_position="last"
        ).reset_index(drop=True)

        logger.info(f"Final merged rows: {len(event_log)}")
        logger.info(f"Unique cases: {event_log['case_id'].nunique()}")
        logger.info(f"Source distribution:\n{event_log['source_table'].value_counts()}")

        return event_log
    def _validate_and_log_connection_config(self):
        token = settings.CELONIS_API_TOKEN or ""
        token_info = f"Exists (Starts with: {token[:5]}...)" if token else "Missing"
        
        logger.info(
            "Celonis Config Validation -> base_url=%s, key_type=%s, token=%s, pool_id=%s, model_id=%s",
            settings.CELONIS_BASE_URL,
            settings.CELONIS_KEY_TYPE,
            token_info,
            settings.CELONIS_DATA_POOL_ID,
            settings.CELONIS_DATA_MODEL_ID
        )

    def _connect(self):
        if not settings.CELONIS_BASE_URL or not settings.CELONIS_API_TOKEN:
            raise CelonisConnectionError(
                "CELONIS_BASE_URL and CELONIS_API_TOKEN are required in .env"
            )

        with CelonisService._shared_lock:
            if CelonisService._shared_initialized:
                self.celonis = CelonisService._shared_celonis
                self.data_pool = CelonisService._shared_data_pool
                self.data_model = CelonisService._shared_data_model
                return

        try:
            from pycelonis import get_celonis

            self._validate_and_log_connection_config()

            celonis = self._run_with_timeout(
                lambda: get_celonis(
                    base_url=settings.CELONIS_BASE_URL,
                    api_token=settings.CELONIS_API_TOKEN,
                    key_type=settings.CELONIS_KEY_TYPE,
                    permissions=False,   # ← skips _print_permissions() which requires admin scope
                    connect=True,
                ),
                timeout_seconds=max(int(getattr(settings, "CELONIS_CONNECT_TIMEOUT_SECONDS", 20) or 20), 5),
                label="Celonis SDK connection",
            )
            logger.info("Celonis SDK connection successful")
        except Exception as e:
            raise CelonisConnectionError(
                f"Failed to connect to Celonis: {str(e)}. "
                "Check CELONIS_BASE_URL, CELONIS_API_TOKEN, CELONIS_KEY_TYPE."
            )

        self.celonis = celonis
        self.data_model = self._get_data_model()

        with CelonisService._shared_lock:
            if not CelonisService._shared_initialized:
                CelonisService._shared_celonis = self.celonis
                CelonisService._shared_data_pool = self.data_pool
                CelonisService._shared_data_model = self.data_model
                CelonisService._shared_initialized = True

    def _get_data_model(self):
        try:
            if not settings.CELONIS_DATA_POOL_ID:
                raise CelonisConnectionError("CELONIS_DATA_POOL_ID is required in .env")
            if not settings.CELONIS_DATA_MODEL_ID:
                raise CelonisConnectionError("CELONIS_DATA_MODEL_ID is required in .env")

            timeout_seconds = max(int(getattr(settings, "CELONIS_CONNECT_TIMEOUT_SECONDS", 20) or 20), 5)
            self.data_pool = self._run_with_timeout(
                lambda: self.celonis.data_integration.get_data_pool(settings.CELONIS_DATA_POOL_ID),
                timeout_seconds=timeout_seconds,
                label="Celonis data pool lookup",
            )
            model = self._run_with_timeout(
                lambda: self.data_pool.get_data_model(settings.CELONIS_DATA_MODEL_ID),
                timeout_seconds=timeout_seconds,
                label="Celonis data model lookup",
            )
            logger.info("Loaded data model: %s", model.name)
            return model
        except Exception as e:
            error_msg = str(e)
            exc_class = e.__class__.__name__

            if "Permission" in exc_class or "permission" in error_msg.lower() or "403" in error_msg:
                if settings.CELONIS_KEY_TYPE == "APP_KEY" and "academic" in settings.CELONIS_BASE_URL.lower():
                    actionable_msg = "Wrong key type for academic account. Academic accounts require USER_KEY, but APP_KEY is configured."
                else:
                    actionable_msg = "Token is valid but lacks access to the configured data pool. Or, the configured pool/model is not accessible by this user."
                raise CelonisConnectionError(f"{actionable_msg} (Original Error: {exc_class})")

            raise CelonisConnectionError(f"Failed to load data pool/model: {error_msg}")

    @staticmethod
    def _run_with_timeout(fn, timeout_seconds: int, label: str):
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(fn)
        try:
            return future.result(timeout=timeout_seconds)
        except TimeoutError:
            future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise CelonisConnectionError(f"{label} timed out after {timeout_seconds}s")
        finally:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────────
    # PQL EXECUTION
    # ─────────────────────────────────────────────────────────────────────

    def _run_pql(self, query: Any, operation_name: str) -> pd.DataFrame:
        pql_timeout = max(int(getattr(settings, "CELONIS_PQL_TIMEOUT_SECONDS", 90) or 90), 10)
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="Deprecation", category=UserWarning)
                warnings.filterwarnings(
                    "ignore", category=UserWarning, module=r"pycelonis\.utils\.deprecation"
                )
                return self._run_with_timeout(
                    lambda: self.data_model.export_data_frame(query),
                    timeout_seconds=pql_timeout,
                    label=f"PQL export ({operation_name})",
                )
        except Exception as e:
            try:
                import pycelonis.pql as pql

                saola_df = self._run_with_timeout(
                    lambda: pql.DataFrame.from_pql(query, data_model=self.data_model),
                    timeout_seconds=pql_timeout,
                    label=f"PQL saola dataframe ({operation_name})",
                )
                return saola_df.to_pandas()
            except Exception as e2:
                logger.error("PQL query failed during %s", operation_name)
                raise Exception(
                    f"{operation_name} failed: legacy_export={str(e)} | saola={str(e2)}"
                )

    # ─────────────────────────────────────────────────────────────────────
    # TABLE / COLUMN DISCOVERY  (FIX 1: smarter caching + validation)
    # ─────────────────────────────────────────────────────────────────────

    def _get_table_names(self) -> List[str]:
        with CelonisService._shared_lock:
            if CelonisService._shared_table_names:
                return list(CelonisService._shared_table_names)
        timeout_seconds = max(int(getattr(settings, "CELONIS_CONNECT_TIMEOUT_SECONDS", 20) or 20), 5)
        names = self._run_with_timeout(
            lambda: [table.name for table in self.data_model.get_tables()],
            timeout_seconds=timeout_seconds,
            label="Celonis table name lookup",
        )
        logger.info("Discovered %d tables in data model: %s", len(names), names)
        with CelonisService._shared_lock:
            CelonisService._shared_table_names = list(names)
            CelonisService._shared_tables_loaded_at = time.time()
        return list(names)

    def _table_columns_safe(self, table_name: str) -> List[str]:
        try:
            with CelonisService._shared_lock:
                cached = CelonisService._shared_table_columns.get(table_name)
                if cached is not None:
                    return list(cached)

            timeout_seconds = max(int(getattr(settings, "CELONIS_CONNECT_TIMEOUT_SECONDS", 20) or 20), 5)
            tables = self._run_with_timeout(
                lambda: list(self.data_model.get_tables()),
                timeout_seconds=timeout_seconds,
                label="Celonis table lookup for columns",
            )
            match = next((table for table in tables if table.name == table_name), None)
            if match is None:
                return []
            cols = self._run_with_timeout(
                lambda: [col.name for col in match.get_columns()],
                timeout_seconds=timeout_seconds,
                label=f"Celonis column lookup ({table_name})",
            )
            logger.debug("Table %s columns: %s", table_name, cols)
            with CelonisService._shared_lock:
                CelonisService._shared_table_columns[table_name] = list(cols)
            return list(cols)
        except Exception:
            return []

    @staticmethod
    def _normalize_col_key(col_name: str) -> str:
        return "".join(ch for ch in str(col_name).upper() if ch.isalnum())

    def _find_col_by_aliases(self, table_columns: List[str], aliases: List[str]) -> Optional[str]:
        if not table_columns:
            return None
        lookup = {self._normalize_col_key(c): c for c in table_columns}
        for alias in aliases:
            key = self._normalize_col_key(alias)
            if key in lookup:
                return lookup[key]
        return None

    # ─────────────────────────────────────────────────────────────────────
    # EVENT SOURCE DISCOVERY  (FIX 2: logs every candidate + result)
    # ─────────────────────────────────────────────────────────────────────

    def _discover_event_source(self) -> Dict[str, Any]:
        # Return cached result if already discovered this session
        with CelonisService._shared_lock:
            if CelonisService._shared_event_source is not None:
                return dict(CelonisService._shared_event_source)

        aliases = {
            "case_id": [
                self.case_col,
                "CASEKEY", "_CASE_KEY", "CASE_KEY",
                "CASE_ID", "CASEID", "PROCESSINSTANCEID", "PROCESS_INSTANCE_ID",
                "BELNR", "EBELN", "VBELN", "AUFNR",
                "ID", "KEY", "INSTANCEID", "INSTANCE_ID",
            ],
            "activity": [
                self.activity_col,
                "ACTIVITYEN", "ACTIVITY_EN",
                "ACTIVITY", "ACTIVITY_NAME", "ACTIVITYNAME",
                "VORGN", "ACTIVITY_DE", "ACTIVITYDE",
                "TASKNAME", "TASK_NAME", "STEP", "STEPNAME", "EVENT",
                "EVENTNAME", "EVENT_NAME", "ACTION",
            ],
            "timestamp": [
                self.timestamp_col,
                "EVENTTIME", "EVENT_TIME",
                "ERDAT", "ERZEIT", "BUDAT", "BLDAT", "ZFBDT",
                "TIMESTAMP", "TIME", "DATETIME", "STARTTIME", "START_TIME",
                "ENDTIME", "END_TIME", "COMPLETIONTIME", "COMPLETION_TIME",
                "CREATEDAT", "CREATED_AT", "DATE",
            ],
            "resource": [
                self.resource_col,
                "USERNAME", "USER_NAME", "USER",
                "RESOURCE", "UNAME", "USNAM",
                "AGENT", "AGENTNAME", "PERFORMER",
            ],
            "resource_role": [
                self.resource_role_col,
                "USERTYPE", "USER_TYPE",
                "ROLE", "RESOURCE_ROLE",
                "CLASS", "ORGTYPE", "ORG_TYPE",
            ],
            "document_number": [
                self.document_col,
                "EBELN", "DOCUMENT_NUMBER", "DOCUMENTNUMBER",
                "PO_NUMBER", "PONUMBER", "BELNR", "VBELN",
            ],
            "transaction_code": [
                self.transaction_col,
                "TRANSACTIONCODE", "TRANSACTION_CODE",
                "TCODE", "T_CODE",
            ],
        }

        preferred = list(self.activity_tables) + [
            "o_custom_AccountingDocumentHeader",
            "o_custom_AccountingDocumentSegment",
            "o_custom_Invoice",
            "o_custom_VimHeader",
        ]
        # Deduplicate while preserving order
        seen: set = set()
        candidate_names: List[str] = []
        known_names = set(self._get_table_names())

        for name in preferred:
            if name in known_names and name not in seen:
                candidate_names.append(name)
                seen.add(name)

        logger.info(
            "Event source discovery — preferred candidates found in model: %s",
            candidate_names,
        )

        best: Optional[Dict[str, Any]] = None

        for table_name in candidate_names:
            cols = self._table_columns_safe(table_name)
            if not cols:
                logger.warning("Event source candidate %s — no columns returned, skipping.", table_name)
                continue

            mapping = {target: self._find_col_by_aliases(cols, a) for target, a in aliases.items()}
            mandatory = all(mapping.get(k) for k in ["case_id", "activity", "timestamp"])
            score = sum(1 for v in mapping.values() if v)
            logger.info(
                "Event source candidate %s: mandatory=%s score=%d mapping=%s",
                table_name, mandatory, score, mapping,
            )
            if not mandatory:
                continue

            candidate = {"table": table_name, "mapping": mapping, "score": score}
            if best is None or candidate["score"] > best["score"]:
                best = candidate

            

        # Global scan fallback
        if best is None:
            full_scan_limit = max(int(getattr(settings, "CELONIS_DISCOVERY_MAX_TABLES", 80) or 80), 1)
            ranked_names = sorted(
                known_names,
                key=lambda n: (
                    0 if str(n).startswith("t_o_custom_") else
                    1 if str(n).startswith("t_e_custom_") else 2
                ),
            )
            logger.info(
                "No preferred event table found; running global scan over %d tables.",
                min(len(ranked_names), full_scan_limit),
            )
            for table_name in ranked_names[:full_scan_limit]:
                if table_name in seen:
                    continue
                cols = self._table_columns_safe(table_name)
                if not cols:
                    continue
                mapping = {target: self._find_col_by_aliases(cols, a) for target, a in aliases.items()}
                mandatory = all(mapping.get(k) for k in ["case_id", "activity", "timestamp"])
                score = sum(1 for v in mapping.values() if v)
                if not mandatory:
                    continue
                candidate = {"table": table_name, "mapping": mapping, "score": score}
                logger.info("Global scan candidate %s: score=%d", table_name, score)
                if best is None or candidate["score"] > best["score"]:
                    best = candidate

        if best is None:
            raise Exception(
                "No suitable event table found with required columns (case/activity/timestamp). "
                f"Known tables: {sorted(known_names)}"
            )

        logger.info(
            "Event source resolved → table=%s score=%d mapping=%s",
            best["table"], best["score"], best["mapping"],
        )
        with CelonisService._shared_lock:
            CelonisService._shared_event_source = dict(best)
        return dict(best)

    # ─────────────────────────────────────────────────────────────────────
    # CASE ATTRIBUTE SOURCE DISCOVERY  (FIX 3: same logging pattern)
    # ─────────────────────────────────────────────────────────────────────

    def _discover_case_attr_source(self) -> Dict[str, Any]:
        with CelonisService._shared_lock:
            if CelonisService._shared_case_attr_source is not None:
                return dict(CelonisService._shared_case_attr_source)

        aliases = {
            "document_number": [
                self.case_table_doc_col,
                "EBELN", "DOCUMENT_NUMBER", "DOCUMENTNUMBER", "PO_NUMBER", "PONUMBER",
                "BELNR", "VBELN",
            ],
            "vendor_id": [
                self.vendor_col,
                "LIFNR", "VENDOR", "VENDOR_ID", "VENDORID",
                "LIEFERANT", "KRED",
            ],
            "payment_terms": [
                self.payment_terms_col,
                "ZTERM", "PAYMENT_TERMS", "PAYMENTTERMS",
                "ZAHLUNGSBEDINGUNG", "PTERMS",
            ],
            "currency": [
                self.currency_col,
                "WAERS", "CURRENCY", "CURR", "CURCODE",
            ],
        }
        preferred = [
            self.case_table,
            "t_o_custom_PurchasingDocumentHeader",
            "PurchasingDocumentHeader",
            "t_o_custom_VimHeader",
        ]
        seen: set = set()
        candidate_names: List[str] = []
        known_names = set(self._get_table_names())
        for name in preferred:
            if name in known_names and name not in seen:
                candidate_names.append(name)
                seen.add(name)

        logger.info("Case attr source discovery — candidates: %s", candidate_names)
        best: Optional[Dict[str, Any]] = None

        for table_name in candidate_names:
            cols = self._table_columns_safe(table_name)
            if not cols:
                continue
            mapping = {target: self._find_col_by_aliases(cols, a) for target, a in aliases.items()}
            mandatory = all(mapping.get(k) for k in ["document_number", "vendor_id"])
            score = sum(1 for v in mapping.values() if v)
            logger.info(
                "Case attr candidate %s: mandatory=%s score=%d mapping=%s",
                table_name, mandatory, score, mapping,
            )
            if not mandatory:
                continue
            candidate = {"table": table_name, "mapping": mapping, "score": score}
            if best is None or candidate["score"] > best["score"]:
                best = candidate
            if table_name == self.case_table and score >= 3:
                break

        if best is None:
            full_scan_limit = max(int(getattr(settings, "CELONIS_DISCOVERY_MAX_TABLES", 80) or 80), 1)
            ranked_names = sorted(
                known_names,
                key=lambda n: (
                    0 if str(n).startswith("t_o_custom_") else
                    1 if str(n).startswith("t_e_custom_") else 2
                ),
            )
            for table_name in ranked_names[:full_scan_limit]:
                if table_name in seen:
                    continue
                cols = self._table_columns_safe(table_name)
                if not cols:
                    continue
                mapping = {target: self._find_col_by_aliases(cols, a) for target, a in aliases.items()}
                mandatory = all(mapping.get(k) for k in ["document_number", "vendor_id"])
                score = sum(1 for v in mapping.values() if v)
                if not mandatory:
                    continue
                candidate = {"table": table_name, "mapping": mapping, "score": score}
                if best is None or candidate["score"] > best["score"]:
                    best = candidate

        if best is None:
            raise Exception(
                "No suitable case attribute table found with required columns "
                f"(document_number/vendor_id). Known tables: {sorted(known_names)}"
            )

        logger.info(
            "Case attr source resolved → table=%s score=%d mapping=%s",
            best["table"], best["score"], best["mapping"],
        )
        with CelonisService._shared_lock:
            CelonisService._shared_case_attr_source = dict(best)
        return dict(best)

    # ─────────────────────────────────────────────────────────────────────
    # TABLE NAME RESOLUTION
    # ─────────────────────────────────────────────────────────────────────

    def _resolve_table_name(self, configured_name: str) -> str:
        table_names = self._get_table_names()
        if configured_name in table_names:
            return configured_name

        lower_map = {name.lower(): name for name in table_names}
        if configured_name.lower() in lower_map:
            return lower_map[configured_name.lower()]

        normalized_candidates = {
            configured_name,
            configured_name[2:] if configured_name.startswith("t_") else configured_name,
            configured_name[2:] if configured_name.startswith("o_") else configured_name,
            f"t_{configured_name}" if not configured_name.startswith("t_") else configured_name,
            f"o_{configured_name}" if not configured_name.startswith("o_") else configured_name,
        }
        for candidate in normalized_candidates:
            if candidate in table_names:
                return candidate

        return configured_name

    def _normalize_configured_tables(self) -> None:
        table_names = set(self._get_table_names())

        resolved_activity = self._resolve_table_name(self.activity_table)
        if resolved_activity not in table_names:
            if not CelonisService._warned_missing_activity_table:
                logger.warning(
                    "Configured activity table %s resolved to %s but not present in model. "
                    "Will auto-discover at query time.",
                    self.activity_table,
                    resolved_activity,
                )
                CelonisService._warned_missing_activity_table = True
        elif resolved_activity != self.activity_table:
            logger.warning("Activity table %s not found, using %s", self.activity_table, resolved_activity)
            self.activity_table = resolved_activity

        resolved_case = self._resolve_table_name(self.case_table)
        if resolved_case not in table_names:
            if not CelonisService._warned_missing_case_table:
                logger.warning(
                    "Configured case table %s resolved to %s but not present in model. "
                    "Will auto-discover at query time.",
                    self.case_table,
                    resolved_case,
                )
                CelonisService._warned_missing_case_table = True
        elif resolved_case != self.case_table:
            logger.warning("Case table %s not found, using %s", self.case_table, resolved_case)
            self.case_table = resolved_case

    @staticmethod
    def _to_pql_table_name(table_name: str) -> str:
        return table_name[2:] if table_name.startswith("t_") else table_name

    # ─────────────────────────────────────────────────────────────────────
    # BATCH / ROW HELPERS
    # ─────────────────────────────────────────────────────────────────────

    def _effective_batch_size(self, requested_batch_size: Optional[int] = None) -> int:
        if requested_batch_size and requested_batch_size > 0:
            return int(requested_batch_size)
        configured = int(getattr(settings, "CELONIS_EXPORT_BATCH_SIZE", 5000) or 5000)
        return max(configured, 1000)

    def _effective_max_rows(self, requested_max_rows: Optional[int] = None) -> Optional[int]:
        if requested_max_rows is not None and requested_max_rows > 0:
            return int(requested_max_rows)
        configured = int(getattr(settings, "CELONIS_EXPORT_MAX_ROWS", 0) or 0)
        return int(configured) if configured > 0 else None

    # ─────────────────────────────────────────────────────────────────────
    # PAGINATED EXTRACTION  (FIX 4: validate row count after first page)
    # ─────────────────────────────────────────────────────────────────────

    def _extract_table_rows_paginated(
        self,
        table_name: str,
        columns: List[str],
        operation_name: str,
        batch_size: Optional[int] = None,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        try:
            from pycelonis.pql import PQL, PQLColumn
        except Exception as e:
            raise Exception(f"{operation_name} failed: unable to import pycelonis PQL ({str(e)})")

        if not columns:
            return pd.DataFrame()

        resolved_table = self._resolve_table_name(table_name)
        available_cols = set(self.list_columns(resolved_table))
        missing = [col for col in columns if col not in available_cols]
        if missing:
            raise Exception(
                f"{operation_name} failed: columns not found in '{resolved_table}': {missing}. "
                f"Available: {sorted(available_cols)}"
            )

        pql_table = self._to_pql_table_name(resolved_table)
        chunk_size = self._effective_batch_size(batch_size)
        row_cap = self._effective_max_rows(max_rows)

        frames: List[pd.DataFrame] = []
        offset = 0
        total_rows = 0
        page = 0
        last_page_signature = None

        while True:
            effective_limit = chunk_size
            if row_cap is not None:
                remaining = row_cap - total_rows
                if remaining <= 0:
                    break
                effective_limit = min(chunk_size, remaining)

            query = PQL(limit=effective_limit, offset=offset)
            for col in columns:
                query += PQLColumn(name=col, query=f'"{pql_table}"."{col}"')

            chunk = self._run_pql(
                query, f"{operation_name} (page={page}, offset={offset}, limit={effective_limit})"
            )
            if chunk is None or chunk.empty:
                break

            # ── FIX: log what we got from Celonis so we can verify it's real ──
            logger.info(
                "%s page=%d offset=%d → %d rows fetched (table=%s)",
                operation_name, page, offset, len(chunk), resolved_table,
            )
            if page == 0 and not chunk.empty:
                logger.debug(
                    "%s first-row sample: %s",
                    operation_name,
                    chunk.iloc[0].to_dict(),
                )

            signature = (
                tuple(chunk.iloc[0].astype(str).tolist()),
                tuple(chunk.iloc[-1].astype(str).tolist()),
                int(len(chunk)),
            )
            if last_page_signature is not None and signature == last_page_signature:
                logger.warning(
                    "Repeating page detected in %s from %s — stopping pagination.",
                    operation_name, resolved_table,
                )
                break
            last_page_signature = signature

            frames.append(chunk)
            fetched = int(len(chunk))
            total_rows += fetched
            offset += fetched
            page += 1

            if fetched > effective_limit:
                logger.warning(
                    "%s returned %s rows for limit=%s (table=%s). "
                    "Backend ignoring limit/offset — keeping first page only.",
                    operation_name, fetched, effective_limit, resolved_table,
                )
                break

            if row_cap is not None and total_rows >= row_cap:
                break
            if fetched < effective_limit:
                break

        if not frames:
            logger.warning("%s — no rows returned from %s", operation_name, resolved_table)
            return pd.DataFrame(columns=columns)

        result = pd.concat(frames, ignore_index=True)
        if row_cap is not None and len(result) > row_cap:
            result = result.iloc[:row_cap].copy()

        logger.info(
            "%s completed for %s: total=%d rows (batch=%d, max_rows=%s)",
            operation_name, resolved_table, len(result), chunk_size,
            row_cap if row_cap is not None else "unlimited",
        )
        return result
    
    # ─────────────────────────────────────────────────────────────────────
    # EVENT LOG  (FIX 5: assert real rows before caching)
    # ─────────────────────────────────────────────────────────────────────

    def _build_ocpm_event_log(self) -> pd.DataFrame:
        """
        Build an event log from ALL activity-like OCPM tables instead of one table.
        This is the main fix for the 140-events problem.
        """
        table_names = self._get_table_names()

        activity_aliases = [
            self.activity_col,
            "ACTIVITYEN", "ACTIVITY_EN",
            "ACTIVITY", "ACTIVITY_NAME", "ACTIVITYNAME",
            "VORGN", "TASKNAME", "TASK_NAME", "STEP", "STEPNAME",
            "EVENT", "EVENTNAME", "EVENT_NAME", "ACTION",
        ]
        timestamp_aliases = [
            self.timestamp_col,
            "EVENTTIME", "EVENT_TIME",
            "TIMESTAMP", "TIME", "DATETIME",
            "STARTTIME", "START_TIME",
            "ENDTIME", "END_TIME",
            "CREATEDAT", "CREATED_AT",
            "ERDAT", "ERZEIT", "BUDAT", "BLDAT", "ZFBDT", "DATE",
        ]
        case_aliases = [
            self.case_col,
            "CASEKEY", "_CASE_KEY", "CASE_KEY",
            "CASE_ID", "CASEID",
            "PROCESSINSTANCEID", "PROCESS_INSTANCE_ID",
            "BELNR", "EBELN", "VBELN", "AUFNR",
            "ACCOUNTINGDOCUMENTHEADER_ID",
            "PURCHASINGDOCUMENTHEADER_ID",
            "APINVOICE_ID",
            "VIMHEADER_ID",
            "ID",
        ]
        resource_aliases = [
            self.resource_col,
            "USERNAME", "USER_NAME", "USER",
            "RESOURCE", "UNAME", "USNAM",
            "AGENT", "AGENTNAME", "PERFORMER",
        ]
        role_aliases = [
            self.resource_role_col,
            "USERTYPE", "USER_TYPE",
            "ROLE", "RESOURCE_ROLE", "CLASS",
        ]
        document_aliases = [
            self.document_col,
            "EBELN", "DOCUMENT_NUMBER", "DOCUMENTNUMBER",
            "PO_NUMBER", "PONUMBER", "BELNR", "VBELN",
        ]
        tcode_aliases = [
            self.transaction_col,
            "TRANSACTIONCODE", "TRANSACTION_CODE", "TCODE", "T_CODE",
        ]

        candidate_tables = []
        for table_name in table_names:
            cols = self._table_columns_safe(table_name)
            if not cols:
                continue

            activity_col = self._find_col_by_aliases(cols, activity_aliases)
            timestamp_col = self._find_col_by_aliases(cols, timestamp_aliases)

            if activity_col and timestamp_col:
                candidate_tables.append({
                    "table": table_name,
                    "columns": cols,
                    "activity": activity_col,
                    "timestamp": timestamp_col,
                    "case_id": self._find_col_by_aliases(cols, case_aliases),
                    "resource": self._find_col_by_aliases(cols, resource_aliases),
                    "resource_role": self._find_col_by_aliases(cols, role_aliases),
                    "document_number": self._find_col_by_aliases(cols, document_aliases),
                    "transaction_code": self._find_col_by_aliases(cols, tcode_aliases),
                })
            elif table_name.startswith("t_e_custom_"):
                logger.info("🔥 USING EVENT TABLE: %s", table_name)
                candidate_tables.append({
                    "table": table_name,
                    "columns": cols,
                    "activity": None,
                    "timestamp": timestamp_col or self._find_col_by_aliases(cols, ["EVENTTIME", "CREATEDAT", "DATE", "TIME"]),
                    "case_id": self._find_col_by_aliases(cols, case_aliases),
                    "resource": self._find_col_by_aliases(cols, resource_aliases),
                    "resource_role": self._find_col_by_aliases(cols, role_aliases),
                    "document_number": self._find_col_by_aliases(cols, document_aliases),
                    "transaction_code": self._find_col_by_aliases(cols, tcode_aliases),
                })

        logger.info(
            "OCPM event log build — found %d activity-like tables: %s",
            len(candidate_tables),
            [c["table"] for c in candidate_tables],
        )

        if not candidate_tables:
            raise Exception("No activity-like tables found across the OCPM model.")

        frames = []
        for candidate in candidate_tables:
            table_name = candidate["table"]

            selected_cols = []
            for key in ["case_id", "activity", "timestamp", "resource", "resource_role", "document_number", "transaction_code"]:
                col = candidate.get(key)
                if col and col not in selected_cols:
                    selected_cols.append(col)

            # fallback: if case_id not found, try the first *_ID column
            if not candidate.get("case_id"):
                id_like = [c for c in candidate["columns"] if str(c).upper().endswith("_ID")]
                if id_like:
                    candidate["case_id"] = id_like[0]
                    if id_like[0] not in selected_cols:
                        selected_cols.append(id_like[0])

            if not selected_cols:
                continue

            logger.info("Extracting OCPM events from %s with columns=%s", table_name, selected_cols)

            raw_df = self._extract_table_rows_paginated(
                table_name=table_name,
                columns=selected_cols,
                operation_name="event log extraction"
            )

            if raw_df.empty:
                continue

            rename_map = {}
            for std_col in ["case_id", "activity", "timestamp", "resource", "resource_role", "document_number", "transaction_code"]:
                src = candidate.get(std_col)
                if src and src in raw_df.columns:
                    rename_map[src] = std_col

            df = raw_df.rename(columns=rename_map).copy()

            for col in ["case_id", "activity", "timestamp", "resource", "resource_role", "document_number", "transaction_code"]:
                if col not in df.columns:
                    df[col] = None

            if candidate.get("activity") is None:
                base_name = table_name.replace("t_e_custom_", "")
                readable_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', base_name)
                df["activity"] = readable_name

            df["source_table"] = table_name
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

            # keep only rows that really look like events
            df = df[df["activity"].notna() & df["timestamp"].notna()].copy()

            if not df.empty:
                frames.append(df[[
                    "case_id", "activity", "timestamp",
                    "resource", "resource_role",
                    "document_number", "transaction_code",
                    "source_table"
                ]])

        if not frames:
            raise Exception("No event rows could be built from activity-like OCPM tables.")

        event_log = pd.concat(frames, ignore_index=True)

        # fallback case id
        event_log["case_id"] = event_log["case_id"].fillna(event_log["document_number"])
        event_log["case_id"] = event_log["case_id"].fillna(event_log["source_table"])

        event_log = event_log.drop_duplicates().sort_values(
            ["case_id", "timestamp", "activity"],
            na_position="last"
        ).reset_index(drop=True)

        logger.info(
            "OCPM event log built successfully: %d rows, %d unique cases, %d unique activities",
            len(event_log),
            event_log["case_id"].nunique(),
            event_log["activity"].nunique(),
        )

        return event_log

    def get_event_log(self, use_cache: bool = True) -> pd.DataFrame:
        if use_cache and self._event_log_cache is not None:
            logger.info("Event Log Fetch Mode: CACHE USED. Total row count: %d", len(self._event_log_cache))
            return self._event_log_cache.copy()

        logger.info("Event Log Fetch Mode: FRESH FETCH (OCPM multi-table build)")
        try:
            df = self._build_ocpm_event_log()

            expected_cols = [
                "case_id", "activity", "timestamp",
                "resource", "resource_role",
                "document_number", "transaction_code",
            ]
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None

            logger.info("Total row count fetched: %d", len(df))
            if not df.empty and "timestamp" in df.columns:
                logger.info("Event Log Timestamps -> min: %s | max: %s", df["timestamp"].min(), df["timestamp"].max())
                logger.info("Sample format (first 5 rows): %s", df.head(5).to_dict(orient="records"))

            self._event_log_cache = df[expected_cols].copy()
            return self._event_log_cache.copy()

        except Exception as e:
            raise Exception(f"Event log extraction failed: {str(e)}")

    # ─────────────────────────────────────────────────────────────────────
    # CASE ATTRIBUTES
    # ─────────────────────────────────────────────────────────────────────

    def get_case_attributes(self) -> pd.DataFrame:
        if self._case_attributes_cache is not None:
            return self._case_attributes_cache.copy()

        try:
            source = self._discover_case_attr_source()
            source_table = source["table"]
            mapping = source["mapping"]
            source_columns = [col for col in mapping.values() if col]

            logger.info(
                "Extracting case attributes from table=%s columns=%s",
                source_table, source_columns,
            )

            raw_df = self._extract_table_rows_paginated(
                table_name=source_table,
                columns=source_columns,
                operation_name="case attributes extraction",
            )
            rename_map = {src: tgt for tgt, src in mapping.items() if src}
            df = raw_df.rename(columns=rename_map)

            if df.empty:
                logger.warning("Case attributes returned 0 rows from table %s.", source_table)
                return pd.DataFrame(columns=["document_number", "vendor_id", "payment_terms", "currency"])

            for col in ["document_number", "vendor_id", "payment_terms", "currency"]:
                if col not in df.columns:
                    df[col] = None

            df = df.drop_duplicates().reset_index(drop=True)
            logger.info(
                "Case attributes loaded: %d rows, %d unique vendors.",
                len(df), df["vendor_id"].nunique(),
            )
            self._case_attributes_cache = df
            return self._case_attributes_cache.copy()
        except Exception as e:
            raise Exception(f"Case attributes extraction failed: {str(e)}")

    # ─────────────────────────────────────────────────────────────────────
    # VENDOR MAPPING & ENRICHMENT
    # ─────────────────────────────────────────────────────────────────────

    def get_vendor_mapping(self) -> pd.DataFrame:
        if self._vendor_mapping_cache is not None:
            return self._vendor_mapping_cache.copy()

        try:
            events = self.get_event_log()[["case_id", "document_number"]].dropna().drop_duplicates()
            attrs = self.get_case_attributes()[["document_number", "vendor_id", "payment_terms", "currency"]]

            mapping = events.merge(attrs, on="document_number", how="left")
            mapping["vendor_id"] = mapping["vendor_id"].fillna("UNKNOWN")
            self._vendor_mapping_cache = mapping.reset_index(drop=True)
            return self._vendor_mapping_cache.copy()
        except Exception as e:
            raise Exception(f"Vendor mapping extraction failed: {str(e)}")

    def get_event_log_with_vendor(self) -> pd.DataFrame:
        if self._event_with_vendor_cache is not None:
            return self._event_with_vendor_cache.copy()

        try:
            events = self.get_event_log()
            vendor_map = self.get_vendor_mapping()[
                ["case_id", "document_number", "vendor_id", "payment_terms", "currency"]
            ]
            enriched = events.merge(vendor_map, on=["case_id", "document_number"], how="left")
            enriched["vendor_id"] = enriched["vendor_id"].fillna("UNKNOWN")
            self._event_with_vendor_cache = enriched
            return self._event_with_vendor_cache.copy()
        except Exception as e:
            raise Exception(f"Event log enrichment with vendor failed: {str(e)}")

    # ─────────────────────────────────────────────────────────────────────
    # GENERIC TABLE DATA
    # ─────────────────────────────────────────────────────────────────────

    def get_table_data(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        max_rows: Optional[int] = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        resolved_table = self._resolve_table_name(table_name)
        requested_cols = list(columns) if columns else self.list_columns(resolved_table)
        if not requested_cols:
            return pd.DataFrame()

        cache_key = (
            f"{resolved_table}|{','.join(requested_cols)}|"
            f"{self._effective_batch_size(batch_size)}|{self._effective_max_rows(max_rows)}"
        )
        if use_cache and cache_key in self._table_data_cache:
            return self._table_data_cache[cache_key].copy()

        df = self._extract_table_rows_paginated(
            table_name=resolved_table,
            columns=requested_cols,
            operation_name=f"table extraction ({resolved_table})",
            batch_size=batch_size,
            max_rows=max_rows,
        )

        if use_cache:
            self._table_data_cache[cache_key] = df.copy()
        return df

    # ─────────────────────────────────────────────────────────────────────
    # WORKING CAPITAL EXTRACTS
    # ─────────────────────────────────────────────────────────────────────

    def get_working_capital_extract(
        self,
        include_rows: bool = False,
        max_rows_per_table: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            tables = self.list_tables()
            candidate_names = sorted(
                {
                    t["table_name"]
                    for t in tables
                    if t["table_name"].startswith("t_o_custom_")
                    or t["table_name"].startswith("t_e_custom_")
                    or t["table_name"] in {self.activity_table, self.case_table}
                }
            )

            output: List[Dict[str, Any]] = []
            for table_name in candidate_names:
                columns = self.list_columns(table_name)
                table_df = self.get_table_data(
                    table_name=table_name,
                    columns=columns,
                    batch_size=batch_size,
                    max_rows=max_rows_per_table,
                    use_cache=False,
                )
                table_payload: Dict[str, Any] = {
                    "table_name": table_name,
                    "row_count": int(len(table_df)),
                    "column_count": int(len(columns)),
                    "columns": columns,
                }
                if include_rows:
                    table_payload["rows"] = table_df.where(pd.notnull(table_df), None).to_dict(orient="records")
                else:
                    table_payload["sample"] = (
                        table_df.head(20).where(pd.notnull(table_df), None).to_dict(orient="records")
                    )
                output.append(table_payload)

            return {
                "tables_extracted": len(output),
                "max_rows_per_table": self._effective_max_rows(max_rows_per_table),
                "batch_size": self._effective_batch_size(batch_size),
                "tables": output,
            }
        except Exception as e:
            raise Exception(f"Working Capital extraction failed: {str(e)}")

    @staticmethod
    def _table_is_activity_like(columns: List[str], activity_col: str, timestamp_col: str) -> bool:
        colset = set(columns)
        return activity_col in colset and timestamp_col in colset

    @staticmethod
    def _candidate_object_table_names(base_name: str) -> List[str]:
        normalized = base_name.strip()
        return [
            f"t_o_custom_{normalized}",
            f"t_o_{normalized}",
            normalized,
        ]

    @staticmethod
    def _normalize_table_filters(items: Optional[List[str]]) -> List[str]:
        if not items:
            return []
        out: List[str] = []
        for item in items:
            value = str(item or "").strip()
            if not value:
                continue
            if value not in out:
                out.append(value)
        return out

    def _infer_event_object_group(
        self,
        event_table_name: str,
        event_columns: List[str],
        object_table_names: set,
    ) -> str:
        for col in event_columns:
            if not col.endswith("_ID"):
                continue
            base_name = col[: -len("_ID")]
            for candidate in self._candidate_object_table_names(base_name):
                if candidate in object_table_names:
                    return candidate
        return "unlinked_events"

    def get_working_capital_grouped_extract(
        self,
        include_rows: bool = False,
        max_rows_per_table: Optional[int] = None,
        batch_size: Optional[int] = None,
        table_prefixes: Optional[List[str]] = None,
        table_allowlist: Optional[List[str]] = None,
        include_event_tables: bool = True,
        max_tables: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            all_tables = sorted([t["table_name"] for t in self.list_tables()])
            prefixes = self._normalize_table_filters(table_prefixes) or ["t_o_custom_", "t_e_custom_"]
            allowlist = self._normalize_table_filters(table_allowlist)
            allowset = set(allowlist)

            required_tables = {
                self.activity_table,
                self.case_table,
                (getattr(settings, "WCM_OLAP_SOURCE_TABLE", "") or "").strip(),
            }
            required_tables = {t for t in required_tables if t}

            candidate_tables = [t for t in all_tables if any(t.startswith(p) for p in prefixes)]
            if allowlist:
                candidate_tables = [t for t in candidate_tables if t in allowset or t in required_tables]

            for req in sorted(required_tables):
                if req in all_tables and req not in candidate_tables:
                    candidate_tables.append(req)

            if max_tables and max_tables > 0 and len(candidate_tables) > max_tables:
                prioritized = [t for t in candidate_tables if t in required_tables]
                remaining = [t for t in candidate_tables if t not in required_tables]
                candidate_tables = (prioritized + remaining)[:max_tables]

            object_tables = sorted([t for t in candidate_tables if t.startswith("t_o_custom_")])
            event_tables = sorted([
                t for t in candidate_tables
                if t.startswith("t_e_custom_") and include_event_tables
            ])
            object_table_set = set(object_tables)

            object_activity_streams: Dict[str, Dict[str, Any]] = {}
            object_master_tables: Dict[str, Dict[str, Any]] = {}
            event_streams_by_object: Dict[str, Dict[str, Any]] = {}

            def build_table_payload(table_name: str) -> Dict[str, Any]:
                columns = self.list_columns(table_name)
                sample_cap = max(int(getattr(settings, "WCM_GROUPED_SAMPLE_MAX_ROWS", 200) or 200), 20)
                if include_rows:
                    table_row_cap = max_rows_per_table
                else:
                    table_row_cap = min(max_rows_per_table or sample_cap, sample_cap)
                table_df = self.get_table_data(
                    table_name=table_name,
                    columns=columns,
                    batch_size=batch_size,
                    max_rows=table_row_cap,
                    use_cache=False,
                )
                payload: Dict[str, Any] = {
                    "table_name": table_name,
                    "row_count": int(len(table_df)),
                    "column_count": int(len(columns)),
                    "columns": columns,
                }
                if include_rows:
                    payload["rows"] = table_df.where(pd.notnull(table_df), None).to_dict(orient="records")
                else:
                    payload["sample"] = table_df.head(20).where(pd.notnull(table_df), None).to_dict(orient="records")
                return payload

            for table_name in object_tables:
                columns = self.list_columns(table_name)
                payload = build_table_payload(table_name)
                if self._table_is_activity_like(columns, self.activity_col, self.timestamp_col):
                    group_key = table_name.replace("t_o_custom_", "")
                    object_activity_streams[group_key] = {
                        "group_name": f"{group_key} Activity",
                        "group_type": "object_activity_stream",
                        "tables": [payload],
                        "total_rows": payload["row_count"],
                    }
                else:
                    group_key = table_name.replace("t_o_custom_", "")
                    object_master_tables[group_key] = {
                        "group_name": group_key,
                        "group_type": "object_master_table",
                        "tables": [payload],
                        "total_rows": payload["row_count"],
                    }

            for table_name in event_tables:
                columns = self.list_columns(table_name)
                payload = build_table_payload(table_name)
                linked_object = self._infer_event_object_group(table_name, columns, object_table_set)
                if linked_object == "unlinked_events":
                    group_key = "unlinked_events"
                    group_name = "Unlinked Event Streams"
                else:
                    group_key = linked_object.replace("t_o_custom_", "")
                    group_name = f"{group_key} Event Streams"

                if group_key not in event_streams_by_object:
                    event_streams_by_object[group_key] = {
                        "group_name": group_name,
                        "group_type": "event_stream_group",
                        "linked_object_table": linked_object,
                        "tables": [],
                        "total_rows": 0,
                    }
                event_streams_by_object[group_key]["tables"].append(payload)
                event_streams_by_object[group_key]["total_rows"] += payload["row_count"]

            grouped = {
                "object_activity_streams": list(object_activity_streams.values()),
                "event_streams_by_object": list(event_streams_by_object.values()),
                "object_master_tables": list(object_master_tables.values()),
            }
            total_group_count = sum(len(v) for v in grouped.values())
            total_tables = (
                sum(len(g["tables"]) for g in grouped["object_activity_streams"])
                + sum(len(g["tables"]) for g in grouped["event_streams_by_object"])
                + sum(len(g["tables"]) for g in grouped["object_master_tables"])
            )
            total_rows = (
                sum(g["total_rows"] for g in grouped["object_activity_streams"])
                + sum(g["total_rows"] for g in grouped["event_streams_by_object"])
                + sum(g["total_rows"] for g in grouped["object_master_tables"])
            )

            return {
                "group_count": int(total_group_count),
                "tables_extracted": int(total_tables),
                "total_rows_extracted": int(total_rows),
                "max_rows_per_table": self._effective_max_rows(max_rows_per_table),
                "batch_size": self._effective_batch_size(batch_size),
                "table_prefixes": prefixes,
                "table_allowlist": allowlist,
                "include_event_tables": bool(include_event_tables),
                "max_tables": max_tables,
                "selected_tables": candidate_tables,
                "groups": grouped,
            }
        except Exception as e:
            raise Exception(f"Working Capital grouped extraction failed: {str(e)}")

    # ─────────────────────────────────────────────────────────────────────
    # AMOUNT HELPERS
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _pick_best_amount_column(df: pd.DataFrame) -> Optional[str]:
        if df is None or df.empty:
            return None
        preferred = [
            "CONVERTED_INV_VALUE_USD", "CONVERTEDINVVALUEUSD",
            "INV_VALUE_USD", "INVOICE_VALUE_USD",
            "INV_VALUE", "INVOICE_VALUE",
            "WRBTR", "DMBTR", "NETWR",
        ]
        upper_map = {str(c).upper(): c for c in df.columns}
        for key in preferred:
            if key in upper_map:
                return upper_map[key]

        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        rank_keywords = ["VALUE", "AMOUNT", "USD", "INV", "NET", "TOTAL"]
        ranked = []
        for col in numeric_cols:
            score = sum(1 for kw in rank_keywords if kw in str(col).upper())
            ranked.append((score, col))
        ranked.sort(reverse=True, key=lambda x: x[0])
        return ranked[0][1] if ranked and ranked[0][0] > 0 else None

    @staticmethod
    def _find_amount_like_columns(df: pd.DataFrame) -> List[str]:
        if df is None or df.empty:
            return []
        amount_keywords = [
            "VALUE", "AMOUNT", "USD", "INV", "NET", "TOTAL",
            "WRBTR", "DMBTR", "NETWR", "MENGE", "PRICE", "COST",
        ]
        result: List[str] = []
        for col in df.columns:
            upper = str(col).upper()
            if any(k in upper for k in amount_keywords):
                result.append(str(col))
        return result

    def _build_amount_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        if df is None or df.empty:
            return {
                "amount_column_detected": None,
                "amount_columns": [],
                "totals_by_column": {},
                "non_null_count_by_column": {},
            }

        amount_columns = self._find_amount_like_columns(df)
        totals_by_column: Dict[str, float] = {}
        non_null_count_by_column: Dict[str, int] = {}

        for col in amount_columns:
            series = pd.to_numeric(df[col], errors="coerce")
            non_null_count_by_column[col] = int(series.notna().sum())
            if series.notna().any():
                total = series.fillna(0).sum()
                totals_by_column[col] = float(total) if math.isfinite(float(total)) else 0.0

        preferred = self._pick_best_amount_column(df[amount_columns]) if amount_columns else None
        return {
            "amount_column_detected": preferred,
            "amount_columns": amount_columns,
            "totals_by_column": totals_by_column,
            "non_null_count_by_column": non_null_count_by_column,
        }

    # ─────────────────────────────────────────────────────────────────────
    # TABLE EXTRACT PAYLOADS
    # ─────────────────────────────────────────────────────────────────────

    def get_table_extract_payload(
        self,
        table_name: str,
        include_rows: bool = False,
        max_rows: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        resolved = self._resolve_table_name(table_name)
        columns = self.list_columns(resolved)
        if not columns:
            raise Exception(f"Table not found or has no columns: {table_name}")

        df = self.get_table_data(
            table_name=resolved,
            columns=columns,
            batch_size=batch_size,
            max_rows=max_rows,
            use_cache=False,
        )
        payload: Dict[str, Any] = {
            "table_name": resolved,
            "row_count": int(len(df)),
            "column_count": int(len(columns)),
            "columns": columns,
            "amount_summary": self._build_amount_summary(df),
            "sample": df.head(20).where(pd.notnull(df), None).to_dict(orient="records"),
        }
        if include_rows:
            payload["rows"] = df.where(pd.notnull(df), None).to_dict(orient="records")
        return self._json_safe(payload)

    def get_all_tables_extract(
        self,
        include_rows: bool = False,
        max_rows_per_table: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            all_table_names = sorted([t["table_name"] for t in self.list_tables()])
            extracted: List[Dict[str, Any]] = []
            total_rows = 0

            for table_name in all_table_names:
                payload = self.get_table_extract_payload(
                    table_name=table_name,
                    include_rows=include_rows,
                    max_rows=max_rows_per_table,
                    batch_size=batch_size,
                )
                total_rows += int(payload["row_count"])
                extracted.append(payload)

            return {
                "tables_extracted": int(len(extracted)),
                "total_rows_extracted": int(total_rows),
                "max_rows_per_table": self._effective_max_rows(max_rows_per_table),
                "batch_size": self._effective_batch_size(batch_size),
                "tables": extracted,
            }
        except Exception as e:
            raise Exception(f"All-table extraction failed: {str(e)}")

    def get_all_tables_grouped_extract(
        self,
        include_rows: bool = False,
        max_rows_per_table: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            all_tables = sorted([t["table_name"] for t in self.list_tables()])
            groups: Dict[str, Dict[str, Any]] = {
                "object_tables": {
                    "group_name": "Object Tables",
                    "group_type": "object_tables",
                    "tables": [],
                    "total_rows": 0,
                },
                "event_tables": {
                    "group_name": "Event Tables",
                    "group_type": "event_tables",
                    "tables": [],
                    "total_rows": 0,
                },
                "other_tables": {
                    "group_name": "Other Tables",
                    "group_type": "other_tables",
                    "tables": [],
                    "total_rows": 0,
                },
            }

            for table_name in all_tables:
                payload = self.get_table_extract_payload(
                    table_name=table_name,
                    include_rows=include_rows,
                    max_rows=max_rows_per_table,
                    batch_size=batch_size,
                )
                if table_name.startswith("t_o_"):
                    key = "object_tables"
                elif table_name.startswith("t_e_"):
                    key = "event_tables"
                else:
                    key = "other_tables"

                groups[key]["tables"].append(payload)
                groups[key]["total_rows"] += int(payload["row_count"])

            grouped_list = [groups["object_tables"], groups["event_tables"], groups["other_tables"]]
            total_rows = sum(g["total_rows"] for g in grouped_list)
            total_tables = sum(len(g["tables"]) for g in grouped_list)

            return {
                "group_count": int(len(grouped_list)),
                "tables_extracted": int(total_tables),
                "total_rows_extracted": int(total_rows),
                "max_rows_per_table": self._effective_max_rows(max_rows_per_table),
                "batch_size": self._effective_batch_size(batch_size),
                "groups": grouped_list,
            }
        except Exception as e:
            raise Exception(f"All-table grouped extraction failed: {str(e)}")

    # ─────────────────────────────────────────────────────────────────────
    # OLAP HELPERS
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_col(col_name: str) -> str:
        return "".join(ch for ch in str(col_name).upper() if ch.isalnum())

    def _find_matching_column(self, columns: List[str], aliases: List[str]) -> Optional[str]:
        if not columns:
            return None
        normalized_map = {self._normalize_col(c): c for c in columns}
        for alias in aliases:
            n_alias = self._normalize_col(alias)
            if n_alias in normalized_map:
                return normalized_map[n_alias]
        return None

    def _score_table_for_olap(self, columns: List[str], required_aliases: Dict[str, List[str]]) -> int:
        score = 0
        for aliases in required_aliases.values():
            if self._find_matching_column(columns, aliases):
                score += 1
        return score

    def _discover_best_olap_table(self, required_aliases: Dict[str, List[str]]) -> Dict[str, Any]:
        candidates = [t["table_name"] for t in self.list_tables()]
        ranked: List[Dict[str, Any]] = []
        for table_name in candidates:
            try:
                cols = self.list_columns(table_name)
                score = self._score_table_for_olap(cols, required_aliases)
                if score > 0:
                    ranked.append({"table_name": table_name, "score": score, "columns": cols})
            except Exception:
                continue

        if not ranked:
            raise Exception("No table with Detailed Transaction OLAP-like columns was found in the data model.")

        ranked.sort(
            key=lambda x: (
                x["score"],
                1 if "APBSEGOPEN" in x["table_name"].upper() else 0,
                1 if "ACCOUNTINGDOCUMENTSEGMENT" in x["table_name"].upper() else 0,
                len(x["columns"]),
            ),
            reverse=True,
        )
        logger.info(
            "OLAP table discovery top candidates: %s",
            [(r["table_name"], r["score"]) for r in ranked[:5]],
        )
        return ranked[0]

    def get_detailed_transaction_olap(
        self,
        include_rows: bool = False,
        max_rows: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            field_aliases: Dict[str, List[str]] = {
                "company_code": ["Company Code", "BUKRS", "COMPANYCODE"],
                "supplier_type": ["Supplier Type", "SUPPLIERTYPE", "VENDOR_TYPE"],
                "vendor_id": ["LIFNR", "Vendor ID", "VENDOR", "VENDORID"],
                "supplier_name": ["Supplier Name", "NAME1", "VENDORNAME", "SUPPLIERNAME"],
                "invoice_number": ["Inv Number", "Invoice Number", "BELNR", "INVOICENUMBER", "VBELN"],
                "invoice_line_item_number": ["Inv Line Item Number", "BUZEI", "LINEITEM", "ITEMNUMBER"],
                "invoice_value_usd": [
                    "Inv. value (in USD)", "Inv value in USD", "INV_VALUE_USD",
                    "INVOICE_VALUE_USD", "DMBTR", "WRBTR", "NETWR",
                ],
                "currency": ["Currency", "WAERS"],
                "converted_invoice_value_usd": [
                    "Converted Inv Value", "CONVERTED_INV_VALUE_USD",
                    "CONVERTEDINVVALUEUSD", "INVOICE_VALUE_CONVERTED",
                ],
                "fiscal_year": ["Fiscal Year", "GJAHR", "FISCALYEAR"],
                "clearing_document_number": [
                    "Clearing Document", "AUGBL", "CLEARINGDOC", "CLEARINGDOCUMENTNUMBER",
                ],
                "payment_status": ["Payment Status", "PAYMENT_STATUS", "RECOMMENDATION", "STATUS"],
                "invoice_payment_terms": ["Invoice PT", "INVOICE_PT", "INVOICE_PAYMENT_TERMS", "ZTERM_INV"],
                "po_payment_terms": ["PO Payment Term", "PO_PAYMENT_TERM", "PO_PAYMENT_TERMS", "ZTERM_PO"],
                "vendor_master_payment_terms": [
                    "Vendor Master PT", "VENDOR_MASTER_PT", "VENDOR_PAYMENT_TERMS", "ZTERM_VENDOR",
                ],
                "recommendation": ["Recommendation", "RECOMMENDATION_TEXT", "RECOMMENDED_ACTION"],
                "due_date": ["Due Date", "DUE_DATE", "FAEDT", "NETDT"],
                "baseline_date": ["Baseline Date", "BASELINE_DATE", "ZFBDT"],
                "posting_date": ["Posting Date", "POSTING_DATE", "BUDAT"],
                "cleared_date": ["Cleared Date", "CLEARED_DATE", "AUGDT"],
            }

            explicit_mapping = self._get_explicit_olap_mapping()
            for target_field, explicit_col in explicit_mapping.items():
                if explicit_col:
                    field_aliases[target_field] = [explicit_col] + field_aliases.get(target_field, [])

            explicit_table = (getattr(settings, "WCM_OLAP_SOURCE_TABLE", "") or "").strip()
            if explicit_table:
                source_table = self._resolve_table_name(explicit_table)
                source_columns = self.list_columns(source_table)
                best = {
                    "table_name": source_table,
                    "score": self._score_table_for_olap(source_columns, field_aliases),
                    "columns": source_columns,
                    "selection_mode": "explicit",
                }
            else:
                best = self._discover_best_olap_table(field_aliases)
                best["selection_mode"] = "auto"

            source_table = best["table_name"]
            source_columns = best["columns"]
            logger.info(
                "OLAP source resolved → table=%s score=%d mode=%s",
                source_table, best["score"], best["selection_mode"],
            )

            selected_source_cols: List[str] = []
            selected_map: Dict[str, Optional[str]] = {}
            for target_field, aliases in field_aliases.items():
                matched = self._find_matching_column(source_columns, aliases)
                selected_map[target_field] = matched
                if matched and matched not in selected_source_cols:
                    selected_source_cols.append(matched)

            if not selected_source_cols:
                raise Exception(
                    f"Could not map any OLAP columns from table '{source_table}'. "
                    f"Available columns: {source_columns}"
                )

            logger.info(
                "OLAP column mapping: %s",
                {k: v for k, v in selected_map.items() if v},
            )

            raw = self.get_table_data(
                table_name=source_table,
                columns=selected_source_cols,
                batch_size=batch_size,
                max_rows=max_rows,
                use_cache=False,
            )

            logger.info(
                "OLAP raw data: %d rows from %s", len(raw), source_table
            )

            olap = pd.DataFrame(index=raw.index)
            for target_field, source_col in selected_map.items():
                if source_col and source_col in raw.columns:
                    olap[target_field] = raw[source_col]
                else:
                    olap[target_field] = None

            if "invoice_value_usd" in olap.columns:
                olap["invoice_value_usd"] = pd.to_numeric(olap["invoice_value_usd"], errors="coerce")
            if "converted_invoice_value_usd" in olap.columns:
                olap["converted_invoice_value_usd"] = pd.to_numeric(
                    olap["converted_invoice_value_usd"], errors="coerce"
                )

            payload: Dict[str, Any] = {
                "source_table": source_table,
                "source_score": best["score"],
                "selection_mode": best.get("selection_mode", "auto"),
                "source_column_mapping": selected_map,
                "source_column_mapping_missing": [k for k, v in selected_map.items() if not v],
                "explicit_mapping_used": {k: v for k, v in explicit_mapping.items() if v},
                "row_count": int(len(olap)),
                "columns": list(olap.columns),
                "sample": self._df_to_json_records(olap.head(25)),
            }
            if include_rows:
                payload["rows"] = self._df_to_json_records(olap)

            return self._json_safe(payload)
        except Exception as e:
            raise Exception(f"Detailed Transaction OLAP extraction failed: {str(e)}")

    @staticmethod
    def _get_explicit_olap_mapping() -> Dict[str, str]:
        return {
            "company_code": getattr(settings, "WCM_OLAP_COL_COMPANY_CODE", ""),
            "supplier_type": getattr(settings, "WCM_OLAP_COL_SUPPLIER_TYPE", ""),
            "vendor_id": getattr(settings, "WCM_OLAP_COL_VENDOR_ID", ""),
            "supplier_name": getattr(settings, "WCM_OLAP_COL_SUPPLIER_NAME", ""),
            "invoice_number": getattr(settings, "WCM_OLAP_COL_INVOICE_NUMBER", ""),
            "invoice_line_item_number": getattr(settings, "WCM_OLAP_COL_LINE_ITEM", ""),
            "invoice_value_usd": getattr(settings, "WCM_OLAP_COL_INVOICE_VALUE_USD", ""),
            "currency": getattr(settings, "WCM_OLAP_COL_CURRENCY", ""),
            "converted_invoice_value_usd": getattr(settings, "WCM_OLAP_COL_CONVERTED_VALUE_USD", ""),
            "fiscal_year": getattr(settings, "WCM_OLAP_COL_FISCAL_YEAR", ""),
            "clearing_document_number": getattr(settings, "WCM_OLAP_COL_CLEARING_DOC", ""),
            "payment_status": getattr(settings, "WCM_OLAP_COL_PAYMENT_STATUS", ""),
            "invoice_payment_terms": getattr(settings, "WCM_OLAP_COL_INVOICE_PT", ""),
            "po_payment_terms": getattr(settings, "WCM_OLAP_COL_PO_PT", ""),
            "vendor_master_payment_terms": getattr(settings, "WCM_OLAP_COL_VENDOR_PT", ""),
            "recommendation": getattr(settings, "WCM_OLAP_COL_RECOMMENDATION", ""),
            "due_date": getattr(settings, "WCM_OLAP_COL_DUE_DATE", ""),
            "baseline_date": getattr(settings, "WCM_OLAP_COL_BASELINE_DATE", ""),
            "posting_date": getattr(settings, "WCM_OLAP_COL_POSTING_DATE", ""),
            "cleared_date": getattr(settings, "WCM_OLAP_COL_CLEARED_DATE", ""),
        }

    @staticmethod
    def _df_to_json_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df is None or df.empty:
            return []
        safe = df.copy()
        for col in safe.columns:
            if pd.api.types.is_datetime64_any_dtype(safe[col]):
                safe[col] = safe[col].apply(
                    lambda value: value.isoformat() if pd.notna(value) else None
                )
            else:
                safe[col] = safe[col].where(pd.notnull(safe[col]), None)
        return safe.to_dict(orient="records")

    @classmethod
    def _json_safe(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: cls._json_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._json_safe(item) for item in value]
        if isinstance(value, tuple):
            return [cls._json_safe(item) for item in value]
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if pd.isna(value):
            return None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value

    # ─────────────────────────────────────────────────────────────────────
    # VENDOR STATISTICS
    # ─────────────────────────────────────────────────────────────────────

    def get_vendor_statistics(self) -> pd.DataFrame:
        if self._vendor_stats_cache is not None:
            return self._vendor_stats_cache.copy()

        try:
            df = self.get_event_log_with_vendor()
            if df.empty:
                return pd.DataFrame(columns=["vendor_id", "case_count", "exception_rate_pct"])

            df = df.copy()
            activity_lower = df["activity"].astype(str).str.lower()
            df["_is_exception"] = activity_lower.str.contains(
                "exception|moved out|due date passed|block|payment term",
                regex=True,
                na=False,
            )
            case_vendor = (
                df.groupby("case_id", sort=False)
                .agg(
                    vendor_id=("vendor_id", "first"),
                    is_exception=("_is_exception", "any"),
                )
                .reset_index()
            )
            stats = (
                case_vendor.groupby("vendor_id")
                .agg(
                    case_count=("case_id", "count"),
                    exception_case_count=("is_exception", "sum"),
                )
                .reset_index()
            )
            stats["exception_rate_pct"] = (
                stats["exception_case_count"]
                / stats["case_count"].replace(0, pd.NA)
                * 100
            ).fillna(0.0).round(2)
            self._vendor_stats_cache = stats
            return self._vendor_stats_cache.copy()
        except Exception as e:
            raise Exception(f"Vendor statistics computation failed: {str(e)}")

    def get_vendor_stats_api(self) -> List[Dict[str, Any]]:
        try:
            stats_df = self.get_vendor_statistics()
            case_durations = self.get_case_durations()
            vendor_map = self.get_vendor_mapping()[["case_id", "vendor_id"]].drop_duplicates()

            case_table_df = self.get_table_data(self.case_table)
            total_value_by_vendor: Dict[str, float] = {}
            vendor_lifnr_by_vendor: Dict[str, str] = {}
            if not case_table_df.empty:
                renamed = case_table_df.rename(columns={self.vendor_col: "vendor_id"})
                if "vendor_id" in renamed.columns:
                    renamed["vendor_id"] = renamed["vendor_id"].astype(str).str.strip()
                    vendor_lifnr_by_vendor = (
                        renamed.groupby("vendor_id")["vendor_id"].first().astype(str).to_dict()
                    )
                    amount_col = self._pick_best_amount_column(renamed)
                    if amount_col:
                        work = renamed[["vendor_id", amount_col]].copy()
                        work[amount_col] = pd.to_numeric(work[amount_col], errors="coerce").fillna(0.0)
                        total_value_by_vendor = (
                            work.groupby("vendor_id")[amount_col].sum().round(2).to_dict()
                        )

            avg_dpo_by_vendor = {}
            if not case_durations.empty and not vendor_map.empty:
                duration_map = case_durations.merge(vendor_map, on="case_id", how="left")
                avg_dpo_by_vendor = (
                    duration_map.groupby("vendor_id")["duration_days"].mean().round(2).to_dict()
                )

            rows: List[Dict[str, Any]] = []
            if not stats_df.empty:
                for _, row in stats_df.iterrows():
                    vendor_id = str(row.get("vendor_id", "UNKNOWN"))
                    exception_rate = float(row.get("exception_rate_pct", 0) or 0)
                    avg_dpo = float(avg_dpo_by_vendor.get(vendor_id, 0) or 0)

                    risk = "LOW"
                    if exception_rate >= 60 or avg_dpo >= 60:
                        risk = "CRITICAL"
                    elif exception_rate >= 40 or avg_dpo >= 40:
                        risk = "HIGH"
                    elif exception_rate >= 20 or avg_dpo >= 20:
                        risk = "MEDIUM"

                    rows.append({
                        "vendor_id": vendor_id,
                        "vendor_lifnr": vendor_lifnr_by_vendor.get(vendor_id, vendor_id),
                        "total_cases": int(row.get("case_count", 0) or 0),
                        "total_value": float(total_value_by_vendor.get(vendor_id, 0.0)),
                        "exception_rate": round(exception_rate, 2),
                        "avg_dpo": round(avg_dpo, 2),
                        "payment_behavior": {
                            "on_time_pct": None,
                            "early_pct": None,
                            "late_pct": None,
                            "open_pct": None,
                            "_source": "celonis_direct",
                        },
                        "risk_score": risk,
                    })

            rows.sort(key=lambda x: (x["total_value"], x["total_cases"]), reverse=True)
            return rows
        except Exception as e:
            raise Exception(f"Vendor stats API payload generation failed: {str(e)}")

    def get_vendor_paths(self, vendor_id: str) -> Dict[str, Any]:
        try:
            df = self.get_event_log_with_vendor()
            if df.empty:
                return {"vendor_id": vendor_id, "happy_paths": [], "exception_paths": []}

            normalized_vendor = df["vendor_id"].astype(str).str.upper().str.strip()
            vendor_df = df[normalized_vendor == vendor_id.upper().strip()].copy()
            if vendor_df.empty:
                return {"vendor_id": vendor_id, "happy_paths": [], "exception_paths": []}

            def classify_exception_type(variant: str) -> str:
                v = variant.lower()
                if "payment terms" in v:
                    return "Payment Terms Mismatch"
                if "exception" in v:
                    return "Invoice Exception"
                if "short" in v or "immediate" in v or "0-day" in v:
                    return "Short Payment Terms"
                if "early" in v or "due date passed" in v:
                    return "Early Payment"
                if "moved out" in v:
                    return "Invoice Exception"
                if "block" in v:
                    return "Invoice Exception"
                return ""

            def aggregate_variants(frame: pd.DataFrame) -> pd.DataFrame:
                work = frame.sort_values(["case_id", "timestamp"]).reset_index(drop=True)
                variant_by_case = (
                    work.groupby("case_id")["activity"]
                    .apply(lambda x: " → ".join(x.astype(str).tolist()))
                    .reset_index(name="variant")
                )
                case_duration = (
                    work.groupby("case_id")
                    .agg(start_time=("timestamp", "min"), end_time=("timestamp", "max"))
                    .reset_index()
                )
                case_duration["duration_days"] = (
                    (case_duration["end_time"] - case_duration["start_time"]).dt.total_seconds() / 86400
                ).fillna(0)
                variant_with_duration = variant_by_case.merge(
                    case_duration[["case_id", "duration_days"]], on="case_id", how="left"
                )
                agg = (
                    variant_with_duration.groupby("variant")
                    .agg(
                        frequency=("case_id", "nunique"),
                        avg_duration_days=("duration_days", "mean"),
                    )
                    .reset_index()
                )
                total = max(int(agg["frequency"].sum()), 1)
                agg["percentage"] = (agg["frequency"] / total * 100).round(2)
                agg["avg_duration_days"] = agg["avg_duration_days"].round(2)
                return agg.sort_values("frequency", ascending=False).reset_index(drop=True)

            def partition_paths(agg: pd.DataFrame, source: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
                happy: List[Dict[str, Any]] = []
                exception: List[Dict[str, Any]] = []
                for _, row in agg.iterrows():
                    record = {
                        "variant": row["variant"],
                        "frequency": int(row["frequency"]),
                        "percentage": float(row["percentage"]),
                        "avg_duration_days": float(row["avg_duration_days"]),
                        "source": source,
                    }
                    exception_type = classify_exception_type(str(row["variant"]))
                    variant_lower = str(row["variant"]).lower()
                    has_exception_signal = any(
                        kw in variant_lower
                        for kw in [
                            "exception", "due date passed", "block", "moved out",
                            "short payment", "immediate", "early payment",
                        ]
                    )
                    if has_exception_signal:
                        record["exception_type"] = exception_type or "Invoice Exception"
                        exception.append(record)
                    else:
                        happy.append(record)
                return happy, exception

            vendor_agg = aggregate_variants(vendor_df)
            happy_paths, exception_paths = partition_paths(vendor_agg, "vendor")

            if not happy_paths:
                global_agg = aggregate_variants(df)
                global_happy_paths, _ = partition_paths(global_agg, "global_baseline")
                if global_happy_paths:
                    happy_paths = global_happy_paths[:3]

            return {
                "vendor_id": vendor_id,
                "happy_paths": happy_paths,
                "exception_paths": exception_paths,
            }
        except Exception as e:
            raise Exception(f"Vendor paths extraction failed for vendor '{vendor_id}': {str(e)}")

    # ─────────────────────────────────────────────────────────────────────
    # VARIANTS / THROUGHPUT / FREQUENCIES
    # ─────────────────────────────────────────────────────────────────────

    def get_variants(self) -> pd.DataFrame:
        if self._variants_cache is not None:
            return self._variants_cache.copy()

        try:
            df = self.get_event_log()
            if df.empty:
                return pd.DataFrame(columns=["variant", "frequency", "percentage"])

            variants = (
                df.sort_values(["case_id", "timestamp"])
                .groupby("case_id")["activity"]
                .apply(lambda x: " → ".join(x.astype(str).tolist()))
                .reset_index(name="variant")
            )
            result = (
                variants.groupby("variant")
                .size()
                .reset_index(name="frequency")
                .sort_values("frequency", ascending=False)
            )
            total = result["frequency"].sum()
            result["percentage"] = (result["frequency"] / total * 100).round(2)
            self._variants_cache = result
            return self._variants_cache.copy()
        except Exception as e:
            raise Exception(f"Variant extraction failed: {str(e)}")

    def get_throughput_times(self) -> pd.DataFrame:
        if self._throughput_cache is not None:
            return self._throughput_cache.copy()

        try:
            df = self.get_event_log()
            if df.empty:
                return pd.DataFrame(
                    columns=[
                        "source_activity", "target_activity",
                        "avg_duration_days", "median_duration_days", "case_count",
                    ]
                )

            transitions = []
            for case_id, group in df.groupby("case_id"):
                group = group.sort_values("timestamp").reset_index(drop=True)
                for i in range(len(group) - 1):
                    source_activity = group.loc[i, "activity"]
                    target_activity = group.loc[i + 1, "activity"]
                    source_time = group.loc[i, "timestamp"]
                    target_time = group.loc[i + 1, "timestamp"]
                    duration_days = None
                    if pd.notnull(source_time) and pd.notnull(target_time):
                        duration_days = (target_time - source_time).total_seconds() / 86400
                    transitions.append({
                        "case_id": case_id,
                        "source_activity": source_activity,
                        "target_activity": target_activity,
                        "duration_days": duration_days,
                    })

            trans_df = pd.DataFrame(transitions)
            if trans_df.empty:
                return pd.DataFrame(
                    columns=[
                        "source_activity", "target_activity",
                        "avg_duration_days", "median_duration_days", "case_count",
                    ]
                )

            result = (
                trans_df.groupby(["source_activity", "target_activity"])
                .agg(
                    avg_duration_days=("duration_days", "mean"),
                    median_duration_days=("duration_days", "median"),
                    case_count=("case_id", "count"),
                )
                .reset_index()
            )
            result["avg_duration_days"] = result["avg_duration_days"].round(2)
            result["median_duration_days"] = result["median_duration_days"].round(2)
            result = result.sort_values("case_count", ascending=False).reset_index(drop=True)
            self._throughput_cache = result
            return self._throughput_cache.copy()
        except Exception as e:
            raise Exception(f"Throughput extraction failed: {str(e)}")

    def get_activity_frequencies(self) -> pd.DataFrame:
        if self._activity_freq_cache is not None:
            return self._activity_freq_cache.copy()
        try:
            df = self.get_event_log()
            if df.empty:
                return pd.DataFrame(columns=["activity", "frequency"])
            result = (
                df.groupby("activity")
                .size()
                .reset_index(name="frequency")
                .sort_values("frequency", ascending=False)
            )
            self._activity_freq_cache = result
            return self._activity_freq_cache.copy()
        except Exception as e:
            raise Exception(f"Activity frequency extraction failed: {str(e)}")

    def get_resource_activity_mapping(self) -> pd.DataFrame:
        if self._resource_mapping_cache is not None:
            return self._resource_mapping_cache.copy()
        try:
            df = self.get_event_log()
            if df.empty:
                return pd.DataFrame(columns=["activity", "resource_role", "frequency"])
            result = (
                df.groupby(["activity", "resource_role"])
                .size()
                .reset_index(name="frequency")
                .sort_values("frequency", ascending=False)
            )
            self._resource_mapping_cache = result
            return self._resource_mapping_cache.copy()
        except Exception as e:
            raise Exception(f"Resource-role mapping extraction failed: {str(e)}")

    def get_case_durations(self) -> pd.DataFrame:
        if self._case_durations_cache is not None:
            return self._case_durations_cache.copy()
        try:
            df = self.get_event_log()
            if df.empty:
                return pd.DataFrame(columns=["case_id", "start_time", "end_time", "duration_days"])

            result = (
                df.groupby("case_id")
                .agg(start_time=("timestamp", "min"), end_time=("timestamp", "max"))
                .reset_index()
            )
            result["duration_days"] = (
                (result["end_time"] - result["start_time"]).dt.total_seconds() / 86400
            ).round(2)
            self._case_durations_cache = result
            return self._case_durations_cache.copy()
        except Exception as e:
            raise Exception(f"Case duration extraction failed: {str(e)}")

    # ─────────────────────────────────────────────────────────────────────
    # METADATA / DIAGNOSTICS
    # ─────────────────────────────────────────────────────────────────────

    def list_pools_and_models(self) -> List[Dict]:
        try:
            result = []
            pools = self.celonis.data_integration.get_data_pools()
            for pool in pools:
                pool_info = {"pool_id": pool.id, "pool_name": pool.name, "models": []}
                try:
                    for model in pool.get_data_models():
                        pool_info["models"].append({"model_id": model.id, "model_name": model.name})
                except Exception as e:
                    pool_info["error"] = str(e)
                result.append(pool_info)
            return result
        except Exception as e:
            raise Exception(f"Pool/model discovery failed: {str(e)}")

    def list_tables(self) -> List[Dict]:
        try:
            return [
                {"table_name": table.name, "table_id": getattr(table, "id", "N/A")}
                for table in self.data_model.get_tables()
            ]
        except Exception as e:
            raise Exception(f"Table listing failed: {str(e)}")

    def list_columns(self, table_name: str) -> List[str]:
        try:
            resolved = self._resolve_table_name(table_name)
            for table in self.data_model.get_tables():
                if table.name == resolved:
                    return [col.name for col in table.get_columns()]
            return []
        except Exception as e:
            raise Exception(f"Column listing failed for table '{table_name}': {str(e)}")

    def get_connection_info(self) -> Dict:
        return {
            "connected": True,
            "base_url": settings.CELONIS_BASE_URL,
            "key_type": settings.CELONIS_KEY_TYPE,
            "data_pool_id": settings.CELONIS_DATA_POOL_ID,
            "data_model_id": settings.CELONIS_DATA_MODEL_ID,
            "data_model_name": self.data_model.name if self.data_model else "N/A",
            "activity_table": self.activity_table,
            "activity_tables": self.activity_tables,
            "case_column": self.case_col,
            "activity_column": self.activity_col,
            "timestamp_column": self.timestamp_col,
            "document_column": self.document_col,
            "transaction_column": self.transaction_col,
            "case_table": self.case_table,
            "vendor_column": self.vendor_col,
            # ── NEW: expose resolved discovery results for debugging ──
            "discovered_event_source": CelonisService._shared_event_source,
            "discovered_case_attr_source": CelonisService._shared_case_attr_source,
        }

    # ─────────────────────────────────────────────────────────────────────
    # CONNECTION VALIDATION ENDPOINT  (NEW — call this first to debug)
    # ─────────────────────────────────────────────────────────────────────

    def validate_data_fetch(self) -> Dict[str, Any]:
        """
        Diagnostic method: tries to fetch 5 rows from both the event table and
        the case table and reports exactly what it found.  Call /api/celonis/validate
        (add a route for this) to confirm real data is flowing before relying on
        the cache.
        """
        result: Dict[str, Any] = {
            "connection": "ok",
            "tables_in_model": [],
            "event_source": None,
            "case_attr_source": None,
            "event_sample": [],
            "case_attr_sample": [],
            "errors": [],
        }
        try:
            result["tables_in_model"] = self._get_table_names()
        except Exception as e:
            result["errors"].append(f"table listing: {e}")

        try:
            event_source = self._discover_event_source()
            result["event_source"] = {
                "table": event_source["table"],
                "score": event_source["score"],
                "mapping": event_source["mapping"],
            }
            # Fetch just 5 rows
            from pycelonis.pql import PQL, PQLColumn
            pql_table = self._to_pql_table_name(event_source["table"])
            q = PQL(limit=5, offset=0)
            for col in [v for v in event_source["mapping"].values() if v]:
                q += PQLColumn(name=col, query=f'"{pql_table}"."{col}"')
            sample_df = self._run_pql(q, "validate event source")
            result["event_sample"] = sample_df.where(pd.notnull(sample_df), None).to_dict(orient="records")
        except Exception as e:
            result["errors"].append(f"event source: {e}")

        try:
            case_source = self._discover_case_attr_source()
            result["case_attr_source"] = {
                "table": case_source["table"],
                "score": case_source["score"],
                "mapping": case_source["mapping"],
            }
            from pycelonis.pql import PQL, PQLColumn
            pql_table = self._to_pql_table_name(case_source["table"])
            q = PQL(limit=5, offset=0)
            for col in [v for v in case_source["mapping"].values() if v]:
                q += PQLColumn(name=col, query=f'"{pql_table}"."{col}"')
            sample_df = self._run_pql(q, "validate case attr source")
            result["case_attr_sample"] = sample_df.where(pd.notnull(sample_df), None).to_dict(orient="records")
        except Exception as e:
            result["errors"].append(f"case attr source: {e}")

        result["ok"] = len(result["errors"]) == 0
        return result