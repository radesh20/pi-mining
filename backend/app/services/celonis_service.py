import logging
import math
import threading
import warnings
import time
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

    def _connect(self):
        if not settings.CELONIS_BASE_URL or not settings.CELONIS_API_TOKEN:
            raise CelonisConnectionError(
                "CELONIS_BASE_URL and CELONIS_API_TOKEN are required in .env"
            )

        # Reuse the same Celonis SDK objects across requests to avoid repeated slow handshakes.
        with CelonisService._shared_lock:
            if CelonisService._shared_initialized:
                self.celonis = CelonisService._shared_celonis
                self.data_pool = CelonisService._shared_data_pool
                self.data_model = CelonisService._shared_data_model
                return

        try:
            from pycelonis import get_celonis

            logger.info(
                "Connecting to Celonis: base_url=%s key_type=%s",
                settings.CELONIS_BASE_URL,
                settings.CELONIS_KEY_TYPE,
            )
            celonis = self._run_with_timeout(
                lambda: get_celonis(
                    base_url=settings.CELONIS_BASE_URL,
                    api_token=settings.CELONIS_API_TOKEN,
                    key_type=settings.CELONIS_KEY_TYPE,
                ),
                timeout_seconds=max(int(getattr(settings, "CELONIS_CONNECT_TIMEOUT_SECONDS", 20) or 20), 5),
                label="Celonis SDK connection",
            )
            logger.info("Celonis SDK connection successful")
        except Exception as e:
            raise CelonisConnectionError(
                f"Failed to connect to Celonis: {str(e)}. Check CELONIS_BASE_URL, CELONIS_API_TOKEN, CELONIS_KEY_TYPE."
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
            raise CelonisConnectionError(f"Failed to load data pool/model: {str(e)}")

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

    def _run_pql(self, query: Any, operation_name: str) -> pd.DataFrame:
        pql_timeout = max(int(getattr(settings, "CELONIS_PQL_TIMEOUT_SECONDS", 90) or 90), 10)
        try:
            # Keep legacy export path for compatibility/performance in this environment.
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="Deprecation",
                    category=UserWarning,
                )
                warnings.filterwarnings(
                    "ignore",
                    category=UserWarning,
                    module=r"pycelonis\.utils\.deprecation",
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
                logger.error(str(query))
                raise Exception(
                    f"{operation_name} failed: legacy_export={str(e)} | saola={str(e2)}"
                )

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

    def _discover_event_source(self) -> Dict[str, Any]:
        """
        Find the best extractable event source table and column mapping.
        Handles environments where _CEL_* helper tables exist but are not extractable.
        """
        aliases = {
            "case_id": [self.case_col, "CASEKEY", "_CASE_KEY", "CASE_ID", "CASEID"],
            "activity": [self.activity_col, "ACTIVITYEN", "ACTIVITY_EN", "ACTIVITY", "ACTIVITY_NAME"],
            "timestamp": [self.timestamp_col, "EVENTTIME", "EVENT_TIME", "TIMESTAMP", "TIME"],
            "resource": [self.resource_col, "USERNAME", "USER_NAME", "USER", "RESOURCE"],
            "resource_role": [self.resource_role_col, "USERTYPE", "USER_TYPE", "ROLE", "RESOURCE_ROLE"],
            "document_number": [self.document_col, "EBELN", "DOCUMENT_NUMBER", "PO_NUMBER"],
            "transaction_code": [self.transaction_col, "TRANSACTIONCODE", "TRANSACTION_CODE", "TCODE"],
        }

        preferred = list(self.activity_tables) + [
            "t_o_custom_AccountingDocumentHeader",
            "AccountingDocumentHeader",
            "t_o_custom_VimHeader",
            "t_o_custom_VIMHEADER",
            "VimHeader",
        ]
        candidate_names = []
        known_names = set(self._get_table_names())
        for name in preferred:
            if name in known_names and name not in candidate_names:
                candidate_names.append(name)
        best: Optional[Dict[str, Any]] = None

        for table_name in candidate_names:
            cols = self._table_columns_safe(table_name)
            if not cols:
                continue

            mapping = {target: self._find_col_by_aliases(cols, a) for target, a in aliases.items()}
            mandatory = all(mapping.get(k) for k in ["case_id", "activity", "timestamp"])
            score = sum(1 for v in mapping.values() if v)
            if not mandatory:
                continue

            candidate = {"table": table_name, "mapping": mapping, "score": score}
            if best is None or candidate["score"] > best["score"]:
                best = candidate

            # Fast path when configured table is already a strong match.
            if table_name == self.activity_table and score >= 6:
                return candidate

            if table_name in set(candidate_names) and score >= 6:
                return candidate

        # Last resort: global scan across all tables.
        if best is None:
            full_scan_limit = max(int(getattr(settings, "CELONIS_DISCOVERY_MAX_TABLES", 80) or 80), 1)
            ranked_names = sorted(
                known_names,
                key=lambda n: (
                    0 if str(n).startswith("t_o_custom_") else
                    1 if str(n).startswith("t_e_custom_") else
                    2
                ),
            )
            for table_name in ranked_names[:full_scan_limit]:
                if table_name in candidate_names:
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
                if best is None or candidate["score"] > best["score"]:
                    best = candidate

        if best is None:
            raise Exception("No suitable event table found with required columns (case/activity/timestamp).")
        return best

    def _discover_case_attr_source(self) -> Dict[str, Any]:
        aliases = {
            "document_number": [self.case_table_doc_col, "EBELN", "DOCUMENT_NUMBER", "PO_NUMBER"],
            "vendor_id": [self.vendor_col, "LIFNR", "VENDOR", "VENDOR_ID"],
            "payment_terms": [self.payment_terms_col, "ZTERM", "PAYMENT_TERMS"],
            "currency": [self.currency_col, "WAERS", "CURRENCY"],
        }
        preferred = [
            self.case_table,
            "t_o_custom_AccountingDocumentHeader",
            "AccountingDocumentHeader",
            "t_o_custom_PurchasingDocumentHeader",
            "PurchasingDocumentHeader",
            "t_o_custom_VimHeader",
        ]
        candidate_names = []
        known_names = set(self._get_table_names())
        for name in preferred:
            if name in known_names and name not in candidate_names:
                candidate_names.append(name)
        best: Optional[Dict[str, Any]] = None

        for table_name in candidate_names:
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
            if table_name == self.case_table and score >= 3:
                return candidate
            if table_name in {"t_o_custom_PurchasingDocumentHeader", "PurchasingDocumentHeader"} and score >= 3:
                return candidate

        if best is None:
            full_scan_limit = max(int(getattr(settings, "CELONIS_DISCOVERY_MAX_TABLES", 80) or 80), 1)
            ranked_names = sorted(
                known_names,
                key=lambda n: (
                    0 if str(n).startswith("t_o_custom_") else
                    1 if str(n).startswith("t_e_custom_") else
                    2
                ),
            )
            for table_name in ranked_names[:full_scan_limit]:
                if table_name in candidate_names:
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
            raise Exception("No suitable case attribute table found with required columns (document_number/vendor_id).")
        return best

    def _discover_accounting_attr_source(self) -> Dict[str, Any]:
        aliases = {
            "case_id": [self.case_col, "CASEKEY", "_CASE_KEY", "CASE_ID", "CASEID"],
            "document_number": [self.case_table_doc_col, "EBELN", "DOCUMENT_NUMBER", "PO_NUMBER"],
            "invoice_number": ["BELNR", "INVOICE_NUMBER", "INV_NUMBER"],
            "fiscal_year": ["GJAHR", "FISCAL_YEAR"],
            "company_code": ["BUKRS", "COMPANY_CODE"],
            "clearing_document_number": ["AUGBL", "CLEARING_DOCUMENT_NUMBER", "CLEARING_DOCUMENT"],
            "baseline_date": ["ZFBDT", "BASELINE_DATE"],
            "due_date": ["FAEDT", "NETDT", "DUE_DATE"],
            "cleared_date": ["AUGDT", "CLEARED_DATE"],
            "invoice_payment_terms": ["ZTERM", "INVOICE_PAYMENT_TERMS", "INVOICE_PT"],
            "po_payment_terms": ["ZTERM_PO", "PO_PAYMENT_TERMS", "PO_PAYMENT_TERM"],
            "vendor_master_payment_terms": ["ZTERM_VENDOR", "VENDOR_MASTER_PAYMENT_TERMS", "VENDOR_MASTER_PT"],
        }
        preferred = [
            "t_o_custom_AccountingDocumentHeader",
            "AccountingDocumentHeader",
        ]

        candidate_names = []
        known_names = set(self._get_table_names())
        for name in preferred:
            if name in known_names and name not in candidate_names:
                candidate_names.append(name)

        best: Optional[Dict[str, Any]] = None
        for table_name in candidate_names:
            cols = self._table_columns_safe(table_name)
            if not cols:
                continue
            mapping = {target: self._find_col_by_aliases(cols, a) for target, a in aliases.items()}
            mandatory = bool(mapping.get("invoice_number")) or bool(mapping.get("document_number")) or bool(mapping.get("case_id"))
            score = sum(1 for v in mapping.values() if v)
            if not mandatory:
                continue
            candidate = {"table": table_name, "mapping": mapping, "score": score}
            if best is None or candidate["score"] > best["score"]:
                best = candidate
            if score >= 4:
                return candidate

        if best is not None:
            return best
        raise Exception("No suitable accounting header table found with case/invoice/document keys.")

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
                    "Configured activity table %s resolved to %s but table is not present. "
                    "Will auto-discover source table at query time.",
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
                    "Configured case table %s resolved to %s but table is not present. "
                    "Will auto-discover case table at query time.",
                    self.case_table,
                    resolved_case,
                )
                CelonisService._warned_missing_case_table = True
        elif resolved_case != self.case_table:
            logger.warning("Case table %s not found, using %s", self.case_table, resolved_case)
            self.case_table = resolved_case

    @staticmethod
    def _to_pql_table_name(table_name: str) -> str:
        """
        Some Celonis setups require the technical name without leading 't_' in PQL references.
        """
        return table_name[2:] if table_name.startswith("t_") else table_name

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
                f"{operation_name} failed: columns not found in '{resolved_table}': {missing}"
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

            signature = (
                tuple(chunk.iloc[0].astype(str).tolist()),
                tuple(chunk.iloc[-1].astype(str).tolist()),
                int(len(chunk)),
            )
            if last_page_signature is not None and signature == last_page_signature:
                logger.warning(
                    "Detected repeating page while extracting %s from %s. Stopping pagination to avoid loop.",
                    operation_name,
                    resolved_table,
                )
                break
            last_page_signature = signature

            frames.append(chunk)
            fetched = int(len(chunk))
            total_rows += fetched
            offset += fetched
            page += 1

            # Some environments ignore offset/limit and return full result each call.
            # In that case, keep the first chunk and stop to avoid long/repeating loops.
            if fetched > effective_limit:
                logger.warning(
                    "%s returned %s rows for limit=%s (table=%s). "
                    "Assuming backend-side limit/offset was ignored; stopping after first page.",
                    operation_name,
                    fetched,
                    effective_limit,
                    resolved_table,
                )
                break

            if row_cap is not None and total_rows >= row_cap:
                break
            if fetched < effective_limit:
                break

        if not frames:
            return pd.DataFrame(columns=columns)

        result = pd.concat(frames, ignore_index=True)
        if row_cap is not None and len(result) > row_cap:
            result = result.iloc[:row_cap].copy()

        logger.info(
            "%s completed for %s: extracted %s rows (batch=%s, max_rows=%s)",
            operation_name,
            resolved_table,
            len(result),
            chunk_size,
            row_cap if row_cap is not None else "unlimited",
        )
        return result

    def get_event_log(self) -> pd.DataFrame:
        """
        Extract event log from t_o_custom_VimHeader.
        Output columns:
        - case_id, activity, timestamp, resource, resource_role, document_number, transaction_code
        """
        if self._event_log_cache is not None:
            return self._event_log_cache.copy()

        try:
            source = self._discover_event_source()
            source_table = source["table"]
            mapping = source["mapping"]
            source_columns = [col for col in mapping.values() if col]
            raw_df = self._extract_table_rows_paginated(
                table_name=source_table,
                columns=source_columns,
                operation_name="event log extraction",
                max_rows=(settings.CELONIS_EVENT_LOG_MAX_ROWS if settings.CELONIS_EVENT_LOG_MAX_ROWS > 0 else None),
            )
            rename_map = {src: tgt for tgt, src in mapping.items() if src}
            df = raw_df.rename(columns=rename_map)

            expected_cols = [
                "case_id",
                "activity",
                "timestamp",
                "resource",
                "resource_role",
                "document_number",
                "transaction_code",
            ]
            if df.empty:
                return pd.DataFrame(columns=expected_cols)

            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None

            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df.sort_values(["case_id", "timestamp"], na_position="last").reset_index(drop=True)
            self._event_log_cache = df[expected_cols]
            return self._event_log_cache.copy()
        except Exception as e:
            raise Exception(f"Event log extraction failed: {str(e)}")

    def get_case_attributes(self) -> pd.DataFrame:
        """
        Extract case table attributes from t_o_custom_PurchasingDocumentHeader.
        """
        if self._case_attributes_cache is not None:
            return self._case_attributes_cache.copy()

        try:
            source = self._discover_case_attr_source()
            source_table = source["table"]
            mapping = source["mapping"]
            source_columns = [col for col in mapping.values() if col]
            raw_df = self._extract_table_rows_paginated(
                table_name=source_table,
                columns=source_columns,
                operation_name="case attributes extraction",
            )
            rename_map = {src: tgt for tgt, src in mapping.items() if src}
            df = raw_df.rename(columns=rename_map)
            if df.empty:
                return pd.DataFrame(columns=["document_number", "vendor_id", "payment_terms", "currency"])

            for col in ["document_number", "vendor_id", "payment_terms", "currency"]:
                if col not in df.columns:
                    df[col] = None

            df = df.drop_duplicates().reset_index(drop=True)
            self._case_attributes_cache = df
            return self._case_attributes_cache.copy()
        except Exception as e:
            raise Exception(f"Case attributes extraction failed: {str(e)}")

    def get_accounting_document_attributes(self) -> pd.DataFrame:
        try:
            source = self._discover_accounting_attr_source()
            source_table = source["table"]
            mapping = source["mapping"]
            source_columns = [col for col in mapping.values() if col]
            if not source_columns:
                return pd.DataFrame()

            raw_df = self._extract_table_rows_paginated(
                table_name=source_table,
                columns=source_columns,
                operation_name="accounting header extraction",
            )
            rename_map = {src: tgt for tgt, src in mapping.items() if src}
            df = raw_df.rename(columns=rename_map)
            if df.empty:
                return pd.DataFrame(columns=list(mapping.keys()))

            for col in mapping.keys():
                if col not in df.columns:
                    df[col] = None

            for dt_col in ["baseline_date", "due_date", "cleared_date"]:
                if dt_col in df.columns:
                    df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")

            return df.drop_duplicates().reset_index(drop=True)
        except Exception as e:
            logger.warning("Accounting header extraction unavailable: %s", str(e))
            return pd.DataFrame()

    def get_vendor_statistics(self) -> pd.DataFrame:
        try:
            df = self.get_event_log_with_vendor()
            if df.empty:
                return pd.DataFrame(columns=["vendor_id", "case_count", "exception_case_count", "exception_rate_pct"])

            work = df.copy()
            work["vendor_id"] = work["vendor_id"].fillna("UNKNOWN").astype(str)
            work["case_id"] = work["case_id"].astype(str)
            work["activity"] = work["activity"].fillna("").astype(str).str.lower()
            exception_case_map = (
                work.groupby("case_id")["activity"]
                .apply(lambda s: int(s.str.contains(r"\bexception\b|moved out|due date passed|block", regex=True, na=False).any()))
                .reset_index(name="is_exception")
            )
            vendor_case_map = (
                work.groupby("case_id")["vendor_id"]
                .agg(lambda s: s.dropna().iloc[0] if not s.dropna().empty else "UNKNOWN")
                .reset_index()
            )
            merged = vendor_case_map.merge(exception_case_map, on="case_id", how="left")
            merged["is_exception"] = merged["is_exception"].fillna(0).astype(int)

            result = (
                merged.groupby("vendor_id", dropna=False)
                .agg(
                    case_count=("case_id", "nunique"),
                    exception_case_count=("is_exception", "sum"),
                )
                .reset_index()
            )
            result["exception_rate_pct"] = (
                result["exception_case_count"] / result["case_count"].replace(0, pd.NA) * 100
            ).fillna(0.0).round(2)
            return result
        except Exception as e:
            raise Exception(f"Vendor statistics extraction failed: {str(e)}")

    def get_vendor_mapping(self) -> pd.DataFrame:
        """
        Link VimHeader.EBELN -> PurchasingDocumentHeader.EBELN -> vendor_id (LIFNR).
        """
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
            vendor_map = self.get_vendor_mapping()[["case_id", "document_number", "vendor_id", "payment_terms", "currency"]]

            enriched = events.merge(
                vendor_map,
                on=["case_id", "document_number"],
                how="left",
            )
            enriched["vendor_id"] = enriched["vendor_id"].fillna("UNKNOWN")
            self._event_with_vendor_cache = enriched
            return self._event_with_vendor_cache.copy()
        except Exception as e:
            raise Exception(f"Event log enrichment with vendor failed: {str(e)}")

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
                    renamed.groupby("vendor_id")["vendor_id"]
                    .first().astype(str).to_dict()
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

        # ── NEW: derive payment behavior from case-level event log ──────────
        try:
            from app.services.data_cache_service import DataCacheService
            event_log = self.get_event_log_with_vendor()
            payment_behavior_by_vendor = (
                DataCacheService._derive_payment_behavior_from_case_level(event_log)
                if not event_log.empty else {}
            )
        except Exception as pb_err:
            logger.warning("Payment behavior derivation failed: %s", pb_err)
            payment_behavior_by_vendor = {}
        # ────────────────────────────────────────────────────────────────────

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

                # Real payment behavior from case-level data, or explicit null fallback
                pb = payment_behavior_by_vendor.get(vendor_id)
                if pb:
                    payment_behavior = {**pb, "_source": "celonis"}
                else:
                    payment_behavior = {
                        "on_time_pct": None,
                        "early_pct": None,
                        "late_pct": None,
                        "open_pct": None,
                        "_source": "unavailable",
                    }

                rows.append({
                    "vendor_id": vendor_id,
                    "vendor_lifnr": vendor_lifnr_by_vendor.get(vendor_id, vendor_id),
                    "total_cases": int(row.get("case_count", 0) or 0),
                    "total_value": float(total_value_by_vendor.get(vendor_id, 0.0)),
                    "exception_rate": round(exception_rate, 2),
                    "avg_dpo": round(avg_dpo, 2),
                    "payment_behavior": payment_behavior,
                    "risk_score": risk,
                })

        rows.sort(key=lambda x: (x["total_value"], x["total_cases"]), reverse=True)
        return rows
     except Exception as e:
        raise Exception(f"Vendor stats API payload generation failed: {str(e)}")

    def get_table_data(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        max_rows: Optional[int] = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """
        Generic full table extractor using paginated PQL limit/offset.
        """
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

    def get_working_capital_extract(
        self,
        include_rows: bool = False,
        max_rows_per_table: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Extracts all working-capital-related tables from the data model.
        Set include_rows=true to return full records for each table.
        """
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
        """
        Infer owning object table for an event table based on *_ID linking columns.
        """
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
        """
        Extract all Working Capital tables and group them into separate buckets:
        - object_activity_streams: object-centric streams (e.g. VimHeader Activity)
        - event_streams_by_object: t_e_* tables grouped by linked object
        - object_master_tables: non-activity object tables
        """
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

            # Ensure required tables are never dropped by filters.
            for req in sorted(required_tables):
                if req in all_tables and req not in candidate_tables:
                    candidate_tables.append(req)

            if max_tables and max_tables > 0 and len(candidate_tables) > max_tables:
                prioritized = [t for t in candidate_tables if t in required_tables]
                remaining = [t for t in candidate_tables if t not in required_tables]
                candidate_tables = (prioritized + remaining)[:max_tables]

            object_tables = sorted([t for t in candidate_tables if t.startswith("t_o_custom_")])
            event_tables = sorted([t for t in candidate_tables if t.startswith("t_e_custom_") and include_event_tables])
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

            # Group object tables into activity streams vs master tables.
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

            # Group event tables by linked object table.
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

    @staticmethod
    def _pick_best_amount_column(df: pd.DataFrame) -> Optional[str]:
        if df is None or df.empty:
            return None
        preferred = [
            "CONVERTED_INV_VALUE_USD",
            "CONVERTEDINVVALUEUSD",
            "INV_VALUE_USD",
            "INVOICE_VALUE_USD",
            "INV_VALUE",
            "INVOICE_VALUE",
            "WRBTR",
            "DMBTR",
            "NETWR",
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
            "VALUE",
            "AMOUNT",
            "USD",
            "INV",
            "NET",
            "TOTAL",
            "WRBTR",
            "DMBTR",
            "NETWR",
            "MENGE",
            "PRICE",
            "COST",
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
        """
        Extract all tables in the data model with row counts + amount summaries.
        """
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
        """
        Extract all model tables grouped separately by table family.
        """
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
        return ranked[0]

    def get_detailed_transaction_olap(
        self,
        include_rows: bool = False,
        max_rows: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Returns a frontend-ready Detailed Transaction OLAP dataset with stable output keys.
        """
        try:
            field_aliases: Dict[str, List[str]] = {
                "company_code": ["Company Code", "BUKRS", "COMPANYCODE"],
                "supplier_type": ["Supplier Type", "SUPPLIERTYPE", "VENDOR_TYPE"],
                "vendor_id": ["LIFNR", "Vendor ID", "VENDOR", "VENDORID"],
                "supplier_name": ["Supplier Name", "NAME1", "VENDORNAME", "SUPPLIERNAME"],
                "invoice_number": ["Inv Number", "Invoice Number", "BELNR", "INVOICENUMBER", "VBELN"],
                "invoice_line_item_number": ["Inv Line Item Number", "BUZEI", "LINEITEM", "ITEMNUMBER"],
                "invoice_value_usd": [
                    "Inv. value (in USD)",
                    "Inv value in USD",
                    "INV_VALUE_USD",
                    "INVOICE_VALUE_USD",
                    "DMBTR",
                    "WRBTR",
                    "NETWR",
                ],
                "currency": ["Currency", "WAERS"],
                "converted_invoice_value_usd": [
                    "Converted Inv Value",
                    "CONVERTED_INV_VALUE_USD",
                    "CONVERTEDINVVALUEUSD",
                    "INVOICE_VALUE_CONVERTED",
                ],
                "fiscal_year": ["Fiscal Year", "GJAHR", "FISCALYEAR"],
                "clearing_document_number": [
                    "Clearing Document",
                    "AUGBL",
                    "CLEARINGDOC",
                    "CLEARINGDOCUMENTNUMBER",
                ],
                "payment_status": ["Payment Status", "PAYMENT_STATUS", "RECOMMENDATION", "STATUS"],
                "invoice_payment_terms": ["Invoice PT", "INVOICE_PT", "INVOICE_PAYMENT_TERMS", "ZTERM_INV"],
                "po_payment_terms": ["PO Payment Term", "PO_PAYMENT_TERM", "PO_PAYMENT_TERMS", "ZTERM_PO"],
                "vendor_master_payment_terms": [
                    "Vendor Master PT",
                    "VENDOR_MASTER_PT",
                    "VENDOR_PAYMENT_TERMS",
                    "ZTERM_VENDOR",
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

            selected_source_cols: List[str] = []
            selected_map: Dict[str, Optional[str]] = {}
            for target_field, aliases in field_aliases.items():
                matched = self._find_matching_column(source_columns, aliases)
                selected_map[target_field] = matched
                if matched and matched not in selected_source_cols:
                    selected_source_cols.append(matched)

            if not selected_source_cols:
                raise Exception(
                    f"Could not map any OLAP columns from table '{source_table}'. Available columns: {source_columns}"
                )

            raw = self.get_table_data(
                table_name=source_table,
                columns=selected_source_cols,
                batch_size=batch_size,
                max_rows=max_rows,
                use_cache=False,
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

    def get_vendor_stats_api(self) -> List[Dict[str, Any]]:
        """
        API-ready vendor stats used by /process/vendor-stats.
        """
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
                        renamed.groupby("vendor_id")["vendor_id"]
                        .first()
                        .astype(str)
                        .to_dict()
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

                    rows.append(
                        {
                            "vendor_id": vendor_id,
                            "vendor_lifnr": vendor_lifnr_by_vendor.get(vendor_id, vendor_id),
                            "total_cases": int(row.get("case_count", 0) or 0),
                            "total_value": float(total_value_by_vendor.get(vendor_id, 0.0)),
                            "exception_rate": round(exception_rate, 2),
                            "avg_dpo": round(avg_dpo, 2),
                            "payment_behavior": {
                                "on_time_pct": 0.0,
                                "early_pct": 0.0,
                                "late_pct": 0.0,
                                "open_pct": 0.0,
                                "_source": "requires_case_level_enrichment",
                            },
                            "risk_score": risk,
                        }
                    )

            rows.sort(key=lambda x: (x["total_value"], x["total_cases"]), reverse=True)
            return rows
        except Exception as e:
            raise Exception(f"Vendor stats API payload generation failed: {str(e)}")

    def get_vendor_paths(self, vendor_id: str) -> Dict[str, Any]:
        """
        Returns happy vs exception paths for a vendor.
        """
        try:
            df = self.get_event_log_with_vendor()
            if df.empty:
                return {"vendor_id": vendor_id, "happy_paths": [], "exception_paths": []}

            alias_lifnr = ""

            normalized_vendor = df["vendor_id"].astype(str).str.upper().str.strip()
            vendor_df = df[normalized_vendor == vendor_id.upper().strip()].copy()
            if vendor_df.empty and alias_lifnr:
                vendor_df = df[
                    df["vendor_id"].astype(str).str.strip() == alias_lifnr
                ].copy()
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
                    case_duration[["case_id", "duration_days"]],
                    on="case_id",
                    how="left",
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
                        for kw in ["exception", "due date passed", "block", "moved out", "short payment", "immediate", "early payment"]
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
                elif not global_agg.empty:
                    top_global = global_agg.iloc[0]
                    happy_paths = [
                        {
                            "variant": top_global["variant"],
                            "frequency": int(top_global["frequency"]),
                            "percentage": float(top_global["percentage"]),
                            "avg_duration_days": float(top_global["avg_duration_days"]),
                            "source": "global_baseline",
                            "note": "Using top Celonis baseline path because this vendor currently appears only in exception traces.",
                        }
                    ]

            return {
                "vendor_id": vendor_id,
                "happy_paths": happy_paths,
                "exception_paths": exception_paths,
            }
        except Exception as e:
            raise Exception(f"Vendor paths extraction failed for vendor '{vendor_id}': {str(e)}")

    def get_variants(self) -> pd.DataFrame:
        """
        Build variants in Python from ordered event traces.
        """
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
        """
        Build directly-follows throughput transitions in Python.
        """
        if self._throughput_cache is not None:
            return self._throughput_cache.copy()

        try:
            df = self.get_event_log()
            if df.empty:
                return pd.DataFrame(
                    columns=[
                        "source_activity",
                        "target_activity",
                        "avg_duration_days",
                        "median_duration_days",
                        "case_count",
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

                    transitions.append(
                        {
                            "case_id": case_id,
                            "source_activity": source_activity,
                            "target_activity": target_activity,
                            "duration_days": duration_days,
                        }
                    )

            trans_df = pd.DataFrame(transitions)
            if trans_df.empty:
                return pd.DataFrame(
                    columns=[
                        "source_activity",
                        "target_activity",
                        "avg_duration_days",
                        "median_duration_days",
                        "case_count",
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

    def list_pools_and_models(self) -> List[Dict]:
        try:
            result = []
            pools = self.celonis.data_integration.get_data_pools()
            for pool in pools:
                pool_info = {
                    "pool_id": pool.id,
                    "pool_name": pool.name,
                    "models": [],
                }
                try:
                    for model in pool.get_data_models():
                        pool_info["models"].append(
                            {
                                "model_id": model.id,
                                "model_name": model.name,
                            }
                        )
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
        }
