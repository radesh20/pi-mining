import logging
import threading
import time
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from app.config import settings
from app.services.celonis_service import CelonisService
from app.services.process_insight_service import ProcessInsightService

logger = logging.getLogger(__name__)


class DataCacheService:
    """
    Singleton-style in-memory cache for Celonis-derived data and prepared analytics.
    """

    # Hard ceiling: if cache age exceeds this, enforce_max_staleness() raises.
    # Operators can override via CACHE_MAX_STALENESS_SECONDS env-var (see ensure_loaded).
    MAX_STALENESS_SECONDS: int = 7200  # 2 hours default

    EXCEPTION_LABELS = [
        "Payment Terms Mismatch",
        "Invoices with Exception",
        "Short Payment Terms (0 Days)",
        "Early Payment / DPO 0-7 Days",
        "Paid Late",
        "Open Invoices at Risk",
    ]

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._refresh_cond = threading.Condition(self._lock)
        self._refresh_in_progress = False
        self._refresh_started_at: Optional[float] = None
        self._is_loaded = False
        self.last_refreshed_at: Optional[str] = None
        self.load_duration_seconds: float = 0.0
        self.last_error: Optional[str] = None

        self.event_log_df = pd.DataFrame()
        self.purchasing_header_df = pd.DataFrame()
        self.accounting_header_df = pd.DataFrame()
        self.enriched_event_log_df = pd.DataFrame()
        self.case_level_df = pd.DataFrame()
        self.case_table_full_df = pd.DataFrame()
        self.wcm_olap_df = pd.DataFrame()
        self.wcm_grouped_extract: Dict[str, Any] = {}
        self.process_context: Dict[str, Any] = {}
        self.vendor_stats: List[Dict[str, Any]] = []
        self.vendor_paths_map: Dict[str, Dict[str, Any]] = {}
        self.vendor_records_map: Dict[str, List[Dict[str, Any]]] = {}
        self.exception_records_map: Dict[str, List[Dict[str, Any]]] = {}
        self.exception_categories: List[Dict[str, Any]] = []
        self.available_vendors: List[str] = []
        self.cache_meta: Dict[str, Any] = {}

    def ensure_loaded(self) -> None:
        wait_seconds = max(int(getattr(settings, "CACHE_REFRESH_WAIT_SECONDS", 30) or 30), 1)
        initial_wait_seconds = max(
            int(getattr(settings, "CACHE_INITIAL_LOAD_WAIT_SECONDS", 600) or 600),
            wait_seconds,
        )
        serve_stale_while_refresh = bool(getattr(settings, "CACHE_STALE_WHILE_REFRESH", True))

        with self._lock:
            self._clear_stuck_refresh_locked()
            loaded = self._is_loaded
            stale = self._is_stale_locked()
            refresh_running = self._refresh_in_progress

            if loaded and not stale:
                return

            if loaded and stale and serve_stale_while_refresh:
                if not refresh_running:
                    self._refresh_in_progress = True
                    self._refresh_started_at = time.time()
                    threading.Thread(
                        target=self._refresh_in_background,
                        daemon=True,
                        name="cache-refresh-bg",
                    ).start()
                return

            if (not loaded) and refresh_running and serve_stale_while_refresh:
                # Avoid request hangs during initial Celonis load.
                return

            if (not loaded) and (not refresh_running) and serve_stale_while_refresh:
                self._refresh_in_progress = True
                self._refresh_started_at = time.time()
                threading.Thread(
                    target=self._refresh_in_background,
                    daemon=True,
                    name="cache-initial-load-bg",
                ).start()
                return

            if refresh_running:
                effective_wait = wait_seconds if loaded else initial_wait_seconds
                end_time = time.time() + effective_wait
                while self._refresh_in_progress:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        break
                    self._refresh_cond.wait(timeout=remaining)
                if self._is_loaded:
                    return
                if self._refresh_in_progress:
                    wait_type = "initial cache load" if not loaded else "cache refresh"
                    raise RuntimeError(
                        f"Timed out waiting for {wait_type} to complete "
                        f"(waited ~{effective_wait}s)."
                    )
                raise RuntimeError("Cache refresh completed but no cache snapshot is available.")

            self._refresh_in_progress = True
            self._refresh_started_at = time.time()

        try:
            self._refresh_all_data_impl()
        finally:
            with self._lock:
                self._refresh_in_progress = False
                self._refresh_started_at = None
                self._refresh_cond.notify_all()

    def _refresh_in_background(self) -> None:
        try:
            self._refresh_all_data_impl()
        except Exception as e:  # noqa: BLE001
            logger.warning("Background cache refresh failed; continuing with stale cache: %s", str(e))
        finally:
            with self._lock:
                self._refresh_in_progress = False
                self._refresh_started_at = None
                self._refresh_cond.notify_all()

    def _is_stale_locked(self) -> bool:
        if not self._is_loaded or not self.last_refreshed_at:
            return True
        ttl = max(int(getattr(settings, "CACHE_TTL_SECONDS", 0) or 0), 0)
        if ttl <= 0:
            return False
        try:
            refreshed = datetime.fromisoformat(self.last_refreshed_at.replace("Z", "+00:00"))
        except Exception:
            return True
        return (datetime.now(timezone.utc) - refreshed).total_seconds() > ttl

    def refresh_all_data(self) -> Dict[str, Any]:
        wait_seconds = max(int(getattr(settings, "CACHE_REFRESH_WAIT_SECONDS", 30) or 30), 1)
        with self._lock:
            self._clear_stuck_refresh_locked()
            if self._refresh_in_progress:
                end_time = time.time() + wait_seconds
                while self._refresh_in_progress:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        break
                    self._refresh_cond.wait(timeout=remaining)
                if self._refresh_in_progress:
                    logger.warning(
                        "Timed out waiting for ongoing cache refresh; returning current cache snapshot."
                    )
                    return self.get_cache_status()
                return self.get_cache_status()
            self._refresh_in_progress = True
            self._refresh_started_at = time.time()

        try:
            self._refresh_all_data_impl()
        finally:
            with self._lock:
                self._refresh_in_progress = False
                self._refresh_started_at = None
                self._refresh_cond.notify_all()

        return self.get_cache_status()

    def _refresh_all_data_impl(self) -> None:
        start = time.perf_counter()
        logger.info("Refreshing Celonis data cache...")
        try:
            celonis = CelonisService()
            insight_service = ProcessInsightService(celonis)

            event_log = celonis.get_event_log().copy()
            case_attrs = celonis.get_case_attributes().copy()
            accounting_attrs = celonis.get_accounting_document_attributes().copy()
            case_table_full = celonis.get_table_data(celonis.case_table, use_cache=False).copy()

            # ============================================================
            # PHASE 1: Build initial process_context from Celonis
            #   (needed as input to _build_case_level_dataset)
            # ============================================================
            process_context = insight_service.build_process_context()

            wcm_mode = getattr(settings, "WCM_CONTEXT_MODE", "full")

            grouped_extract = {}
            if wcm_mode != "legacy" and getattr(settings, "WCM_ENABLE_GROUPED_EXTRACT", False):
                grouped_prefixes = [
                    p.strip()
                    for p in str(getattr(settings, "WCM_GROUPED_TABLE_PREFIXES", "") or "").split(",")
                    if p.strip()
                ]
                grouped_allowlist = [
                    t.strip()
                    for t in str(getattr(settings, "WCM_GROUPED_TABLE_ALLOWLIST", "") or "").split(",")
                    if t.strip()
                ]
                grouped_extract = celonis.get_working_capital_grouped_extract(
                    include_rows=getattr(settings, "WCM_GROUPED_INCLUDE_ROWS", False),
                    max_rows_per_table=(
                        getattr(settings, "WCM_GROUPED_MAX_ROWS_PER_TABLE", 10000)
                        if getattr(settings, "WCM_GROUPED_MAX_ROWS_PER_TABLE", 0) > 0
                        else None
                    ),
                    table_prefixes=grouped_prefixes,
                    table_allowlist=grouped_allowlist,
                    include_event_tables=getattr(settings, "WCM_GROUPED_INCLUDE_EVENT_TABLES", True),
                    max_tables=(
                        getattr(settings, "WCM_GROUPED_MAX_TABLES", 0)
                        if getattr(settings, "WCM_GROUPED_MAX_TABLES", 0) > 0
                        else None
                    ),
                )

            detailed_olap_payload = celonis.get_detailed_transaction_olap(
                include_rows=True,
                max_rows=(getattr(settings, "WCM_OLAP_MAX_ROWS", 0) or None),
            )
            detailed_olap_df = pd.DataFrame(detailed_olap_payload.get("rows", []))

            enriched = self._build_enriched_event_log(
                event_log,
                case_attrs,
                accounting_attrs,
                case_table_full,
                celonis,
            )
            case_level = self._build_case_level_dataset(enriched, process_context)
            case_level = self._enrich_case_level_with_olap(case_level, detailed_olap_df)
            case_level = self._backfill_invoice_amounts_from_aux_tables(case_level, celonis)

            # ============================================================
            # 🔥 PHASE 2: UPDATE process_context with REAL enriched data
            #   This is where the fix actually happens
            # ============================================================
            process_context["total_cases"] = int(len(case_level))
            process_context["total_events"] = int(len(event_log))

            # 🔥 Rebuild variants from case_level if empty/missing
            if not process_context.get("variants"):
                logger.warning("process_context had empty variants, rebuilding from case_level...")
                if "activity_sequence" in case_level.columns:
                    variant_counts = case_level["activity_sequence"].value_counts()
                    total = max(len(case_level), 1)
                    process_context["variants"] = [
                        {
                            "variant": str(v),
                            "frequency": int(c),
                            "percentage": round((c / total) * 100, 1),
                        }
                        for v, c in variant_counts.head(15).items()
                    ]
                elif "variant" in case_level.columns:
                    variant_counts = case_level["variant"].value_counts()
                    total = max(len(case_level), 1)
                    process_context["variants"] = [
                        {
                            "variant": str(v),
                            "frequency": int(c),
                            "percentage": round((c / total) * 100, 1),
                        }
                        for v, c in variant_counts.head(15).items()
                    ]

            # 🔥 Rebuild exception_rate if zero
            if not process_context.get("exception_rate"):
                exc_col = None
                for col_name in ["has_exception", "is_exception", "exception_flag"]:
                    if col_name in case_level.columns:
                        exc_col = col_name
                        break
                if exc_col:
                    exc_count = int(case_level[exc_col].sum())
                    total = max(len(case_level), 1)
                    process_context["exception_rate"] = round((exc_count / total) * 100, 2)
                    logger.info("Rebuilt exception_rate from case_level: %.2f%%", process_context["exception_rate"])

            # 🔥 Rebuild golden_path_percentage if zero
            if not process_context.get("golden_path_percentage") and process_context.get("variants"):
                process_context["golden_path_percentage"] = process_context["variants"][0].get("percentage", 0)

            # 🔥 Update avg_end_to_end_days from case_level if available
            if not process_context.get("avg_end_to_end_days"):
                for col_name in ["end_to_end_days", "cycle_time_days", "duration_days"]:
                    if col_name in case_level.columns:
                        avg_days = case_level[col_name].mean()
                        if pd.notna(avg_days):
                            process_context["avg_end_to_end_days"] = round(float(avg_days), 2)
                            break

            logger.info(
                "Phase 2 process_context update: total_cases=%d, total_events=%d, variants=%d, exception_rate=%.2f%%",
                process_context.get("total_cases", 0),
                process_context.get("total_events", 0),
                len(process_context.get("variants", [])),
                process_context.get("exception_rate", 0),
            )
            # ============================================================
            # END PHASE 2
            # ============================================================

            exception_records_map = self._build_exception_records_map(case_level, process_context)
            vendor_stats = self._build_vendor_stats(case_level, detailed_olap_df, process_context)
            process_context["vendor_stats"] = vendor_stats
            vendor_records_map = self._build_vendor_records_map(case_level, exception_records_map)
            vendor_records_map = self._build_vendor_records_map(case_level, exception_records_map)
            vendor_paths_map = self._build_vendor_paths_map(celonis, case_level)
            vendor_paths_map = self._build_vendor_paths_map(celonis, case_level)
            exception_categories = self._build_exception_categories(exception_records_map)
            profile_summary = self._build_profile_summary(case_level, detailed_olap_df)
            discovered_exceptions = self._build_discovered_exception_summary(exception_records_map)
            celonis_context_layer = self._build_celonis_context_layer(
                process_context=process_context,
                case_level=case_level,
                exception_categories=exception_categories,
                exception_records_map=exception_records_map,
            )
            process_context["profile_view_summary"] = profile_summary
            process_context["discovered_exception_types"] = discovered_exceptions
            process_context["celonis_context_layer"] = celonis_context_layer
            process_context["working_capital_source_summary"] = {
                "mode": wcm_mode,
                "grouped_extract_group_count": int(grouped_extract.get("group_count", 0) if grouped_extract else 0),
                "grouped_extract_tables": int(grouped_extract.get("tables_extracted", 0) if grouped_extract else 0),
                "grouped_selected_tables": grouped_extract.get("selected_tables", []) if grouped_extract else [],
                "grouped_include_event_tables": grouped_extract.get("include_event_tables",
                                                                    False) if grouped_extract else False,
                "grouped_max_tables": grouped_extract.get("max_tables", 0) if grouped_extract else 0,
                "olap_rows": int(len(detailed_olap_df)),
                "includes_open_and_closed": True,
            }

            available_vendors = sorted(
                {
                    str(v).strip()
                    for v in case_level.get("vendor_id", pd.Series(dtype=str)).dropna().tolist()
                    if str(v).strip()
                }
            )

            refreshed_at = datetime.now(timezone.utc).isoformat()
            load_seconds = round(time.perf_counter() - start, 3)
            cache_meta = {
                "event_rows": int(len(event_log)),
                "enriched_event_rows": int(len(enriched)),
                "case_rows": int(len(case_level)),
                "case_table_full_rows": int(len(case_table_full)),
                "accounting_header_rows": int(len(accounting_attrs)),
                "wcm_olap_rows": int(len(detailed_olap_df)),
                "wcm_group_count": int(grouped_extract.get("group_count", 0) if grouped_extract else 0),
                "wcm_olap_source_table": detailed_olap_payload.get("source_table"),
                "wcm_olap_selection_mode": detailed_olap_payload.get("selection_mode"),
                "wcm_olap_missing_fields": detailed_olap_payload.get("source_column_mapping_missing", []),
                "vendor_count": len(available_vendors),
                "exception_categories_count": len(exception_categories),
                "tables_loaded": [
                    celonis.activity_table,
                    celonis.case_table,
                    "t_o_custom_AccountingDocumentHeader",
                ],
                "total_cases": int(process_context.get("total_cases", 0) or 0),
                "total_events": int(process_context.get("total_events", 0) or 0),
            }
        except Exception as e:
            with self._lock:
                self.last_error = str(e)
            logger.exception("Celonis data cache refresh failed.")
            raise

        with self._lock:
            self.event_log_df = event_log
            self.purchasing_header_df = case_attrs
            self.accounting_header_df = accounting_attrs
            self.case_table_full_df = case_table_full
            self.enriched_event_log_df = enriched
            self.case_level_df = case_level
            self.wcm_olap_df = detailed_olap_df
            self.wcm_grouped_extract = grouped_extract
            self.vendor_stats = vendor_stats
            self.vendor_records_map = vendor_records_map
            self.process_context = process_context
            self.exception_records_map = exception_records_map
            self.vendor_records_map = vendor_records_map
            self.vendor_paths_map = vendor_paths_map
            self.exception_categories = exception_categories
            self.available_vendors = available_vendors
            self.last_refreshed_at = refreshed_at
            self.load_duration_seconds = load_seconds
            self.cache_meta = cache_meta
            self._is_loaded = True
            self.last_error = None
            logger.info(
                "Celonis data cache refreshed successfully in %.3fs (cases=%s, events=%s, vendors=%s).",
                self.load_duration_seconds,
                self.cache_meta["total_cases"],
                self.cache_meta["total_events"],
                self.cache_meta["vendor_count"],
            )

    def is_stale(self) -> bool:
        with self._lock:
            return self._is_stale_locked()

    def get_age_seconds(self) -> Optional[float]:
        """Return seconds since last successful cache refresh, or None if never loaded."""
        with self._lock:
            if not self.last_refreshed_at:
                return None
            try:
                refreshed = datetime.fromisoformat(self.last_refreshed_at.replace("Z", "+00:00"))
                return (datetime.now(timezone.utc) - refreshed).total_seconds()
            except Exception:
                return None

    def get_data_freshness(self) -> Dict[str, Any]:
        """
        Return structured data-freshness metadata for API responses and UI consumption.

        Contract:
          last_refreshed        – ISO-8601 UTC timestamp of last successful refresh (or null)
          is_stale              – True when TTL has elapsed and background refresh is pending
          age_seconds           – Seconds since last refresh (null if never loaded)
          is_loaded             – Whether any cache snapshot exists
          refresh_in_progress   – Whether a background refresh is running right now
          max_staleness_seconds – Configured hard staleness ceiling
          exceeds_max_staleness – True when age_seconds > max_staleness_seconds
          data_available        – True when loaded AND not exceeding max staleness
        """
        with self._lock:
            age: Optional[float] = None
            exceeds_max = False
            if self.last_refreshed_at:
                try:
                    refreshed = datetime.fromisoformat(self.last_refreshed_at.replace("Z", "+00:00"))
                    age = (datetime.now(timezone.utc) - refreshed).total_seconds()
                    max_staleness = max(
                        int(getattr(settings, "CACHE_MAX_STALENESS_SECONDS", self.MAX_STALENESS_SECONDS) or self.MAX_STALENESS_SECONDS),
                        60,
                    )
                    exceeds_max = age > max_staleness
                except Exception:
                    pass

            return {
                "last_refreshed": self.last_refreshed_at,
                "is_stale": self._is_stale_locked(),
                "age_seconds": round(age, 1) if age is not None else None,
                "is_loaded": self._is_loaded,
                "refresh_in_progress": self._refresh_in_progress,
                "max_staleness_seconds": self.MAX_STALENESS_SECONDS,
                "exceeds_max_staleness": exceeds_max,
                "data_available": self._is_loaded and not exceeds_max,
                "last_error": self.last_error,
            }

    def get_case_table(self) -> List[Dict[str, Any]]:
        """Return full purchasing document header table for detailed views."""
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty case table due to refresh/load error: %s", str(e))
            return []
        with self._lock:
            return self.case_table_full_df.where(
                pd.notnull(self.case_table_full_df), None
            ).to_dict(orient="records")

    def get_cache_status(self) -> Dict[str, Any]:
        with self._lock:
            self._clear_stuck_refresh_locked()
            return {
                "is_loaded": self._is_loaded,
                "is_stale": self._is_stale_locked(),
                "last_refreshed_at": self.last_refreshed_at,
                "load_duration_seconds": self.load_duration_seconds,
                "total_events": int(self.cache_meta.get("total_events", 0)),
                "total_cases": int(self.cache_meta.get("total_cases", 0)),
                "vendors_count": int(self.cache_meta.get("vendor_count", 0)),
                "exception_categories_count": int(self.cache_meta.get("exception_categories_count", 0)),
                "wcm_olap_rows": int(self.cache_meta.get("wcm_olap_rows", 0)),
                "accounting_header_rows": int(self.cache_meta.get("accounting_header_rows", 0)),
                "wcm_group_count": int(self.cache_meta.get("wcm_group_count", 0)),
                "available_vendors": self.available_vendors,
                "last_error": self.last_error,
                "refresh_in_progress": self._refresh_in_progress,
                "refresh_started_at": self._refresh_started_at,
            }

    def reset_refresh_lock(self) -> None:
        with self._lock:
            self._refresh_in_progress = False
            self._refresh_started_at = None
            self._refresh_cond.notify_all()

    def _clear_stuck_refresh_locked(self) -> None:
        if not self._refresh_in_progress:
            return
        started_at = self._refresh_started_at
        if not started_at:
            return
        hard_timeout = max(int(getattr(settings, "CACHE_REFRESH_HARD_TIMEOUT_SECONDS", 900) or 900), 60)
        if (time.time() - started_at) <= hard_timeout:
            return
        self._refresh_in_progress = False
        self._refresh_started_at = None
        if not self.last_error:
            self.last_error = (
                f"Refresh lock cleared after exceeding hard timeout ({hard_timeout}s). "
                "You can trigger /api/cache/refresh again."
            )
        logger.warning("Cache refresh lock exceeded %ss and was reset.", hard_timeout)
        self._refresh_cond.notify_all()

    def get_process_context(self) -> Dict[str, Any]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving degraded process context due to refresh/load error: %s", str(e))
        with self._lock:
            if self.process_context:
                return self.process_context
            fallback = self._empty_process_context()
            fallback["degraded_mode"] = True
            fallback["degraded_reason"] = self.last_error or "Cache not loaded yet"
            return fallback

    def get_event_log(self) -> List[Dict[str, Any]]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty event log due to refresh/load error: %s", str(e))
        with self._lock:
            return self.event_log_df.where(pd.notnull(self.event_log_df), None).to_dict(orient="records")

    def get_vendor_stats_api(self) -> List[Dict[str, Any]]:
        """API-ready vendor stats with computed payment behavior from OLAP."""
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty vendor stats due to refresh/load error: %s", str(e))
        with self._lock:
            return self.vendor_stats  # This already has correct payment_behavior from _build_vendor_olap_summary

    def get_vendor_stats(self) -> List[Dict[str, Any]]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty vendor stats due to refresh/load error: %s", str(e))
        with self._lock:
            return self.vendor_stats

    def get_exception_categories(self) -> List[Dict[str, Any]]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty exception categories due to refresh/load error: %s", str(e))
        with self._lock:
            return self.exception_categories

    def get_exception_records(self, exception_type: str) -> List[Dict[str, Any]]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty exception records due to refresh/load error: %s", str(e))
        key = self._normalize_exception_key(exception_type)
        with self._lock:
            return self.exception_records_map.get(key, [])

    def get_all_exception_records(self) -> List[Dict[str, Any]]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving flattened exception records due to refresh/load error: %s", str(e))
        with self._lock:
            rows: List[Dict[str, Any]] = []
            seen: Set[str] = set()
            for record_list in self.exception_records_map.values():
                for row in record_list or []:
                    record_id = str(row.get("exception_id") or row.get("case_id") or "")
                    if record_id and record_id in seen:
                        continue
                    if record_id:
                        seen.add(record_id)
                    rows.append(self._to_jsonable(dict(row)))
            return rows

    def get_exception_workbench_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            categories = [self._to_jsonable(dict(row)) for row in (self.exception_categories or [])]
            rows: List[Dict[str, Any]] = []
            seen: set[str] = set()
            for record_list in self.exception_records_map.values():
                for row in record_list or []:
                    record_id = str(row.get("exception_id") or row.get("case_id") or "")
                    if record_id and record_id in seen:
                        continue
                    if record_id:
                        seen.add(record_id)
                    rows.append(self._to_jsonable(dict(row)))
            return {
                "categories": categories,
                "records": rows,
                "is_loaded": self._is_loaded,
                "is_stale": self._is_stale_locked(),
                "refresh_in_progress": self._refresh_in_progress,
                "last_error": self.last_error,
            }

    def get_representative_exception_case(self) -> Dict[str, Any]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving fallback representative case due to refresh/load error: %s", str(e))

        with self._lock:
            all_exception_rows: List[Dict[str, Any]] = []
            case_level = self.case_level_df.copy()
            for record_list in self.exception_records_map.values():
                all_exception_rows.extend(record_list or [])

            def score(row: Dict[str, Any]) -> tuple[float, float]:
                return (
                    float(row.get("invoice_amount") or row.get("value_at_risk") or 0.0),
                    float(row.get("dpo") or row.get("actual_dpo") or row.get("days_in_exception") or 0.0),
                )

            for row in sorted(all_exception_rows, key=score, reverse=True):
                case_id = str(row.get("case_id") or row.get("invoice_id") or "").strip()
                if not case_id:
                    continue
                if case_level.empty or "case_id" not in case_level.columns:
                    break
                mask = case_level["case_id"].astype(str) == case_id
                if "document_number" in case_level.columns:
                    mask = mask | (case_level["document_number"].astype(str) == case_id)
                match = case_level[mask]
                if match.empty:
                    continue
                case_row = match.iloc[0].to_dict()
                case_row["activity_trace"] = [str(x) for x in case_row.get("activity_trace", [])]
                return self._to_jsonable(case_row)

            if case_level.empty:
                return {}

            activity_l = case_level.get("activity_trace_text", pd.Series(dtype=str)).astype(str).str.lower().fillna("")
            priority_mask = activity_l.str.contains("exception|moved out|due date passed|payment term|block", regex=True, na=False)
            candidate_df = case_level[priority_mask] if priority_mask.any() else case_level
            if candidate_df.empty:
                return {}
            candidate_df = candidate_df.copy()
            candidate_df["invoice_amount"] = pd.to_numeric(candidate_df.get("invoice_amount"), errors="coerce").fillna(0.0)
            case_row = candidate_df.sort_values("invoice_amount", ascending=False).iloc[0].to_dict()
            case_row["activity_trace"] = [str(x) for x in case_row.get("activity_trace", [])]
            return self._to_jsonable(case_row)

    def get_vendor_records(self, vendor_id: str) -> List[Dict[str, Any]]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty vendor records due to refresh/load error: %s", str(e))
        normalized = str(vendor_id or "").strip().upper()
        with self._lock:
            return self.vendor_records_map.get(normalized, [])

    def get_invoice_case(self, invoice_id: str) -> Dict[str, Any]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty invoice case due to refresh/load error: %s", str(e))
        invoice_key = str(invoice_id or "").strip()
        if not invoice_key:
            return {}

        with self._lock:
            df = self.case_level_df
            if df.empty:
                return {}
            match = df[
                (df["case_id"].astype(str) == invoice_key)
                | (df["document_number"].astype(str) == invoice_key)
            ]
            if match.empty:
                return {}
            row = match.iloc[0].to_dict()
            row["activity_trace"] = [str(x) for x in row.get("activity_trace", [])]
            return self._to_jsonable(row)

    def get_vendor_paths(self, vendor_id: str) -> Dict[str, Any]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving fallback vendor paths due to refresh/load error: %s", str(e))
        normalized = str(vendor_id or "").strip().upper()
        with self._lock:
            return self.vendor_paths_map.get(
                normalized, {"vendor_id": vendor_id, "happy_paths": [], "exception_paths": []}
            )

    def _build_vendor_records_map(
            self,
            case_level: pd.DataFrame,
            exception_records_map: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Build a map of vendor_id -> list of case records for fast vendor lookup."""
        vendor_map: Dict[str, List[Dict[str, Any]]] = {}

        # Add case level records per vendor
        for _, row in case_level.iterrows():
            vid = str(row.get("vendor_id", "UNKNOWN") or "UNKNOWN").strip().upper()
            if not vid:
                vid = "UNKNOWN"
            vendor_map.setdefault(vid, []).append(self._to_jsonable(row.to_dict()))

        # Also add exception records per vendor
        for records in exception_records_map.values():
            for rec in records:
                vid = str(rec.get("vendor_id", "UNKNOWN") or "UNKNOWN").strip().upper()
                if not vid:
                    vid = "UNKNOWN"
                # Avoid duplicates — only add if not already present by case_id
                existing_case_ids = {
                    str(r.get("case_id", "")) for r in vendor_map.get(vid, [])
                }
                if str(rec.get("case_id", "")) not in existing_case_ids:
                    vendor_map.setdefault(vid, []).append(rec)

        return vendor_map

    def get_vendors(self) -> List[Dict[str, Any]]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving empty vendors list due to refresh/load error: %s", str(e))
        with self._lock:
            return [
                {
                    "vendor_id": vid,
                    "records_count": len(self.vendor_records_map.get(vid, [])),
                }
                for vid in self.available_vendors
            ]

    def get_context_coverage(self) -> Dict[str, Any]:
        try:
            self.ensure_loaded()
        except Exception as e:
            logger.warning("Serving degraded context coverage due to refresh/load error: %s", str(e))
        with self._lock:
            grouped = self.wcm_grouped_extract or {}
            groups = grouped.get("groups", {}) if isinstance(grouped, dict) else {}
            object_activity_groups = groups.get("object_activity_streams", []) if isinstance(groups, dict) else []
            event_stream_groups = groups.get("event_streams_by_object", []) if isinstance(groups, dict) else []
            object_master_groups = groups.get("object_master_tables", []) if isinstance(groups, dict) else []

            exception_case_total = int(sum(len(v or []) for v in self.exception_records_map.values()))
            category_breakdown = []
            for category in self.exception_categories:
                category_breakdown.append(
                    {
                        "category_id": category.get("category_id"),
                        "category_label": category.get("category_label"),
                        "case_count": int(category.get("case_count", 0) or 0),
                        "open_count": int(category.get("open_count", 0) or 0),
                        "closed_count": int(category.get("closed_count", 0) or 0),
                        "total_value": float(category.get("total_value", 0) or 0),
                    }
                )

            return {
                "refresh": {
                    "is_loaded": self._is_loaded,
                    "is_stale": self._is_stale_locked(),
                    "last_refreshed_at": self.last_refreshed_at,
                    "load_duration_seconds": self.load_duration_seconds,
                    "last_error": self.last_error,
                },
                "ingestion_scope": {
                    "wcm_context_mode": getattr(settings, "WCM_CONTEXT_MODE", "full"),
                    "activity_table": self.cache_meta.get("tables_loaded", ["", ""])[0] if self.cache_meta.get("tables_loaded") else None,
                    "case_table": self.cache_meta.get("tables_loaded", ["", "", ""])[1] if self.cache_meta.get("tables_loaded") else None,
                    "accounting_table": self.cache_meta.get("tables_loaded", ["", "", ""])[2] if self.cache_meta.get("tables_loaded") else None,
                    "grouped_selected_tables": (self.process_context.get("working_capital_source_summary", {}) or {}).get(
                        "grouped_selected_tables", []
                    ),
                    "grouped_include_event_tables": (self.process_context.get("working_capital_source_summary", {}) or {}).get(
                        "grouped_include_event_tables", False
                    ),
                },
                "coverage": {
                    "total_events": int(self.cache_meta.get("total_events", 0)),
                    "total_cases": int(self.cache_meta.get("total_cases", 0)),
                    "case_level_rows": int(len(self.case_level_df)),
                    "olap_rows": int(len(self.wcm_olap_df)),
                    "group_count": int(grouped.get("group_count", 0) if isinstance(grouped, dict) else 0),
                    "tables_extracted": int(grouped.get("tables_extracted", 0) if isinstance(grouped, dict) else 0),
                    "object_activity_groups": int(len(object_activity_groups)),
                    "event_stream_groups": int(len(event_stream_groups)),
                    "object_master_groups": int(len(object_master_groups)),
                    "exception_category_count": int(len(self.exception_categories)),
                    "exception_record_count": exception_case_total,
                    "vendor_count": int(len(self.available_vendors)),
                },
                "mapping_diagnostics": {
                    "olap_source_table": self.cache_meta.get("wcm_olap_source_table"),
                    "olap_selection_mode": self.cache_meta.get("wcm_olap_selection_mode"),
                    "olap_missing_fields": self.cache_meta.get("wcm_olap_missing_fields", []),
                    "olap_required_fields": self._required_olap_fields(),
                    "olap_missing_required_fields": [
                        f for f in (self.cache_meta.get("wcm_olap_missing_fields", []) or [])
                        if f in self._required_olap_fields()
                    ],
                    "has_missing_olap_mappings": len(
                        [
                            f
                            for f in (self.cache_meta.get("wcm_olap_missing_fields", []) or [])
                            if f in self._required_olap_fields()
                        ]
                    )
                    > 0,
                    "configured_explicit_table": getattr(settings, "WCM_OLAP_SOURCE_TABLE", ""),
                },
                "status_coverage": {
                    "open_closed_status": self.process_context.get("profile_view_summary", {}).get("status_distribution", {}),
                    "payment_status": self.process_context.get("profile_view_summary", {}).get("payment_status_distribution", {}),
                    "total_invoice_value": self.process_context.get("profile_view_summary", {}).get("total_invoice_value", 0),
                    "open_invoice_value": self.process_context.get("profile_view_summary", {}).get("open_invoice_value", 0),
                },
                "exception_coverage": {
                    "discovered_exception_types": self.process_context.get("discovered_exception_types", []),
                    "category_breakdown": category_breakdown,
                },
                "celonis_context_layer": self.process_context.get("celonis_context_layer", {}),
            }

    @staticmethod
    def _empty_process_context() -> Dict[str, Any]:
        return {
            "total_cases": 0,
            "total_events": 0,
            "activities": [],
            "golden_path": "",
            "golden_path_percentage": 0.0,
            "variants": [],
            "activity_durations": {},
            "throughput_times": [],
            "bottleneck": {"activity": "N/A", "duration_days": 0.0},
            "avg_end_to_end_days": 0.0,
            "case_durations": [],
            "exception_patterns": [],
            "exception_rate": 0.0,
            "decision_rules": [],
            "conformance_violations": [],
            "role_mappings": [],
            "vendor_stats": [],
            "connection_info": {},
            "profile_view_summary": {
                "total_invoices": 0,
                "status_distribution": {},
                "payment_status_distribution": {},
                "total_invoice_value": 0.0,
                "open_invoice_value": 0.0,
            },
            "discovered_exception_types": [],
            "working_capital_source_summary": {
                "mode": getattr(settings, "WCM_CONTEXT_MODE", "full"),
                "grouped_extract_group_count": 0,
                "grouped_extract_tables": 0,
                "olap_rows": 0,
                "includes_open_and_closed": False,
            },
            "celonis_context_layer": {
                "context_ready": False,
                "process_map": {"top_transitions": [], "bottleneck": {}, "golden_path": "", "golden_path_percentage": 0.0},
                "variants": {"top_variants": [], "variant_count": 0},
                "resources": {"role_activity_mappings": [], "mapping_count": 0},
                "events": {"total_events": 0, "total_cases": 0, "event_coverage_note": "No event data loaded."},
                "cycle_time": {"avg_end_to_end_days": 0.0, "throughput_transitions_analyzed": 0, "exception_rate_pct": 0.0},
                "exception_contexts": [],
                "sample_case_count_in_cache": 0,
            },
        }

    @staticmethod
    def _required_olap_fields() -> List[str]:
        return [
            "company_code",
            "vendor_id",
            "invoice_number",
            "invoice_line_item_number",
            "fiscal_year",
            "clearing_document_number",
            "baseline_date",
            "cleared_date",
            "invoice_payment_terms",
        ]

    def _build_enriched_event_log(
        self,
        event_log: pd.DataFrame,
        case_attrs: pd.DataFrame,
        accounting_attrs: pd.DataFrame,
        case_table_full: pd.DataFrame,
        celonis: CelonisService,
    ) -> pd.DataFrame:
        if event_log.empty:
            return event_log.copy()

        enriched = event_log.copy()
        if "document_number" in enriched.columns:
            enriched["document_number"] = enriched["document_number"].astype(str).str.strip()
        if "case_id" in enriched.columns:
            enriched["case_id"] = enriched["case_id"].astype(str).str.strip()
        if not case_attrs.empty:
            attrs = case_attrs.drop_duplicates(subset=["document_number"]).copy()
            attrs["document_number"] = attrs["document_number"].astype(str).str.strip()
            enriched = enriched.merge(attrs, on="document_number", how="left")

        if accounting_attrs is not None and not accounting_attrs.empty:
            accounting = accounting_attrs.copy()
            if "document_number" in accounting.columns:
                accounting["document_number"] = accounting["document_number"].astype(str).str.strip()
            if "invoice_number" in accounting.columns:
                accounting["invoice_number"] = accounting["invoice_number"].astype(str).str.strip()
            if "case_id" in accounting.columns:
                accounting["case_id"] = accounting["case_id"].astype(str).str.strip()

            merged = False
            if "document_number" in accounting.columns and accounting["document_number"].notna().any():
                doc_map = accounting.dropna(subset=["document_number"]).drop_duplicates(subset=["document_number"])
                enriched = enriched.merge(
                    doc_map,
                    on="document_number",
                    how="left",
                    suffixes=("", "_accounting"),
                )
                merged = True
            elif "case_id" in accounting.columns and accounting["case_id"].notna().any():
                case_map = accounting.dropna(subset=["case_id"]).drop_duplicates(subset=["case_id"])
                enriched = enriched.merge(
                    case_map,
                    on="case_id",
                    how="left",
                    suffixes=("", "_accounting"),
                )
                merged = True

            if not merged and "invoice_number" in accounting.columns and accounting["invoice_number"].notna().any():
                invoice_map = accounting.dropna(subset=["invoice_number"]).drop_duplicates(subset=["invoice_number"])
                enriched = enriched.merge(
                    invoice_map,
                    left_on="document_number",
                    right_on="invoice_number",
                    how="left",
                    suffixes=("", "_accounting"),
                )

        full = case_table_full.copy()
        if not full.empty:
            rename = {}
            if celonis.case_table_doc_col in full.columns:
                rename[celonis.case_table_doc_col] = "document_number"
            if celonis.vendor_col in full.columns:
                rename[celonis.vendor_col] = "vendor_id_full"
            if celonis.payment_terms_col in full.columns:
                rename[celonis.payment_terms_col] = "po_payment_terms"
            if celonis.currency_col in full.columns:
                rename[celonis.currency_col] = "currency_full"
            if celonis.amount_col in full.columns:
                rename[celonis.amount_col] = "invoice_amount_case_table"
            if "BSART" in full.columns:
                rename["BSART"] = "document_type"
            if "BUKRS" in full.columns:
                rename["BUKRS"] = "company_code"
            if "PROCSTAT" in full.columns:
                rename["PROCSTAT"] = "processing_status"
            if "ERNAM" in full.columns:
                rename["ERNAM"] = "created_by"
            if "FRGZU" in full.columns:
                rename["FRGZU"] = "approval_status"
            if "BEDAT" in full.columns:
                rename["BEDAT"] = "document_date"
            if "SUBMITDATE" in full.columns:
                rename["SUBMITDATE"] = "submit_date"
            if "ORDEREDDATE" in full.columns:
                rename["ORDEREDDATE"] = "ordered_date"
            if "CREATEDATE" in full.columns:
                rename["CREATEDATE"] = "create_date"
            if "AEDAT" in full.columns:
                rename["AEDAT"] = "changed_date"
            if "ZBD1T" in full.columns:
                rename["ZBD1T"] = "discount_days_1"
            if "ZBD2T" in full.columns:
                rename["ZBD2T"] = "discount_days_2"
            if "ZBD3T" in full.columns:
                rename["ZBD3T"] = "discount_days_3"
            full = full.rename(columns=rename)

            if "document_number" in full.columns:
                keep_cols = [c for c in [
                    "document_number",
                    "vendor_id_full",
                    "po_payment_terms",
                    "currency_full",
                    "invoice_amount_case_table",
                    "document_type",
                    "company_code",
                    "processing_status",
                    "created_by",
                    "approval_status",
                    "document_date",
                    "submit_date",
                    "ordered_date",
                    "create_date",
                    "changed_date",
                    "discount_days_1",
                    "discount_days_2",
                    "discount_days_3",
                ] if c in full.columns]
                full_small = full[keep_cols].drop_duplicates(subset=["document_number"])
                enriched = enriched.merge(full_small, on="document_number", how="left")

        if "vendor_id" not in enriched.columns:
            enriched["vendor_id"] = None
        if "vendor_id_full" in enriched.columns:
            enriched["vendor_id"] = enriched["vendor_id"].fillna(enriched["vendor_id_full"])

        if "payment_terms" not in enriched.columns:
            enriched["payment_terms"] = None
        if "po_payment_terms" in enriched.columns:
            enriched["payment_terms"] = enriched["payment_terms"].fillna(enriched["po_payment_terms"])
        if "invoice_payment_terms" in enriched.columns:
            enriched["payment_terms"] = enriched["payment_terms"].fillna(enriched["invoice_payment_terms"])

        if "currency" not in enriched.columns:
            enriched["currency"] = None
        if "currency_full" in enriched.columns:
            enriched["currency"] = enriched["currency"].fillna(enriched["currency_full"])

        if "invoice_amount_case_table" in enriched.columns:
            enriched["invoice_amount_case_table"] = pd.to_numeric(
                enriched["invoice_amount_case_table"], errors="coerce"
            )

        return enriched

    def _build_case_level_dataset(self, enriched_event_log: pd.DataFrame, process_context: Dict[str, Any]) -> pd.DataFrame:
        if enriched_event_log.empty:
            return pd.DataFrame()

        df = enriched_event_log.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.sort_values(["case_id", "timestamp"], na_position="last").reset_index(drop=True)

        grouped = []
        for case_id, g in df.groupby("case_id", sort=False):
            activities = g["activity"].dropna().astype(str).tolist()
            activity_trace_text = " → ".join(activities)
            start_ts = g["timestamp"].min()
            end_ts = g["timestamp"].max()
            amount_series = (
                pd.to_numeric(g["invoice_amount_case_table"], errors="coerce").dropna()
                if "invoice_amount_case_table" in g
                else pd.Series(dtype=float)
            )
            duration_days = (
                ((end_ts - start_ts).total_seconds() / 86400)
                if pd.notnull(start_ts) and pd.notnull(end_ts)
                else 0.0
            )
            row = {
                "case_id": case_id,
                "document_number": g["document_number"].dropna().astype(str).iloc[0] if g["document_number"].notna().any() else str(case_id),
                "invoice_number": g["invoice_number"].dropna().astype(str).iloc[0] if "invoice_number" in g and g["invoice_number"].notna().any() else None,
                "vendor_id": g["vendor_id"].dropna().astype(str).iloc[0] if g["vendor_id"].notna().any() else "UNKNOWN",
                "vendor_id_full": g["vendor_id_full"].dropna().astype(str).iloc[0] if "vendor_id_full" in g and g["vendor_id_full"].notna().any() else None,
                "payment_terms": g["payment_terms"].dropna().astype(str).iloc[0] if g["payment_terms"].notna().any() else None,
                "currency": g["currency"].dropna().astype(str).iloc[0] if g["currency"].notna().any() else None,
                "invoice_payment_terms": g["invoice_payment_terms"].dropna().astype(str).iloc[0] if "invoice_payment_terms" in g and g["invoice_payment_terms"].notna().any() else None,
                "po_payment_terms": g["po_payment_terms"].dropna().astype(str).iloc[0] if "po_payment_terms" in g and g["po_payment_terms"].notna().any() else None,
                "vendor_master_payment_terms": g["vendor_master_payment_terms"].dropna().astype(str).iloc[0] if "vendor_master_payment_terms" in g and g["vendor_master_payment_terms"].notna().any() else None,
                "clearing_document_number": g["clearing_document_number"].dropna().astype(str).iloc[0] if "clearing_document_number" in g and g["clearing_document_number"].notna().any() else None,
                "fiscal_year": g["fiscal_year"].dropna().astype(str).iloc[0] if "fiscal_year" in g and g["fiscal_year"].notna().any() else None,
                "invoice_amount_case_table": float(amount_series.iloc[0]) if not amount_series.empty else None,
                "document_type": g["document_type"].dropna().astype(str).iloc[0] if "document_type" in g and g["document_type"].notna().any() else None,
                "company_code": g["company_code"].dropna().astype(str).iloc[0] if "company_code" in g and g["company_code"].notna().any() else None,
                "processing_status": g["processing_status"].dropna().astype(str).iloc[0] if "processing_status" in g and g["processing_status"].notna().any() else None,
                "created_by": g["created_by"].dropna().astype(str).iloc[0] if "created_by" in g and g["created_by"].notna().any() else None,
                "approval_status": g["approval_status"].dropna().astype(str).iloc[0] if "approval_status" in g and g["approval_status"].notna().any() else None,
                "document_date": g["document_date"].dropna().iloc[0] if "document_date" in g and g["document_date"].notna().any() else None,
                "submit_date": g["submit_date"].dropna().iloc[0] if "submit_date" in g and g["submit_date"].notna().any() else None,
                "ordered_date": g["ordered_date"].dropna().iloc[0] if "ordered_date" in g and g["ordered_date"].notna().any() else None,
                "create_date": g["create_date"].dropna().iloc[0] if "create_date" in g and g["create_date"].notna().any() else None,
                "changed_date": g["changed_date"].dropna().iloc[0] if "changed_date" in g and g["changed_date"].notna().any() else None,
                "discount_days_1": g["discount_days_1"].dropna().iloc[0] if "discount_days_1" in g and g["discount_days_1"].notna().any() else None,
                "discount_days_2": g["discount_days_2"].dropna().iloc[0] if "discount_days_2" in g and g["discount_days_2"].notna().any() else None,
                "discount_days_3": g["discount_days_3"].dropna().iloc[0] if "discount_days_3" in g and g["discount_days_3"].notna().any() else None,
                "baseline_date": g["baseline_date"].dropna().iloc[0] if "baseline_date" in g and g["baseline_date"].notna().any() else None,
                "due_date": g["due_date"].dropna().iloc[0] if "due_date" in g and g["due_date"].notna().any() else None,
                "cleared_date": g["cleared_date"].dropna().iloc[0] if "cleared_date" in g and g["cleared_date"].notna().any() else None,
                "activity_trace": activities,
                "activity_trace_text": activity_trace_text,
                "start_time": start_ts,
                "end_time": end_ts,
                "duration_days": round(duration_days, 3),
            }
            grouped.append(row)

        case_level = pd.DataFrame(grouped)
        case_level["invoice_id"] = case_level["invoice_number"].fillna(case_level["document_number"])

        avg_end_to_end = float(process_context.get("avg_end_to_end_days", 0) or 0)
        case_level["estimated_processing_days"] = avg_end_to_end if avg_end_to_end > 0 else case_level["duration_days"].mean()

        case_level["invoice_amount"] = pd.to_numeric(case_level.get("invoice_amount_case_table"), errors="coerce")
        case_level["value_at_risk"] = None
        case_level["actual_dpo"] = case_level["duration_days"].fillna(0.0)
        case_level["potential_dpo"] = case_level["actual_dpo"]
        for dt_col in ["document_date", "submit_date", "ordered_date", "create_date", "changed_date", "baseline_date", "due_date", "cleared_date"]:
            if dt_col in case_level.columns:
                case_level[dt_col] = pd.to_datetime(case_level[dt_col], errors="coerce")

        discount_days = pd.to_numeric(case_level.get("discount_days_1"), errors="coerce").fillna(0)
        case_level["due_date_estimated"] = case_level["due_date"]
        missing_due_mask = case_level["due_date_estimated"].isna()
        case_level.loc[missing_due_mask, "due_date_estimated"] = (
            case_level.loc[missing_due_mask, "document_date"] + pd.to_timedelta(discount_days.loc[missing_due_mask], unit="D")
        )
        now = pd.Timestamp.now()
        case_level["days_until_due"] = (case_level["due_date_estimated"] - now).dt.total_seconds() / 86400
        return case_level

    def _enrich_case_level_with_olap(self, case_level: pd.DataFrame, olap_df: pd.DataFrame) -> pd.DataFrame:
        if case_level.empty or olap_df is None or olap_df.empty:
            return case_level

        work = olap_df.copy()
        for col in ["invoice_number", "invoice_value_usd", "converted_invoice_value_usd"]:
            if col not in work.columns:
                work[col] = None
        work["invoice_key"] = work["invoice_number"].astype(str).str.strip()
        work = work[work["invoice_key"].str.len() > 0]
        if work.empty:
            return case_level

        for num_col in ["invoice_value_usd", "converted_invoice_value_usd"]:
            work[num_col] = pd.to_numeric(work[num_col], errors="coerce")

        for dt_col in ["due_date", "baseline_date", "posting_date", "cleared_date"]:
            if dt_col in work.columns:
                work[dt_col] = pd.to_datetime(work[dt_col], errors="coerce")

        def first_non_null(series: pd.Series):
            valid = series.dropna()
            return valid.iloc[0] if not valid.empty else None

        grouped = (
            work.groupby("invoice_key", dropna=False)
            .agg(
                invoice_amount_olap=("converted_invoice_value_usd", "sum"),
                invoice_amount_olap_fallback=("invoice_value_usd", "sum"),
                payment_status=("payment_status", first_non_null),
                invoice_payment_terms=("invoice_payment_terms", first_non_null),
                po_payment_terms=("po_payment_terms", first_non_null),
                vendor_master_payment_terms=("vendor_master_payment_terms", first_non_null),
                recommendation=("recommendation", first_non_null),
                due_date=("due_date", "max"),
                baseline_date=("baseline_date", "max"),
                posting_date=("posting_date", "max"),
                cleared_date=("cleared_date", "max"),
                clearing_document_number=("clearing_document_number", first_non_null),
            )
            .reset_index()
        )
        grouped["invoice_amount_olap"] = grouped["invoice_amount_olap"].fillna(grouped["invoice_amount_olap_fallback"])
        grouped = grouped.drop(columns=["invoice_amount_olap_fallback"])

        merged = case_level.copy()
        merged["invoice_key"] = merged["invoice_id"].astype(str).str.strip()
        merged = merged.merge(grouped, on="invoice_key", how="left")
        merged = merged.drop(columns=["invoice_key"])
        merged["invoice_amount"] = pd.to_numeric(merged.get("invoice_amount"), errors="coerce").fillna(
            pd.to_numeric(merged.get("invoice_amount_olap"), errors="coerce")
        )

        for dt_col in ["due_date", "baseline_date", "posting_date", "cleared_date"]:
            if dt_col in merged.columns:
                merged[dt_col] = pd.to_datetime(merged[dt_col], errors="coerce")

        if "invoice_amount" in merged.columns:
            merged["value_at_risk"] = pd.to_numeric(merged["invoice_amount"], errors="coerce").fillna(0.0)

        status = merged.get("payment_status", pd.Series(dtype=str)).astype(str).str.lower()
        processing = merged.get("processing_status", pd.Series(dtype=str)).astype(str).str.lower()
        has_clearing = merged.get("clearing_document_number", pd.Series(dtype=str)).notna()
        merged["open_closed_status"] = "OPEN"
        merged.loc[
            status.str.contains("closed|paid", na=False) | has_clearing | processing.str.contains("closed", na=False),
            "open_closed_status",
        ] = "CLOSED"
        merged.loc[
            status.str.contains("late", na=False) | processing.str.contains("late", na=False),
            "open_closed_status",
        ] = "PAID_LATE"
        merged.loc[
            status.str.contains("early", na=False) | processing.str.contains("early", na=False),
            "open_closed_status",
        ] = "PAID_EARLY"
        merged.loc[
            status.str.contains("on time|ontime", na=False) | processing.str.contains("on time|ontime", na=False),
            "open_closed_status",
        ] = "PAID_ON_TIME"

        if "due_date" in merged.columns:
            now = pd.Timestamp.now()
            merged["days_until_due"] = (
                merged["due_date"] - now
            ).dt.total_seconds().div(86400).fillna(merged.get("days_until_due", 0))

        return merged

    def _backfill_invoice_amounts_from_aux_tables(
        self,
        case_level: pd.DataFrame,
        celonis: CelonisService,
    ) -> pd.DataFrame:
        if case_level.empty:
            return case_level

        merged = case_level.copy()
        current_amount = pd.to_numeric(merged.get("invoice_amount"), errors="coerce").fillna(0.0)
        if float(current_amount.sum()) > 0:
            merged["value_at_risk"] = current_amount
            return merged

        table_candidates = [
            ("t_o_custom_DocumentItemIncomingInvoice", ["BELNR", "WRBTR"]),
            ("t_o_custom_PurchasingDocumentItem", ["EBELN", "NETWR"]),
            ("t_o_custom_AccountingDocumentSegment", ["BELNR", "DMBTR"]),
        ]

        amount_map: Dict[str, float] = {}
        for table_name, cols in table_candidates:
            try:
                available = set(celonis.list_columns(table_name))
                if not all(col in available for col in cols):
                    continue
                df = celonis.get_table_data(table_name=table_name, columns=cols, use_cache=False)
                if df is None or df.empty:
                    continue
                key_col, amount_col = cols
                work = df[[key_col, amount_col]].copy()
                work[key_col] = work[key_col].astype(str).str.strip()
                work[amount_col] = pd.to_numeric(work[amount_col], errors="coerce").fillna(0.0)
                agg = work.groupby(key_col)[amount_col].sum().to_dict()
                for key, value in agg.items():
                    amount_map[key] = amount_map.get(key, 0.0) + float(value or 0.0)
            except Exception as e:
                logger.warning("Aux amount backfill skipped for %s: %s", table_name, str(e))
                continue

        if not amount_map:
            merged["value_at_risk"] = current_amount
            return merged

        merged["invoice_amount"] = merged["invoice_id"].astype(str).str.strip().map(amount_map).fillna(current_amount)
        merged["invoice_amount"] = pd.to_numeric(merged["invoice_amount"], errors="coerce").fillna(0.0)
        merged["value_at_risk"] = merged["invoice_amount"]
        return merged

    def _build_exception_records_map(
        self,
        case_level: pd.DataFrame,
        process_context: Dict[str, Any],
    ) -> Dict[str, List[Dict[str, Any]]]:
        if case_level.empty:
            return {self._normalize_exception_key(label): [] for label in self.EXCEPTION_LABELS}

        records_map: Dict[str, List[Dict[str, Any]]] = {}
        avg_resolution = 0.0
        for p in process_context.get("exception_patterns", []) or []:
            if "exception" in str(p.get("exception_type", "")).lower():
                avg_resolution = float(p.get("avg_resolution_time_days", 0) or 0)
                break

        def mk_record(base_row: pd.Series, ex_type: str, extra: Dict[str, Any]) -> Dict[str, Any]:
            record = {
                "exception_id": f"{self._normalize_exception_key(ex_type)}-{base_row['case_id']}",
                "exception_type": ex_type,
                "invoice_id": base_row.get("invoice_id"),
                "document_number": base_row.get("document_number"),
                "vendor_id": base_row.get("vendor_id"),
                "vendor_name": base_row.get("vendor_id"),
                "invoice_amount": base_row.get("invoice_amount"),
                "currency": base_row.get("currency"),
                "case_id": base_row.get("case_id"),
                "source": "Celonis",
            }
            record.update(extra)
            return self._to_jsonable(record)

        activity_l = case_level["activity_trace_text"].str.lower().fillna("")
        empty_str_series = pd.Series(index=case_level.index, dtype=str)
        payment_terms_l = case_level["payment_terms"].astype(str).str.lower().fillna("")
        invoice_terms_l = case_level.get("invoice_payment_terms", empty_str_series).astype(str).str.lower().fillna("")
        po_terms_l = case_level.get("po_payment_terms", empty_str_series).astype(str).str.lower().fillna("")
        vendor_terms_l = case_level.get("vendor_master_payment_terms", empty_str_series).astype(str).str.lower().fillna("")
        processing_l = case_level["processing_status"].astype(str).str.lower().fillna("")
        payment_status_l = case_level.get("payment_status", empty_str_series).astype(str).str.lower().fillna("")
        open_closed_status_l = case_level.get("open_closed_status", empty_str_series).astype(str).str.lower().fillna("")
        days_until_due = pd.to_numeric(case_level.get("days_until_due"), errors="coerce")
        estimated_days = pd.to_numeric(case_level.get("estimated_processing_days"), errors="coerce").fillna(0).clip(lower=1)

        payment_terms_mask = (
            (
                invoice_terms_l.ne("")
                & (
                    (po_terms_l.ne("") & invoice_terms_l.ne(po_terms_l))
                    | (vendor_terms_l.ne("") & invoice_terms_l.ne(vendor_terms_l))
                )
            )
            | payment_terms_l.isin(["0", "0000", "immediate", "0 days"])
        )
        invoice_exception_mask = (
            processing_l.str.contains("exception|blocked|moved out", regex=True, na=False)
            | activity_l.str.contains(r"\bexception\b|moved out|block", regex=True, na=False)
        )
        short_terms_mask = (
            invoice_terms_l.isin(["0", "0000", "immediate", "0 days"])
            | payment_terms_l.isin(["0", "0000", "immediate", "0 days"])
        )
        early_payment_mask = (
            open_closed_status_l.eq("paid_early")
            | payment_status_l.str.contains("paid early|early", regex=True, na=False)
        )
        paid_late_mask = (
            open_closed_status_l.eq("paid_late")
            | payment_status_l.str.contains("paid late|late", regex=True, na=False)
            | (days_until_due.fillna(9999) < 0)
        )
        open_risk_mask = (
            (
                open_closed_status_l.eq("open")
                | payment_status_l.str.contains("open|pending", regex=True, na=False)
                | processing_l.str.contains("open|pending", regex=True, na=False)
            )
            & days_until_due.notna()
            & (days_until_due <= estimated_days)
        )

        records_map[self._normalize_exception_key("Payment Terms Mismatch")] = [
            mk_record(
                r,
                "Payment Terms Mismatch",
                {
                    "invoice_payment_terms": r.get("invoice_payment_terms") or r.get("payment_terms"),
                    "po_payment_terms": r.get("po_payment_terms") or r.get("payment_terms"),
                    "vendor_master_payment_terms": r.get("vendor_master_payment_terms"),
                    "risk_level": "MEDIUM",
                    "status": "OPEN",
                    "value_at_risk": r.get("invoice_amount"),
                    "dpo": r.get("actual_dpo"),
                },
            )
            for _, r in case_level[payment_terms_mask].iterrows()
        ]

        records_map[self._normalize_exception_key("Invoices with Exception")] = [
            mk_record(
                r,
                "Invoices with Exception",
                {
                    "days_in_exception": avg_resolution if avg_resolution > 0 else r.get("actual_dpo"),
                    "avg_resolution_time_days": avg_resolution,
                    "dpo": r.get("actual_dpo"),
                    "risk_level": "CRITICAL",
                },
            )
            for _, r in case_level[invoice_exception_mask].iterrows()
        ]

        records_map[self._normalize_exception_key("Short Payment Terms (0 Days)")] = [
            mk_record(
                r,
                "Short Payment Terms (0 Days)",
                {
                    "payment_terms": r.get("payment_terms"),
                    "dpo": r.get("actual_dpo"),
                    "risk_level": "HIGH",
                },
            )
            for _, r in case_level[short_terms_mask].iterrows()
        ]

        records_map[self._normalize_exception_key("Early Payment / DPO 0-7 Days")] = [
            mk_record(
                r,
                "Early Payment / DPO 0-7 Days",
                {
                    "actual_dpo": r.get("actual_dpo"),
                    "potential_dpo": max(float(r.get("estimated_processing_days") or 0), float(r.get("actual_dpo") or 0)),
                    "optimization_value": None,
                    "risk_level": "LOW",
                },
            )
            for _, r in case_level[early_payment_mask].iterrows()
        ]

        records_map[self._normalize_exception_key("Paid Late")] = [
            mk_record(
                r,
                "Paid Late",
                {
                    "days_late": max(float(r.get("actual_dpo") or 0) - float(r.get("estimated_processing_days") or 0), 0.0),
                    "dpo": r.get("actual_dpo"),
                    "risk_level": "HIGH",
                },
            )
            for _, r in case_level[paid_late_mask].iterrows()
        ]

        records_map[self._normalize_exception_key("Open Invoices at Risk")] = [
            mk_record(
                r,
                "Open Invoices at Risk",
                {
                    "days_until_due": r.get("days_until_due"),
                    "estimated_processing_days": r.get("estimated_processing_days"),
                    "risk_level": "HIGH" if float(r.get("actual_dpo") or 0) > float(r.get("estimated_processing_days") or 0) else "MEDIUM",
                },
            )
            for _, r in case_level[open_risk_mask].iterrows()
        ]

        # Dynamic individual exception discovery from activity paths.
        dynamic_categories = self._discover_activity_exception_categories(case_level, process_context)
        for category in dynamic_categories:
            label = category["label"]
            mask = category["mask"]
            records_map[self._normalize_exception_key(label)] = [
                mk_record(
                    r,
                    label,
                    {
                        "status": r.get("open_closed_status", "OPEN"),
                        "risk_level": category["risk_level"],
                        "days_until_due": r.get("days_until_due"),
                        "dpo": r.get("actual_dpo"),
                        "exception_signal": category.get("signal"),
                    },
                )
                for _, r in case_level[mask].iterrows()
            ]

        return records_map

    def _build_vendor_records_map(
        self,
        case_level: pd.DataFrame,
        exception_records_map: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        vendor_map: Dict[str, List[Dict[str, Any]]] = {}

        def alias_keys(payload: Dict[str, Any]) -> List[str]:
            keys: List[str] = []
            for raw in [
                payload.get("vendor_id"),
                payload.get("vendor_id_full"),
                payload.get("vendor_lifnr"),
            ]:
                normalized = str(raw or "").strip().upper()
                if normalized and normalized not in keys:
                    keys.append(normalized)
            return keys

        for _, row in case_level.iterrows():
            row_payload = self._to_jsonable(row.to_dict())
            for vid in alias_keys(row_payload) or ["UNKNOWN"]:
                vendor_map.setdefault(vid, []).append(row_payload)

        for records in exception_records_map.values():
            for rec in records:
                for vid in alias_keys(rec) or ["UNKNOWN"]:
                    vendor_map.setdefault(vid, []).append(rec)

        return vendor_map

    def _build_vendor_paths_map(self, celonis: CelonisService, case_level: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        vendor_alias_map: Dict[str, List[str]] = {}

        for _, row in case_level.iterrows():
            primary = str(row.get("vendor_id", "") or "").strip().upper()
            full = str(row.get("vendor_id_full", "") or "").strip().upper()
            aliases = [value for value in [primary, full] if value]
            if not aliases:
                continue
            vendor_alias_map.setdefault(primary or full, [])
            for alias in aliases:
                if alias not in vendor_alias_map[primary or full]:
                    vendor_alias_map[primary or full].append(alias)

        for vid, aliases in vendor_alias_map.items():
            try:
                resolved = None
                ordered_candidates = sorted(
                    aliases,
                    key=lambda value: (
                        0 if value.isdigit() else 1,
                        -len(value),
                    ),
                )
                for candidate in ordered_candidates:
                    payload = celonis.get_vendor_paths(candidate)
                    if payload.get("happy_paths") or payload.get("exception_paths"):
                        resolved = payload
                        break
                    if resolved is None:
                        resolved = payload

                resolved = resolved or {"vendor_id": vid, "happy_paths": [], "exception_paths": []}
                canonical = {**resolved, "vendor_id": vid}
                for alias in aliases:
                    result[alias] = canonical
            except Exception as e:
                logger.warning("Failed vendor path precompute for vendor %s: %s", vid, str(e))
                empty_payload = {"vendor_id": vid, "happy_paths": [], "exception_paths": []}
                for alias in aliases:
                    result[alias] = empty_payload
        return result

    def _build_vendor_stats(
        self,
        case_level: pd.DataFrame,
        olap_df: pd.DataFrame,
        process_context: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if case_level.empty:
            return []

        work = case_level.copy()
        work["vendor_id"] = work.get("vendor_id", pd.Series(dtype=str)).fillna("UNKNOWN").astype(str).str.strip()
        if "vendor_id_full" not in work.columns:
            work["vendor_id_full"] = work["vendor_id"]
        work["vendor_id_full"] = work.get("vendor_id_full", pd.Series(dtype=str)).fillna(work["vendor_id"]).astype(str).str.strip()
        work["invoice_amount"] = pd.to_numeric(work.get("invoice_amount"), errors="coerce").fillna(0.0)
        work["actual_dpo"] = pd.to_numeric(work.get("actual_dpo"), errors="coerce").fillna(0.0)
        work["is_exception_case"] = work.get("activity_trace_text", pd.Series(dtype=str)).astype(str).str.lower().str.contains(
            "exception|moved out|due date passed|short payment|payment terms mismatch|block",
            regex=True,
            na=False,
        )

        event_stats = (
            work.groupby("vendor_id", dropna=False)
            .agg(
                total_cases=("case_id", "nunique"),
                total_value=("invoice_amount", "sum"),
                avg_dpo=("actual_dpo", "mean"),
                vendor_lifnr=("vendor_id_full", lambda s: s.dropna().astype(str).iloc[0] if not s.dropna().empty else None),
                payment_terms=("payment_terms", "first"),
                currency=("currency", "first"),
                exception_case_count=("is_exception_case", "sum"),
            )
            .reset_index()
        )
        event_stats["exception_rate"] = (
            event_stats["exception_case_count"] / event_stats["total_cases"].replace(0, pd.NA) * 100
        ).fillna(0.0)

        variant_source = process_context.get("vendor_stats", []) or []
        variant_map = {
            str(row.get("vendor_id", "")).strip(): row
            for row in variant_source
            if str(row.get("vendor_id", "")).strip()
        }

        olap_summary = self._build_vendor_olap_summary(olap_df)
        rows: List[Dict[str, Any]] = []

        for _, row in event_stats.iterrows():
            vendor_id = str(row.get("vendor_id", "UNKNOWN")).strip()
            event_count = int(work[work["vendor_id"] == vendor_id].shape[0])
            event_variant = variant_map.get(vendor_id, {})
            olap_vendor = olap_summary.get(vendor_id, {})
            payment_behavior = olap_vendor.get("payment_behavior") or self._derive_payment_behavior_from_case_level(
                work[work["vendor_id"] == vendor_id]
            )
            exception_breakdown = self._derive_vendor_exception_breakdown(
                work[work["vendor_id"] == vendor_id],
                olap_vendor,
            )

            total_cases = int(row.get("total_cases", 0) or 0)
            exception_case_count = int(row.get("exception_case_count", 0) or 0)
            exception_rate = float(row.get("exception_rate", 0.0) or 0.0)
            avg_dpo = float(row.get("avg_dpo", 0.0) or 0.0)

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
                    "vendor_lifnr": str(row.get("vendor_lifnr") or vendor_id).strip(),
                    "total_cases": total_cases,
                    "event_count": event_count,
                    "total_value": round(float(row.get("total_value", 0.0) or 0.0), 2),
                    "exception_case_count": exception_case_count,
                    "exception_rate": round(exception_rate, 2),
                    "avg_dpo": round(avg_dpo, 2),
                    "payment_behavior": payment_behavior,
                    "risk_score": risk,
                    "payment_terms": row.get("payment_terms"),
                    "currency": row.get("currency"),
                    "exception_breakdown": exception_breakdown,
                    "most_common_variant": event_variant.get("most_common_variant", ""),
                    "most_common_variant_case_count": int(event_variant.get("most_common_variant_case_count", 0) or 0),
                    "duration_vs_overall_days": float(event_variant.get("duration_vs_overall_days", 0) or 0),
                    "exception_rate_vs_overall_pct": float(event_variant.get("exception_rate_vs_overall_pct", 0) or 0),
                    "olap_metrics": olap_vendor.get("metrics", {}),
                }
            )

        rows.sort(key=lambda item: (float(item.get("total_value", 0) or 0), int(item.get("total_cases", 0) or 0)), reverse=True)
        return rows

    def _build_vendor_olap_summary(self, olap_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        if olap_df is None or olap_df.empty:
            return {}

        work = olap_df.copy()
        if "vendor_id" not in work.columns:
            return {}

        work["vendor_id"] = work["vendor_id"].fillna("UNKNOWN").astype(str).str.strip()
        work["invoice_value"] = pd.to_numeric(
            work.get("converted_invoice_value_usd"), errors="coerce"
        ).fillna(pd.to_numeric(work.get("invoice_value_usd"), errors="coerce")).fillna(0.0)
        for dt_col in ["baseline_date", "due_date", "posting_date", "cleared_date"]:
            if dt_col in work.columns:
                work[dt_col] = pd.to_datetime(work[dt_col], errors="coerce")

        work["payment_bucket"] = work.apply(self._derive_olap_payment_bucket, axis=1)
        work["invoice_pt_days"] = work.get("invoice_payment_terms").apply(self._extract_payment_days) if "invoice_payment_terms" in work.columns else 0
        work["po_pt_days"] = work.get("po_payment_terms").apply(self._extract_payment_days) if "po_payment_terms" in work.columns else 0
        work["vendor_pt_days"] = work.get("vendor_master_payment_terms").apply(self._extract_payment_days) if "vendor_master_payment_terms" in work.columns else 0

        vendor_summary: Dict[str, Dict[str, Any]] = {}
        for vendor_id, group in work.groupby("vendor_id", dropna=False):
            total_rows = int(len(group))
            bucket_counts = group["payment_bucket"].value_counts(dropna=False).to_dict()
            mismatch_mask = (
                (
                    group.get("invoice_payment_terms", pd.Series(index=group.index)).fillna("").astype(str).str.strip()
                    != group.get("vendor_master_payment_terms", pd.Series(index=group.index)).fillna("").astype(str).str.strip()
                )
                & group.get("vendor_master_payment_terms", pd.Series(index=group.index)).notna()
            ) | (
                (
                    group.get("invoice_payment_terms", pd.Series(index=group.index)).fillna("").astype(str).str.strip()
                    != group.get("po_payment_terms", pd.Series(index=group.index)).fillna("").astype(str).str.strip()
                )
                & group.get("po_payment_terms", pd.Series(index=group.index)).notna()
            )
            short_terms_mask = (
                (group["invoice_pt_days"] > 0)
                & (
                    ((group["vendor_pt_days"] > 0) & (group["invoice_pt_days"] < group["vendor_pt_days"]))
                    | ((group["po_pt_days"] > 0) & (group["invoice_pt_days"] < group["po_pt_days"]))
                )
            ) | group.get("invoice_payment_terms", pd.Series(index=group.index)).fillna("").astype(str).str.contains("0", na=False)
            early_mask = group["payment_bucket"].eq("paid_early")

            vendor_summary[str(vendor_id)] = {
                "payment_behavior": {
                    "on_time_pct": round(bucket_counts.get("paid_on_time", 0) / total_rows * 100, 1) if total_rows else 0.0,
                    "early_pct": round(bucket_counts.get("paid_early", 0) / total_rows * 100, 1) if total_rows else 0.0,
                    "late_pct": round(bucket_counts.get("paid_late", 0) / total_rows * 100, 1) if total_rows else 0.0,
                    "open_pct": round(bucket_counts.get("open", 0) / total_rows * 100, 1) if total_rows else 0.0,
                },
                "metrics": {
                    "olap_row_count": total_rows,
                    "cleared_row_count": int((group["payment_bucket"] != "open").sum()),
                },
                "exception_breakdown": {
                    "payment_terms_mismatch": {
                        "count": int(mismatch_mask.fillna(False).sum()),
                        "value": round(float(group.loc[mismatch_mask.fillna(False), "invoice_value"].sum()), 2),
                    },
                    "short_payment_terms": {
                        "count": int(short_terms_mask.fillna(False).sum()),
                        "value": round(float(group.loc[short_terms_mask.fillna(False), "invoice_value"].sum()), 2),
                    },
                    "early_payment": {
                        "count": int(early_mask.fillna(False).sum()),
                        "value": round(float(group.loc[early_mask.fillna(False), "invoice_value"].sum()), 2),
                    },
                },
            }
        return vendor_summary

    def _derive_vendor_exception_breakdown(
        self,
        vendor_cases: pd.DataFrame,
        olap_vendor: Dict[str, Any],
    ) -> Dict[str, Any]:
        total_cases = max(int(vendor_cases["case_id"].nunique()) if not vendor_cases.empty else 0, 1)
        olap_total = max(int(((olap_vendor or {}).get("metrics", {}) or {}).get("olap_row_count", 0) or 0), 1)
        invoice_exception_mask = vendor_cases.get("activity_trace_text", pd.Series(dtype=str)).astype(str).str.lower().str.contains(
            "exception|moved out|block|due date passed",
            regex=True,
            na=False,
        )
        invoice_exception_count = int(invoice_exception_mask.sum())
        invoice_exception_value = float(vendor_cases.loc[invoice_exception_mask, "invoice_amount"].sum()) if not vendor_cases.empty else 0.0
        invoice_exception_dpo = float(vendor_cases.loc[invoice_exception_mask, "actual_dpo"].mean()) if invoice_exception_count else 0.0

        olap_breakdown = olap_vendor.get("exception_breakdown", {}) if isinstance(olap_vendor, dict) else {}
        payment_terms = olap_breakdown.get("payment_terms_mismatch", {})
        short_terms = olap_breakdown.get("short_payment_terms", {})
        early_payment = olap_breakdown.get("early_payment", {})

        return {
            "payment_terms_mismatch": {
                "count": int(payment_terms.get("count", 0) or 0),
                "percentage": round((int(payment_terms.get("count", 0) or 0) / olap_total) * 100, 1),
                "value": round(float(payment_terms.get("value", 0.0) or 0.0), 2),
            },
            "invoice_exception": {
                "count": invoice_exception_count,
                "percentage": round((invoice_exception_count / total_cases) * 100, 1),
                "avg_dpo": round(invoice_exception_dpo, 2),
                "value": round(invoice_exception_value, 2),
                "time_stuck_days": round(invoice_exception_dpo, 1),
            },
            "short_payment_terms": {
                "count": int(short_terms.get("count", 0) or 0),
                "percentage": round((int(short_terms.get("count", 0) or 0) / olap_total) * 100, 1),
                "value": round(float(short_terms.get("value", 0.0) or 0.0), 2),
                "risk_level": "HIGH" if int(short_terms.get("count", 0) or 0) else "LOW",
            },
            "early_payment": {
                "count": int(early_payment.get("count", 0) or 0),
                "percentage": round((int(early_payment.get("count", 0) or 0) / olap_total) * 100, 1),
                "optimization_value": round(float(early_payment.get("value", 0.0) or 0.0), 2),
                "value": round(float(early_payment.get("value", 0.0) or 0.0), 2),
            },
        }

    def _derive_payment_behavior_from_case_level(self, vendor_cases: pd.DataFrame) -> Dict[str, float]:
        if vendor_cases.empty:
            return {"on_time_pct": 0.0, "early_pct": 0.0, "late_pct": 0.0, "open_pct": 0.0}

        status = vendor_cases.get("open_closed_status", pd.Series(dtype=str)).astype(str).str.upper()
        total = max(int(len(vendor_cases)), 1)
        return {
            "on_time_pct": round(status.eq("PAID_ON_TIME").sum() / total * 100, 1),
            "early_pct": round(status.eq("PAID_EARLY").sum() / total * 100, 1),
            "late_pct": round(status.eq("PAID_LATE").sum() / total * 100, 1),
            "open_pct": round(status.eq("OPEN").sum() / total * 100, 1),
        }

    def _derive_olap_payment_bucket(self, row: pd.Series) -> str:
        clearing_doc = str(row.get("clearing_document_number") or "").strip()
        cleared_date = row.get("cleared_date")
        reference_date = row.get("due_date") if pd.notna(row.get("due_date")) else row.get("baseline_date")

        if not clearing_doc and pd.isna(cleared_date):
            return "open"
        if pd.isna(cleared_date) or pd.isna(reference_date):
            return "paid_on_time"
        if cleared_date < reference_date:
            return "paid_early"
        if cleared_date > reference_date:
            return "paid_late"
        return "paid_on_time"

    def _extract_payment_days(self, value: Any) -> int:
        text = str(value or "").strip().upper()
        if not text:
            return 0
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            return 0
        try:
            return int(digits)
        except Exception:
            return 0

    def _build_exception_categories(self, records_map: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        categories = []
        keys_in_order: List[str] = [self._normalize_exception_key(label) for label in self.EXCEPTION_LABELS]

        for key in keys_in_order:
            rows = records_map.get(key, [])
            if rows:
                label = rows[0].get("exception_type", key.replace("_", " ").title())
            else:
                label = key.replace("_", " ").title()

            total_value = 0.0
            open_count = 0
            closed_count = 0
            for r in rows:
                try:
                    total_value += float(r.get("invoice_amount") or 0)
                except Exception:
                    continue
                status = str(r.get("status", "")).upper()
                if "OPEN" in status:
                    open_count += 1
                if "CLOSED" in status or "PAID" in status:
                    closed_count += 1

            categories.append(
                {
                    "category_id": key,
                    "category_label": label,
                    "supported_by_context": len(rows) > 0,
                    "case_count": len(rows),
                    "open_count": open_count,
                    "closed_count": closed_count,
                    "total_value": round(total_value, 2),
                }
            )
        return categories

    def _discover_activity_exception_categories(
        self,
        case_level: pd.DataFrame,
        process_context: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        categories: List[Dict[str, Any]] = []
        activity_l = case_level["activity_trace_text"].str.lower().fillna("")
        keyword_map = [
            ("GR Required", r"\bgr required\b|\bgoods receipt\b", "HIGH"),
            ("PO Not Released", r"\bpo not released\b|\bnot released\b", "HIGH"),
            ("Price Mismatch", r"\bprice mismatch\b|\bprice variance\b", "HIGH"),
            ("Quantity Mismatch", r"\bquantity mismatch\b|\bqty mismatch\b", "HIGH"),
            ("Tax Mismatch", r"\btax mismatch\b|\btax code\b", "HIGH"),
            ("Invoice Exception Start", r"\binvoiceexceptionstart\b|\binvoice exception start\b", "MEDIUM"),
            ("Invoice Exception End", r"\binvoiceexceptionend\b|\binvoice exception end\b", "LOW"),
            ("Moved Out of VIM", r"\bmoved out\b|\bmoved out of vim\b", "HIGH"),
            ("Due Date Passed", r"\bdue date passed\b", "HIGH"),
            ("Blocked Invoice or PO", r"\bblock\b|\bblocked\b", "HIGH"),
        ]
        for label, pattern, risk in keyword_map:
            mask = activity_l.str.contains(pattern, regex=True, na=False)
            if int(mask.sum()) == 0:
                continue
            categories.append({"label": label, "mask": mask, "risk_level": risk, "signal": pattern})

        # Add process-context exception patterns as discoverable categories.
        for pattern in process_context.get("exception_patterns", []) or []:
            label = str(pattern.get("exception_type", "")).strip()
            if not label:
                continue
            norm = self._normalize_exception_key(label)
            if any(self._normalize_exception_key(c["label"]) == norm for c in categories):
                continue
            trigger = str(pattern.get("trigger_condition", "")).lower()
            tokens = re.findall(r"[a-zA-Z]{4,}", trigger)
            if not tokens:
                continue
            regex = "|".join(sorted(set(re.escape(t) for t in tokens if t not in {"activity", "contains", "with"})))
            if not regex:
                continue
            mask = activity_l.str.contains(regex, regex=True, na=False)
            if int(mask.sum()) == 0:
                continue
            categories.append({"label": label, "mask": mask, "risk_level": "HIGH", "signal": regex})

        return categories

    def _build_profile_summary(self, case_level: pd.DataFrame, olap_df: pd.DataFrame) -> Dict[str, Any]:
        if case_level.empty:
            return {
                "total_invoices": 0,
                "status_distribution": {},
                "payment_status_distribution": {},
                "total_invoice_value": 0.0,
                "open_invoice_value": 0.0,
            }

        work = case_level.copy()
        work["invoice_amount"] = pd.to_numeric(work.get("invoice_amount"), errors="coerce").fillna(0.0)
        status_counts = work.get("open_closed_status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict()
        payment_counts = work.get("payment_status", pd.Series(dtype=str)).fillna("UNKNOWN").value_counts(dropna=False).to_dict()
        open_mask = work.get("open_closed_status", pd.Series(dtype=str)).astype(str).str.upper().eq("OPEN")

        return {
            "total_invoices": int(len(work)),
            "status_distribution": {str(k): int(v) for k, v in status_counts.items()},
            "payment_status_distribution": {str(k): int(v) for k, v in payment_counts.items()},
            "total_invoice_value": round(float(work["invoice_amount"].sum()), 2),
            "open_invoice_value": round(float(work.loc[open_mask, "invoice_amount"].sum()), 2),
            "olap_row_count": int(len(olap_df) if olap_df is not None else 0),
        }

    def _build_discovered_exception_summary(self, records_map: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for key, rows in sorted(records_map.items(), key=lambda item: len(item[1]), reverse=True):
            label = rows[0].get("exception_type", key.replace("_", " ").title()) if rows else key
            out.append(
                {
                    "exception_type": label,
                    "category_id": key,
                    "case_count": int(len(rows)),
                }
            )
        return out

    def _build_celonis_context_layer(
        self,
        process_context: Dict[str, Any],
        case_level: pd.DataFrame,
        exception_categories: List[Dict[str, Any]],
        exception_records_map: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        throughput = process_context.get("throughput_times", []) or []
        variants = process_context.get("variants", []) or []
        role_mappings = process_context.get("role_mappings", []) or []
        events_total = int(process_context.get("total_events", 0) or 0)
        cases_total = int(process_context.get("total_cases", 0) or 0)
        avg_e2e_days = float(process_context.get("avg_end_to_end_days", 0) or 0)

        top_process_map = []
        for t in throughput[:8]:
            top_process_map.append(
                {
                    "from_step": t.get("source_activity"),
                    "to_step": t.get("target_activity"),
                    "avg_transition_days": float(t.get("avg_duration_days", 0) or 0),
                    "case_count": int(t.get("case_count", 0) or 0),
                }
            )

        top_variants = []
        for v in variants[:8]:
            top_variants.append(
                {
                    "variant_path": v.get("variant", ""),
                    "frequency": int(v.get("frequency", 0) or 0),
                    "percentage": float(v.get("percentage", 0) or 0),
                }
            )

        top_resources = []
        if isinstance(role_mappings, dict):
            for idx, (activity, role) in enumerate(role_mappings.items()):
                if idx >= 8:
                    break
                top_resources.append(
                    {
                        "resource_role": role,
                        "activity": activity,
                        "frequency": 0,
                    }
                )
        else:
            for r in role_mappings[:8]:
                top_resources.append(
                    {
                        "resource_role": r.get("resource_role"),
                        "activity": r.get("activity"),
                        "frequency": int(r.get("frequency", 0) or 0),
                    }
                )

        exception_contexts = []
        for category in sorted(exception_categories, key=lambda x: int(x.get("case_count", 0) or 0), reverse=True):
            category_id = str(category.get("category_id", ""))
            category_rows = exception_records_map.get(category_id, []) or []
            sample = category_rows[0] if category_rows else {}
            inferred_best_action = self._infer_best_action_for_category(
                category_label=str(category.get("category_label", "")),
                sample_record=sample,
                avg_e2e_days=avg_e2e_days,
            )
            exception_contexts.append(
                {
                    "exception_category_id": category_id,
                    "exception_category_label": category.get("category_label"),
                    "case_count": int(category.get("case_count", 0) or 0),
                    "open_count": int(category.get("open_count", 0) or 0),
                    "closed_count": int(category.get("closed_count", 0) or 0),
                    "total_value": float(category.get("total_value", 0) or 0),
                    "derived_next_best_action": inferred_best_action,
                    "pi_context_used": (
                        "Derived from event sequence, exception recurrence, open/closed behavior, "
                        "and turnaround metrics from Celonis process context."
                    ),
                }
            )

        return {
            "context_ready": True,
            "process_map": {
                "top_transitions": top_process_map,
                "bottleneck": process_context.get("bottleneck", {}),
                "golden_path": process_context.get("golden_path", ""),
                "golden_path_percentage": float(process_context.get("golden_path_percentage", 0) or 0),
            },
            "variants": {
                "top_variants": top_variants,
                "variant_count": int(len(variants)),
            },
            "resources": {
                "role_activity_mappings": top_resources,
                "mapping_count": int(len(role_mappings)),
            },
            "events": {
                "total_events": events_total,
                "total_cases": cases_total,
                "event_coverage_note": "Event-level timestamps and activity transitions are from Celonis event logs.",
            },
            "cycle_time": {
                "avg_end_to_end_days": avg_e2e_days,
                "throughput_transitions_analyzed": int(len(throughput)),
                "exception_rate_pct": float(process_context.get("exception_rate", 0) or 0),
            },
            "exception_contexts": exception_contexts,
            "sample_case_count_in_cache": int(len(case_level)),
        }

    @staticmethod
    def _infer_best_action_for_category(
        category_label: str,
        sample_record: Dict[str, Any],
        avg_e2e_days: float,
    ) -> str:
        label = str(category_label or "").lower()
        days_until_due = float(sample_record.get("days_until_due", 0) or 0)
        estimated_processing = float(sample_record.get("estimated_processing_days", avg_e2e_days) or avg_e2e_days)

        if "mismatch" in label or "exception" in label:
            return "Route to exception validation agent and trigger source-data correction."
        if "open" in label and days_until_due <= max(estimated_processing, 1):
            return "Escalate now because due-date buffer is below historical processing lead-time."
        if "paid late" in label:
            return "Escalate with priority and enforce pre-due-date checkpoint for similar paths."
        if "short payment" in label:
            return "Re-evaluate payment terms and route to approval for terms correction."
        if "early payment" in label:
            return "Optimize payment timing to preserve working capital while honoring policy."
        return "Triage with exception agent using path, role, and cycle-time context."

    @staticmethod
    def _normalize_exception_key(label: str) -> str:
        txt = str(label or "").strip().lower()
        return (
            txt.replace("&", "and")
            .replace("/", " ")
            .replace("(", " ")
            .replace(")", " ")
            .replace("-", " ")
            .replace("__", "_")
            .replace("  ", " ")
            .replace(" ", "_")
        )

    @staticmethod
    def _to_jsonable(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: DataCacheService._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, list):
            return [DataCacheService._to_jsonable(v) for v in value]
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if pd.isna(value):
            return None
        return value


_GLOBAL_CACHE: Optional[DataCacheService] = None
_GLOBAL_CACHE_LOCK = threading.Lock()


def get_data_cache_service() -> DataCacheService:
    global _GLOBAL_CACHE
    if _GLOBAL_CACHE is not None:
        return _GLOBAL_CACHE
    with _GLOBAL_CACHE_LOCK:
        if _GLOBAL_CACHE is None:
            _GLOBAL_CACHE = DataCacheService()
    return _GLOBAL_CACHE
