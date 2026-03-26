import threading
from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from app.services.celonis_service import CelonisService, CelonisConnectionError
from app.services.process_insight_service import ProcessInsightService
from app.services.azure_openai_service import AzureOpenAIService
from app.services.agent_recommendation_service import AgentRecommendationService
from app.services.data_cache_service import get_data_cache_service
from app.config import settings
import pandas as pd

from app.services.prompt_generation_service import PromptGenerationService

router = APIRouter()


@router.get("/process/connection")
def check_connection(live: bool = Query(False, description="When true, performs a live Celonis handshake check.")):
    try:
        if live:
            svc = CelonisService()
            return {"success": True, "data": svc.get_connection_info()}

        cache = get_data_cache_service()
        status = cache.get_cache_status()
        data = {
            "mode": "cache_snapshot",
            "celonis_connected": bool(status.get("is_loaded")) and not bool(status.get("last_error")),
            "is_loaded": status.get("is_loaded", False),
            "is_stale": status.get("is_stale", True),
            "refresh_in_progress": status.get("refresh_in_progress", False),
            "last_refreshed_at": status.get("last_refreshed_at"),
            "last_error": status.get("last_error"),
            "base_url": settings.CELONIS_BASE_URL,
            "data_pool_id": settings.CELONIS_DATA_POOL_ID,
            "data_model_id": settings.CELONIS_DATA_MODEL_ID,
            "activity_table": settings.ACTIVITY_TABLE,
            "case_table": settings.CASE_TABLE,
        }
        return {"success": True, "data": jsonable_encoder(data)}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.get("/process/tables")
def list_tables():
    try:
        svc = CelonisService()
        return {"success": True, "data": svc.list_tables()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/process/pools")
def list_pools():
    try:
        svc = CelonisService()
        return {"success": True, "data": svc.list_pools_and_models()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/process/columns/{table_name}")
def list_columns(table_name: str):
    try:
        svc = CelonisService()
        return {"success": True, "data": svc.list_columns(table_name)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/process/test-event-log")
def test_event_log():
    try:
        svc = CelonisService()
        df = svc.get_event_log()
        return {
            "success": True,
            "row_count": len(df),
            "columns": list(df.columns),
            "sample": jsonable_encoder(df.head(10).where(df.notna(), None).to_dict(orient="records")),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Event log extraction failed: {str(e)}")


@router.get("/process/test-variants")
def test_variants():
    try:
        svc = CelonisService()
        df = svc.get_variants()
        return {
            "success": True,
            "row_count": len(df),
            "columns": list(df.columns),
            "sample": df.head(10).to_dict(orient="records"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Variant extraction failed: {str(e)}")


@router.get("/process/test-throughput")
def test_throughput():
    try:
        svc = CelonisService()
        df = svc.get_throughput_times()
        return {
            "success": True,
            "row_count": len(df),
            "columns": list(df.columns),
            "sample": df.head(10).to_dict(orient="records"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Throughput extraction failed: {str(e)}")


@router.get("/process/test-vendor-mapping")
def test_vendor_mapping():
    try:
        svc = CelonisService()
        df = svc.get_vendor_mapping()
        return {
            "success": True,
            "row_count": len(df),
            "columns": list(df.columns),
            "sample": jsonable_encoder(df.head(10).where(df.notna(), None).to_dict(orient="records")),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vendor mapping extraction failed: {str(e)}")


@router.get("/process/test-event-log-with-vendor")
def test_event_log_with_vendor():
    try:
        svc = CelonisService()
        df = svc.get_event_log_with_vendor()
        return {
            "success": True,
            "row_count": len(df),
            "columns": list(df.columns),
            "sample": jsonable_encoder(df.head(10).where(df.notna(), None).to_dict(orient="records")),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Event log with vendor extraction failed: {str(e)}")


@router.get("/process/test-vendor-stats")
def test_vendor_stats():
    try:
        svc = CelonisService()
        df = svc.get_vendor_statistics()
        return {
            "success": True,
            "row_count": len(df),
            "columns": list(df.columns),
            "sample": jsonable_encoder(df.head(10).where(df.notna(), None).to_dict(orient="records")),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vendor statistics extraction failed: {str(e)}")


@router.get("/process/vendor-stats")
def vendor_stats():
    try:
        cache = get_data_cache_service()
        data = cache.get_vendor_stats()
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vendor stats endpoint failed: {str(e)}")

@router.get("/process/case-table")
def get_case_table_export():
    """
    Export full purchasing document header table.
    Used by frontend for detailed PO/vendor drilldowns.
    """
    cache = get_data_cache_service()
    data = cache.get_case_table()
    return {
        "success": True,
        "row_count": len(data),
        "data": jsonable_encoder(data)
    }

@router.get("/process/vendor/{vendor_id}/paths")
def vendor_paths(vendor_id: str):
    try:
        cache = get_data_cache_service()
        data = cache.get_vendor_paths(vendor_id)
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vendor paths endpoint failed: {str(e)}")


@router.get("/process/insights")
def get_process_insights():
    try:
        cache = get_data_cache_service()
        context = cache.get_process_context()
        return {"success": True, "data": jsonable_encoder(context)}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to extract insights: {str(e)}")


@router.get("/process/context-coverage")
def get_context_coverage():
    """
    Coverage report for WCM context builder:
    tables/rows/status coverage and discovered exception categories.
    """
    try:
        cache = get_data_cache_service()
        data = cache.get_context_coverage()
        return {"success": True, "data": jsonable_encoder(data)}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build context coverage: {str(e)}")


@router.get("/process/celonis-context-layer")
def get_celonis_context_layer():
    """
    Leadership-facing context contract built from Celonis:
    process map, variants, resources, events, cycle-time, and exception contexts.
    """
    try:
        cache = get_data_cache_service()
        context = cache.get_process_context()
        layer = context.get("celonis_context_layer", {})
        return {"success": True, "data": jsonable_encoder(layer)}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch Celonis context layer: {str(e)}")

@router.get("/cache/debug-columns")
def debug_columns():
    """Temporary debug endpoint - REMOVE after fixing."""
    from app.services.data_cache_service import get_data_cache_service

    # Try to import _GLOBAL_CACHE - might fail depending on your structure
    try:
        from app.services.data_cache_service import _GLOBAL_CACHE
        global_cache_exists = _GLOBAL_CACHE is not None
        global_cache_id = id(_GLOBAL_CACHE) if _GLOBAL_CACHE else None
    except ImportError:
        global_cache_exists = "IMPORT_FAILED"
        global_cache_id = None

    result = {
        "global_cache_exists": global_cache_exists,
        "global_cache_id": global_cache_id,
    }

    cache = get_data_cache_service()
    result["cache_from_getter_id"] = id(cache)

    # Check raw attributes before ensure_loaded
    result["_is_loaded"] = getattr(cache, "_is_loaded", "MISSING")
    result["last_error"] = getattr(cache, "last_error", "MISSING")
    result["last_refreshed_at"] = getattr(cache, "last_refreshed_at", "MISSING")

    # Check if DataFrames are None
    event_log = getattr(cache, "event_log_df", None)
    case_level = getattr(cache, "case_level_df", None)
    pc = getattr(cache, "process_context", None)

    result["event_log_df_is_none"] = event_log is None
    result["case_level_df_is_none"] = case_level is None
    result["process_context_is_none"] = pc is None

    # Get details if they exist
    if event_log is not None and hasattr(event_log, "__len__"):
        result["event_log_row_count"] = len(event_log)
        if hasattr(event_log, "columns"):
            result["event_log_columns"] = list(event_log.columns)[:20]
    else:
        result["event_log_row_count"] = 0
        result["event_log_columns"] = []

    if case_level is not None and hasattr(case_level, "__len__"):
        result["case_level_row_count"] = len(case_level)
        if hasattr(case_level, "columns"):
            result["case_level_columns"] = list(case_level.columns)[:20]
    else:
        result["case_level_row_count"] = 0
        result["case_level_columns"] = []

    if pc is not None and isinstance(pc, dict):
        result["process_context_keys"] = list(pc.keys())
        result["process_context_total_cases"] = pc.get("total_cases")
        result["process_context_total_events"] = pc.get("total_events")
        result["process_context_variants_count"] = len(pc.get("variants", []))
        result["process_context_activities"] = pc.get("activities", [])[:10]
    else:
        result["process_context_keys"] = []
        result["process_context_total_cases"] = None

    # Cache meta
    cache_meta = getattr(cache, "cache_meta", None)
    result["cache_meta"] = cache_meta if cache_meta else {}

    return result

@router.get("/process/validate/wcm-context")
def validate_wcm_context():
    """
    Validation checks for WCM context quality and mapping completeness.
    """
    try:
        cache = get_data_cache_service()
        coverage = cache.get_context_coverage()
        checks = []

        case_count = int(coverage.get("coverage", {}).get("total_cases", 0) or 0)
        exception_cat_count = int(coverage.get("coverage", {}).get("exception_category_count", 0) or 0)
        open_closed_states = coverage.get("status_coverage", {}).get("open_closed_status", {}) or {}
        payment_states = coverage.get("status_coverage", {}).get("payment_status", {}) or {}
        missing_olap = coverage.get("mapping_diagnostics", {}).get("olap_missing_required_fields", []) or []
        total_invoice_value = float(coverage.get("status_coverage", {}).get("total_invoice_value", 0) or 0)
        discovered_types = coverage.get("exception_coverage", {}).get("discovered_exception_types", []) or []
        context_layer = coverage.get("celonis_context_layer", {}) or {}
        context_ready = bool(context_layer.get("context_ready", False))
        exception_contexts = context_layer.get("exception_contexts", []) or []

        checks.append(
            {
                "name": "case_volume_loaded",
                "passed": case_count > 0,
                "details": f"Loaded cases: {case_count}",
            }
        )
        checks.append(
            {
                "name": "exception_categories_available",
                "passed": exception_cat_count >= 6,
                "details": f"Exception categories: {exception_cat_count}",
            }
        )
        checks.append(
            {
                "name": "status_coverage_available",
                "passed": len(open_closed_states) > 0 and len(payment_states) > 0,
                "details": f"Open/Closed states: {len(open_closed_states)}, Payment states: {len(payment_states)}",
            }
        )
        checks.append(
            {
                "name": "olap_mapping_complete",
                "passed": len(missing_olap) == 0,
                "details": f"Missing required OLAP mapped fields: {missing_olap}",
            }
        )
        checks.append(
            {
                "name": "invoice_value_coverage_available",
                "passed": total_invoice_value > 0,
                "details": f"Total invoice value in profile summary: {round(total_invoice_value, 2)}",
            }
        )
        checks.append(
            {
                "name": "dynamic_exceptions_discovered",
                "passed": len(discovered_types) > 0,
                "details": f"Discovered exception types: {len(discovered_types)}",
            }
        )
        checks.append(
            {
                "name": "celonis_context_layer_ready",
                "passed": context_ready,
                "details": f"Context ready flag: {context_ready}",
            }
        )
        checks.append(
            {
                "name": "exception_contexts_materialized",
                "passed": len(exception_contexts) > 0,
                "details": f"Exception contexts built: {len(exception_contexts)}",
            }
        )

        recommendations = []
        if len(missing_olap) > 0:
            recommendations.append(
                "Set WCM_OLAP_COL_* env overrides for missing fields or configure WCM_OLAP_SOURCE_TABLE."
            )
        if len(open_closed_states) == 0:
            recommendations.append("Map payment status / clearing columns in OLAP to derive open/closed states.")
        if exception_cat_count < 6:
            recommendations.append("Review activity keyword patterns and process exception extraction coverage.")
        if not context_ready:
            recommendations.append("Populate Celonis context layer from process map, variants, resources, and events.")
        if len(exception_contexts) == 0:
            recommendations.append("Materialize category-level exception contexts with next-best-action evidence.")

        overall_passed = all(bool(c.get("passed")) for c in checks)
        return {
            "success": True,
            "data": {
                "overall_passed": overall_passed,
                "checks": checks,
                "recommendations": recommendations,
                "coverage_snapshot": coverage,
            },
        }
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate WCM context: {str(e)}")


@router.get("/process/agents")
def get_recommended_agents():
    try:
        cache = get_data_cache_service()
        llm = AzureOpenAIService()
        agent_service = AgentRecommendationService(llm)
        context = cache.get_process_context()
        result = agent_service.recommend_agents(context)
        return {
            "success": True,
            "data": jsonable_encoder(result),
            "process_context": jsonable_encoder(context),
        }
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to recommend agents: {str(e)}")


@router.get("/exceptions/pending")
def get_pending_exceptions():
    """
    Compatibility endpoint for UI widgets that poll pending exceptions.
    """
    try:
        cache = get_data_cache_service()
        context = cache.get_process_context()
        return {"success": True, "data": jsonable_encoder(context.get("exception_patterns", []))}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch pending exceptions: {str(e)}")


@router.get("/process-insights/alerts")
def get_process_alerts():
    """
    Compatibility endpoint for UI widgets that poll process alerts.
    """
    try:
        cache = get_data_cache_service()
        context = cache.get_process_context()

        alerts = []
        bottleneck = context.get("bottleneck", {})
        if bottleneck and bottleneck.get("activity") and bottleneck.get("activity") != "N/A":
            alerts.append({
                "type": "bottleneck",
                "severity": "medium",
                "message": f"Bottleneck detected at {bottleneck['activity']}",
                "duration_days": bottleneck.get("duration_days", 0),
            })

        for violation in context.get("conformance_violations", []):
            alerts.append({
                "type": "conformance",
                "severity": "high",
                "message": violation.get("violation_description", "Conformance issue detected"),
                "violation_rate": violation.get("violation_rate", 0),
            })

        return {"success": True, "data": jsonable_encoder(alerts)}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch process alerts: {str(e)}")


@router.get("/cache/status")
def cache_status():
    try:
        cache = get_data_cache_service()
        return {"success": True, "data": jsonable_encoder(cache.get_cache_status())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read cache status: {str(e)}")


@router.post("/cache/refresh")
def refresh_cache(background: bool = Query(True, description="Run refresh asynchronously")):
    try:
        cache = get_data_cache_service()
        if background:
            status = cache.get_cache_status()
            if not status.get("refresh_in_progress", False):
                threading.Thread(
                    target=cache.refresh_all_data,
                    daemon=True,
                    name="cache-refresh-api-bg",
                ).start()
            return {
                "success": True,
                "data": jsonable_encoder(cache.get_cache_status()),
                "message": "Cache refresh started in background",
            }
        data = cache.refresh_all_data()
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to refresh cache: {str(e)}")


@router.get("/agents/{agent_name}/deep-dive/{case_id}")
def agent_deep_dive(agent_name: str, case_id: str):
    """Get deep-dive context with PI evidence for agent-case interaction."""
    cache = get_data_cache_service()
    case_data = cache.get_invoice_case(case_id)
    process_context = cache.get_process_context()
    exceptions = cache.get_exception_records("*")  # Get all exceptions

    if not case_data:
        return {"error": "Case not found", "success": False}, 404

    llm = AzureOpenAIService()
    service = PromptGenerationService(llm)
    prompt = service.generate_prompt_for_agent(agent_name, case_data, process_context, exceptions)
    evidence = service.generate_agent_evidence_context(case_data, process_context, exceptions)

    return {
        "success": True,
        "case_id": case_id,
        "agent_name": agent_name,
        "prompt": prompt,
        "evidence": evidence
    }

@router.post("/cache/reset-lock")
def reset_cache_refresh_lock():
    """
    Emergency endpoint to clear a stuck refresh lock.
    """
    try:
        cache = get_data_cache_service()
        cache.reset_refresh_lock()
        return {"success": True, "data": jsonable_encoder(cache.get_cache_status())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reset cache refresh lock: {str(e)}")


@router.get("/process/runtime-tuning")
def runtime_tuning():
    """
    Active runtime tuning profile for Celonis extraction and cache behavior.
    """
    try:
        cache = get_data_cache_service()
        status = cache.get_cache_status()
        data = {
            "cache": {
                "ttl_seconds": settings.CACHE_TTL_SECONDS,
                "auto_refresh_seconds": settings.CACHE_AUTO_REFRESH_SECONDS,
                "auto_refresh_policy": settings.CACHE_AUTO_REFRESH_POLICY,
                "stale_while_refresh": settings.CACHE_STALE_WHILE_REFRESH,
                "refresh_wait_seconds": settings.CACHE_REFRESH_WAIT_SECONDS,
                "is_loaded": status.get("is_loaded", False),
                "is_stale": status.get("is_stale", True),
                "last_refreshed_at": status.get("last_refreshed_at"),
                "load_duration_seconds": status.get("load_duration_seconds", 0),
            },
            "celonis": {
                "export_batch_size": settings.CELONIS_EXPORT_BATCH_SIZE,
                "export_max_rows": settings.CELONIS_EXPORT_MAX_ROWS,
                "event_log_max_rows": settings.CELONIS_EVENT_LOG_MAX_ROWS,
                "discovery_cache_ttl_seconds": settings.CELONIS_DISCOVERY_CACHE_TTL_SECONDS,
                "discovery_max_tables": settings.CELONIS_DISCOVERY_MAX_TABLES,
            },
            "wcm_grouped_extract": {
                "enabled": settings.WCM_ENABLE_GROUPED_EXTRACT,
                "max_tables": settings.WCM_GROUPED_MAX_TABLES,
                "max_rows_per_table": settings.WCM_GROUPED_MAX_ROWS_PER_TABLE,
                "sample_max_rows": settings.WCM_GROUPED_SAMPLE_MAX_ROWS,
                "include_event_tables": settings.WCM_GROUPED_INCLUDE_EVENT_TABLES,
                "prefixes": settings.WCM_GROUPED_TABLE_PREFIXES,
                "allowlist": settings.WCM_GROUPED_TABLE_ALLOWLIST,
            },
        }
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read runtime tuning: {str(e)}")


@router.get("/process/table/{table_name}/extract")
def extract_table(
    table_name: str,
    max_rows: int = Query(0, ge=0, description="0 = no row cap"),
    batch_size: int = Query(0, ge=0, description="0 = use configured default"),
    include_rows: bool = Query(False),
):
    """
    Extract all rows from any Celonis data-model table using paginated PQL.
    """
    try:
        svc = CelonisService()
        payload = svc.get_table_extract_payload(
            table_name=table_name,
            include_rows=include_rows,
            batch_size=batch_size if batch_size > 0 else None,
            max_rows=max_rows if max_rows > 0 else None,
        )
        return {"success": True, "data": jsonable_encoder(payload)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table extraction failed: {str(e)}")


@router.get("/process/working-capital/extract-all")
def extract_working_capital_all(
    include_rows: bool = Query(False),
    max_rows_per_table: int = Query(0, ge=0, description="0 = no row cap"),
    batch_size: int = Query(0, ge=0, description="0 = use configured default"),
):
    """
    Extract all working-capital tables (t_o_custom_*, t_e_custom_*) from the model.
    """
    try:
        svc = CelonisService()
        data = svc.get_working_capital_extract(
            include_rows=include_rows,
            max_rows_per_table=max_rows_per_table if max_rows_per_table > 0 else None,
            batch_size=batch_size if batch_size > 0 else None,
        )
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Working capital extraction failed: {str(e)}")


@router.get("/process/extract-all-tables")
def extract_all_tables(
    include_rows: bool = Query(False),
    max_rows_per_table: int = Query(0, ge=0, description="0 = no row cap"),
    batch_size: int = Query(0, ge=0, description="0 = use configured default"),
):
    """
    Extract every table from the Celonis data model with amount/value summaries.
    """
    try:
        svc = CelonisService()
        data = svc.get_all_tables_extract(
            include_rows=include_rows,
            max_rows_per_table=max_rows_per_table if max_rows_per_table > 0 else None,
            batch_size=batch_size if batch_size > 0 else None,
        )
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"All-table extraction failed: {str(e)}")


@router.get("/process/extract-all-tables-grouped")
def extract_all_tables_grouped(
    include_rows: bool = Query(False),
    max_rows_per_table: int = Query(0, ge=0, description="0 = no row cap"),
    batch_size: int = Query(0, ge=0, description="0 = use configured default"),
):
    """
    Extract every table from the Celonis data model and return separate groups
    (object tables / event tables / other tables).
    """
    try:
        svc = CelonisService()
        data = svc.get_all_tables_grouped_extract(
            include_rows=include_rows,
            max_rows_per_table=max_rows_per_table if max_rows_per_table > 0 else None,
            batch_size=batch_size if batch_size > 0 else None,
        )
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Grouped all-table extraction failed: {str(e)}")


@router.get("/process/working-capital/extract-grouped")
def extract_working_capital_grouped(
    include_rows: bool = Query(False),
    max_rows_per_table: int = Query(0, ge=0, description="0 = no row cap"),
    batch_size: int = Query(0, ge=0, description="0 = use configured default"),
    max_tables: int = Query(0, ge=0, description="0 = use configured/default behavior"),
    include_event_tables: bool = Query(True),
    table_prefixes: str = Query("", description="Comma-separated prefixes, e.g. t_o_custom_,t_e_custom_"),
    table_allowlist: str = Query("", description="Comma-separated explicit table names"),
):
    """
    Extract all Working Capital data and return it in separate groups:
    - object_activity_streams
    - event_streams_by_object
    - object_master_tables
    """
    try:
        svc = CelonisService()
        prefix_list = [p.strip() for p in table_prefixes.split(",") if p.strip()]
        allow_list = [t.strip() for t in table_allowlist.split(",") if t.strip()]
        data = svc.get_working_capital_grouped_extract(
            include_rows=include_rows,
            max_rows_per_table=max_rows_per_table if max_rows_per_table > 0 else None,
            batch_size=batch_size if batch_size > 0 else None,
            table_prefixes=prefix_list or None,
            table_allowlist=allow_list or None,
            include_event_tables=include_event_tables,
            max_tables=max_tables if max_tables > 0 else None,
        )
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Working capital grouped extraction failed: {str(e)}")


@router.get("/process/working-capital/detailed-transaction-olap")
def detailed_transaction_olap(
    include_rows: bool = Query(False),
    max_rows: int = Query(0, ge=0, description="0 = no row cap"),
    batch_size: int = Query(0, ge=0, description="0 = use configured default"),
):
    """
    Frontend-ready Detailed Transaction OLAP dataset with stable business keys.
    """
    try:
        svc = CelonisService()
        data = svc.get_detailed_transaction_olap(
            include_rows=include_rows,
            max_rows=max_rows if max_rows > 0 else None,
            batch_size=batch_size if batch_size > 0 else None,
        )
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Detailed transaction OLAP extraction failed: {str(e)}")
