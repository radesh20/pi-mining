"""
app/api/routes_chat.py
"""

import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from app.models.requests import ChatRequest, SQLChatRequest
from app.models.responses import ChatResponse, APIResponse
from app.services.azure_openai_service import AzureOpenAIService
from app.services.celonis_service import CelonisService
from app.services.process_insight_service import ProcessInsightService
from app.services.agent_recommendation_service import AgentRecommendationService
from app.services.suggestion_service import SuggestionService
from app.services.chat_service import ChatService
from app.services.sql_chat_service import SQLChatService
from app.services.data_cache_service import get_data_cache_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chat", tags=["chat"])

# ── Singleton services ────────────────────────────────────────────────────────

_chat_service: ChatService | None = None
_sql_chat_service: SQLChatService | None = None


def _get_chat_service() -> ChatService:
    global _chat_service
    if _chat_service is None:
        llm             = AzureOpenAIService()
        celonis         = CelonisService()
        process_insight = ProcessInsightService(celonis_service=celonis)
        agent_rec       = AgentRecommendationService(llm=llm)
        suggestion      = SuggestionService(llm=llm)
        _chat_service   = ChatService(
            llm=llm,
            celonis=celonis,
            process_insight=process_insight,
            agent_recommendation=agent_rec,
            suggestion_service=suggestion,
        )
        logger.info("ChatService singleton created")
    return _chat_service


def _get_sql_chat_service() -> SQLChatService:
    global _sql_chat_service
    if _sql_chat_service is None:
        _sql_chat_service = SQLChatService(llm=AzureOpenAIService())
        logger.info("SQLChatService singleton created")
    return _sql_chat_service


# ── Warming reply helper ──────────────────────────────────────────────────────

def _warming_response(status: dict) -> ChatResponse:
    """
    Build a user-friendly ChatResponse while the cache is still loading.
    Distinguishes between 'warming' (normal first load) and an actual error.
    """
    last_error = status.get("last_error")
    refresh_in_progress = status.get("refresh_in_progress", False)
    is_warming = status.get("is_warming", False)

    if last_error:
        # A real error occurred — surface it clearly so the user/admin knows
        reply = (
            f"There was a problem loading Celonis data: {last_error}. "
            "Please trigger /api/cache/refresh or contact your administrator."
        )
    elif is_warming or refresh_in_progress:
        reply = (
            "I'm still connecting to your Celonis data — this usually takes "
            "30–90 seconds on first load. Please try again in a moment."
        )
    else:
        # Cache is not loaded and nothing is running — something went wrong silently
        reply = (
            "The Celonis data cache is not yet available. "
            "A background refresh may have failed to start. "
            "Please trigger /api/cache/refresh or restart the service."
        )

    return ChatResponse(
        success=False,
        reply=reply,
        suggested_questions=[
            "Show me all active exceptions",
            "Which vendors have the highest exception rate?",
            "Show me the process flow",
        ],
        data_sources=[],
        next_steps=[],
        context_used={
            "cache_status": "warming" if not last_error else "error",
            "refresh_in_progress": refresh_in_progress,
            "is_warming": is_warming,
            "last_error": last_error,
        },
        scope_label="system",
        agent_used="",
        error="cache_warming" if not last_error else "cache_error",
    )


# ── Chat endpoint ─────────────────────────────────────────────────────────────

@router.post("/", response_model=ChatResponse)
def chat(req: ChatRequest):
    """
    Main chat endpoint.

    Guard rail: if the Celonis cache has not finished its initial load,
    return immediately with a friendly warming message instead of hanging
    for minutes and then timing out.
    """
    cache = get_data_cache_service()

    # Fast non-locking check first — avoids lock contention on every request
    if not cache._is_loaded:
        cache.ensure_loaded()  # Trigger background load if not already warming
        status = cache.get_cache_status()
        # Double-check under the status snapshot (get_cache_status holds lock)
        if not status.get("is_loaded"):
            logger.info(
                "Chat request received but cache not yet loaded "
                "(warming=%s, error=%s). Returning warming response.",
                status.get("is_warming"),
                status.get("last_error"),
            )
            return _warming_response(status)

    try:
        service = _get_chat_service()
        history = [
            {"role": msg.role, "content": msg.content}
            for msg in req.conversation_history
        ]
        result = service.chat(
            message=req.message,
            conversation_history=history,
            case_id=req.case_id,
            vendor_id=req.vendor_id,
        )
        return ChatResponse(
            success=result["success"],
            reply=result["reply"],
            suggested_questions=result.get("suggested_questions", []),
            data_sources=result.get("data_sources", []),
            next_steps=result.get("next_steps", []),
            context_used=result.get("context_used", {}),
            scope_label=result.get("scope_label", ""),
            agent_used=result.get("agent_used", ""),
            error=result.get("error"),
            pi_evidence=result.get("pi_evidence"),
            similar_cases=result.get("similar_cases"),
            vendor_context=result.get("vendor_context"),
            graph_path=result.get("graph_path"),
            path_label=result.get("path_label"),
        )
    except Exception as exc:
        logger.error("Chat endpoint error: %s", str(exc), exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# ── SQL Chat endpoint ─────────────────────────────────────────────────────────

@router.post("/sql", response_model=ChatResponse)
def sql_chat(req: SQLChatRequest):
    try:
        service = _get_sql_chat_service()
        history = [
            {"role": msg.role, "content": msg.content}
            for msg in req.conversation_history
        ]
        result = service.chat(
            message=req.message,
            table_name=req.table_name,
            dialect=req.dialect,
            case_id=req.case_id,
            vendor_id=req.vendor_id,
            conversation_history=history,
        )
        return ChatResponse(
            success=result["success"],
            reply=result["reply"],
            suggested_questions=result.get("suggested_questions", []),
            data_sources=result.get("data_sources", []),
            next_steps=result.get("next_steps", []),
            context_used=result.get("context_used", {}),
            scope_label=result.get("scope_label", ""),
            agent_used=result.get("agent_used", ""),
            error=result.get("error"),
            graph_path=None,
            path_label=None,
        )
    except Exception as exc:
        logger.error("SQL chat endpoint error: %s", str(exc), exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Health ────────────────────────────────────────────────────────────────────

@router.get("/health")
def chat_health():
    cache = get_data_cache_service()
    status = cache.get_cache_status()
    return APIResponse(
        success=True,
        data={
            "status": "chat service is up",
            "cache_loaded": status.get("is_loaded"),
            "cache_warming": status.get("is_warming"),
            "cache_last_error": status.get("last_error"),
            "cache_refresh_in_progress": status.get("refresh_in_progress"),
            "cache_last_refreshed_at": status.get("last_refreshed_at"),
            "cache_total_cases": status.get("total_cases"),
            "cache_total_events": status.get("total_events"),
        },
    )


# ── Debug endpoints (require DEBUG_ENDPOINTS=true env var) ───────────────────

@router.get("/debug/columns")
def debug_columns():
    import os
    if os.getenv("DEBUG_ENDPOINTS", "false").lower() != "true":
        raise HTTPException(status_code=403, detail="Debug endpoints disabled")
    try:
        celonis   = CelonisService()
        event_log = celonis.get_event_log()
        source    = celonis._discover_event_source()
        has_vendor = "vendor_id" in event_log.columns
        enriched_cols: list = []
        try:
            enriched = celonis.get_event_log_with_vendor()
            enriched_cols = list(enriched.columns)
        except Exception:
            pass
        return APIResponse(success=True, data={
            "event_source_table":       source["table"],
            "column_mapping":           source["mapping"],
            "sample_case_ids":          event_log["case_id"].head(15).tolist(),
            "case_id_dtype":            str(event_log["case_id"].dtype),
            "all_columns_in_raw_log":   list(event_log.columns),
            "has_vendor_id_col":        has_vendor,
            "enriched_log_columns":     enriched_cols,
            "total_rows":               len(event_log),
        })
    except Exception as exc:
        logger.error("debug-columns failed: %s", str(exc), exc_info=True)
        return APIResponse(success=False, error=str(exc))


@router.get("/debug/ocpm_validation")
def debug_ocpm_validation():
    import os
    if os.getenv("DEBUG_ENDPOINTS", "false").lower() != "true":
        raise HTTPException(status_code=403, detail="Debug endpoints disabled")
    try:
        celonis = CelonisService()
        df = celonis.get_event_log()
        tables_used = df["source_table"].unique().tolist() if "source_table" in df.columns else []
        row_counts = df["source_table"].value_counts().to_dict() if "source_table" in df.columns else {}
        return APIResponse(success=True, data={
            "status": "OCPM Multi-Table Logic Active",
            "total_merged_rows": len(df),
            "tables_used": tables_used,
            "per_table_row_counts": row_counts,
            "unique_cases": df["case_id"].nunique() if not df.empty else 0,
            "unique_activities": df["activity"].nunique() if not df.empty else 0,
            "timestamp_min": str(df["timestamp"].min()) if not df.empty else None,
            "timestamp_max": str(df["timestamp"].max()) if not df.empty else None,
            "sample_activities": df["activity"].value_counts().head(20).to_dict() if not df.empty else {},
        })
    except Exception as exc:
        logger.error("debug-ocpm-validation failed: %s", str(exc), exc_info=True)
        return APIResponse(success=False, error=str(exc))


@router.get("/debug/cache_status")
def debug_cache_status():
    """
    Returns the full current cache status — useful for monitoring warm-up
    progress without needing DEBUG_ENDPOINTS=true.
    """
    cache = get_data_cache_service()
    return APIResponse(success=True, data=cache.get_cache_status())


@router.post("/debug/reset_cache")
def debug_reset_cache():
    import os
    if os.getenv("DEBUG_ENDPOINTS", "false").lower() != "true":
        raise HTTPException(status_code=403, detail="Debug endpoints disabled")
    try:
        CelonisService.reset_shared_connection()
        celonis = CelonisService()
        celonis.clear_instance_caches()
        data_cache = get_data_cache_service()
        data_cache.force_reset()
        return APIResponse(success=True, data={"message": "All caches cleared successfully"})
    except Exception as exc:
        logger.error("debug-reset-cache failed: %s", str(exc), exc_info=True)
        return APIResponse(success=False, error=str(exc))


@router.post("/debug/force_refresh")
def debug_force_refresh():
    """
    Force a full cache reset + synchronous refresh.
    Use this to unblock after a 'hard timeout' error.
    """
    import os
    if os.getenv("DEBUG_ENDPOINTS", "false").lower() != "true":
        raise HTTPException(status_code=403, detail="Debug endpoints disabled")
    try:
        data_cache = get_data_cache_service()
        result = data_cache.force_reset()
        return APIResponse(success=True, data=result)
    except Exception as exc:
        logger.error("debug-force-refresh failed: %s", str(exc), exc_info=True)
        return APIResponse(success=False, error=str(exc))


@router.post("/debug/reset_lock")
def debug_reset_lock():
    """
    Manually clear a stuck refresh lock without wiping the cache.
    Safe to call if you see 'Refresh lock cleared after exceeding hard timeout'.
    """
    import os
    if os.getenv("DEBUG_ENDPOINTS", "false").lower() != "true":
        raise HTTPException(status_code=403, detail="Debug endpoints disabled")
    try:
        data_cache = get_data_cache_service()
        data_cache.reset_refresh_lock()
        return APIResponse(
            success=True,
            data={
                "message": "Refresh lock reset. Trigger /api/cache/refresh to reload.",
                "cache_status": data_cache.get_cache_status(),
            },
        )
    except Exception as exc:
        logger.error("debug-reset-lock failed: %s", str(exc), exc_info=True)
        return APIResponse(success=False, error=str(exc))