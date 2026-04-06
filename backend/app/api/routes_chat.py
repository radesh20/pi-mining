"""
app/api/routes_chat.py  —  FIXED
"""

import logging
from functools import lru_cache
from fastapi import APIRouter, HTTPException

from app.models.requests import ChatRequest, SQLChatRequest
from app.models.responses import ChatResponse, APIResponse
from app.services.azure_openai_service import AzureOpenAIService
from app.services.celonis_service import CelonisService
from app.services.process_insight_service import ProcessInsightService
from app.services.agent_recommendation_service import AgentRecommendationService
from app.services.suggestion_service import SuggestionService
from app.services.chat_service import ChatService
from app.services.sql_chat_service import SQLChatService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chat", tags=["chat"])

# ── Singleton services — built once, reused across requests ──────────────────
# Building LLM + Celonis connections on every request is expensive.
# Using module-level singletons avoids that overhead.

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


# ── Chat endpoint ─────────────────────────────────────────────────────────────

@router.post("/", response_model=ChatResponse)
def chat(req: ChatRequest):
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
    return APIResponse(success=True, data={"status": "chat service is up"})


# ── Debug — REMOVE IN PRODUCTION ─────────────────────────────────────────────
# Kept behind /debug prefix and only active when DEBUG env var is set.

@router.get("/debug/columns")
def debug_columns():
    """Inspect event source columns. Disable in production via env var."""
    import os
    if os.getenv("DEBUG_ENDPOINTS", "false").lower() != "true":
        raise HTTPException(status_code=403, detail="Debug endpoints disabled")
    try:
        celonis   = CelonisService()
        event_log = celonis.get_event_log()
        source    = celonis._discover_event_source()
        # Also show vendor ID column presence
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