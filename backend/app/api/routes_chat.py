"""
app/api/routes_chat.py
"""

import logging
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


def _get_chat_service() -> ChatService:
    llm             = AzureOpenAIService()
    celonis         = CelonisService()
    process_insight = ProcessInsightService(celonis_service=celonis)
    agent_rec       = AgentRecommendationService(llm=llm)
    suggestion      = SuggestionService(llm=llm)
    return ChatService(
        llm=llm,
        celonis=celonis,
        process_insight=process_insight,
        agent_recommendation=agent_rec,
        suggestion_service=suggestion,
    )


def _get_sql_chat_service() -> SQLChatService:
    llm = AzureOpenAIService()
    return SQLChatService(llm=llm)


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
        )
    except Exception as exc:
        logger.error("Chat endpoint error: %s", str(exc), exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


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
        )
    except Exception as exc:
        logger.error("SQL chat endpoint error: %s", str(exc), exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/health")
def chat_health():
    return APIResponse(success=True, data={"status": "chat service is up"})


# ── Debug endpoint — remove after diagnosis ──────────────────────────────────
@router.get("/debug-columns")
def debug_columns():
    """Call once to see what columns and case ID values are in your event source table."""
    try:
        celonis = CelonisService()
        event_log = celonis.get_event_log()
        source = celonis._discover_event_source()
        return APIResponse(success=True, data={
            "event_source_table":    source["table"],
            "column_mapping":        source["mapping"],
            "sample_case_ids":       event_log["case_id"].head(15).tolist(),
            "case_id_dtype":         str(event_log["case_id"].dtype),
            "all_columns_in_table":  celonis.list_columns(source["table"]),
        })
    except Exception as exc:
        logger.error("debug-columns failed: %s", str(exc), exc_info=True)
        return APIResponse(success=False, error=str(exc))
