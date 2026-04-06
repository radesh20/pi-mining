from pydantic import BaseModel
from typing import Any, Dict, List, Optional


class APIResponse(BaseModel):
    success: bool = True
    data: Any = None
    error: Optional[str] = None


class ChatResponse(BaseModel):
    success: bool = True
    reply: str
    suggested_questions: List[str] = []
    data_sources: List[str] = []
    next_steps: List[str] = []
    context_used: Dict[str, Any] = {}
    scope_label: str = ""
    agent_used: str = ""
    error: Optional[str] = None

    # PI-enriched response fields
    pi_evidence: Optional[Dict[str, Any]] = None
    similar_cases: Optional[List[Dict[str, Any]]] = None
    vendor_context: Optional[Dict[str, Any]] = None

    # Graph fields — BOTH must be non-None for the frontend to render a graph.
    # graph_path is None for every question that did not explicitly ask for
    # a visual (graph / flow / diagram / draw etc.).
    # The backend (ChatService.chat) is the single authority on whether
    # graph_path is set — the frontend must not second-guess it.
    graph_path: Optional[str] = None
    path_label: Optional[str] = None