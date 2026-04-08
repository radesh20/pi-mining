import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from app.guardrails.exceptions import GuardrailResult

from app.services.azure_openai_service import AzureOpenAIService
from app.config import settings

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    def __init__(
        self,
        agent_id: str,
        agent_name: str,
        llm: AzureOpenAIService,
        process_context: Dict,
        guardrails: List[str] = None,
        temperature: Optional[float] = None,
    ):
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.llm = llm
        self.process_context = process_context
        self.guardrails = guardrails or []
        # Per-agent temperature override; falls back to global AGENT_TEMPERATURE setting
        self.temperature: float = (
            temperature
            if temperature is not None
            else float(getattr(settings, "AGENT_TEMPERATURE", 0.2) or 0.2)
        )
        self.last_prompt_trace: Dict[str, Any] = {}

    def _context_available(self) -> bool:
        """Return True when process_context carries real Celonis data."""
        return bool(
            self.process_context
            and int(self.process_context.get("total_cases", 0) or 0) > 0
        )

    # ------------------------------------------------------------------
    # Provenance helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_provenance(
        source: str,
        *,
        context_grounded: Optional[bool] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Build a standard _data_provenance block.

        source must be one of: "celonis" | "llm" | "fallback" | "synthetic" | "unavailable"
        """
        valid = {"celonis", "llm", "fallback", "synthetic", "unavailable"}
        if source not in valid:
            source = "unavailable"
        tag: Dict[str, Any] = {"source": source}
        if context_grounded is not None:
            tag["context_grounded"] = context_grounded
        if note:
            tag["note"] = note
        return tag

    def _provenance_tag(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Stamp result with data provenance metadata.
        Extends the result dict in-place and returns it.

        Fields added:
          _data_provenance.context_grounded  – bool
          _data_provenance.context_source    – "celonis" | "unavailable"
          _data_provenance.field_sources     – dict of field → source label
        """
        has_data = self._context_available()
        result["_data_provenance"] = {
            "context_grounded": has_data,
            "context_source": "celonis" if has_data else "unavailable",
            # Per-field source map — filled in by agents that set _field_sources
            "field_sources": result.pop("_field_sources", {}),
        }
        return result

    def _tag_field(
        self,
        result: Dict[str, Any],
        field: str,
        source: str,
    ) -> None:
        """
        Mark an individual output field with its data source.
        Call this before _provenance_tag() so the map is captured.

        source: "celonis" | "llm" | "fallback" | "synthetic"
        """
        result.setdefault("_field_sources", {})[field] = source

    # ------------------------------------------------------------------
    # Guardrail hook — override in concrete agents
    # ------------------------------------------------------------------

    def validate_output(self, output: Dict) -> "GuardrailResult":
        """
        Post-LLM guardrail check.  Called by process() implementations
        after _normalize_result(), before returning to the orchestrator.

        Default: no-op pass.  Concrete agents MUST override this to enforce
        schema, evidence-presence, and confidence constraints.

        Raises GuardrailViolation for hard failures.
        Returns GuardrailResult for soft overrides.
        """
        from app.guardrails.exceptions import GuardrailResult
        return GuardrailResult(passed=True, rule_id="NOOP", reason="No guardrails defined", action_taken="ALLOWED")

    # ------------------------------------------------------------------
    # LLM reasoning
    # ------------------------------------------------------------------

    @abstractmethod
    def process(self, input_data: Dict) -> Dict:
        pass

    def reason_json(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        prompt_purpose: str = "",
        prompt_version: str = "",
        message_bus_input: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Retrieve request_id from context (non-blocking — empty string if no request context)
        try:
            from app.middleware.request_id import get_request_id
            request_id = get_request_id()
        except Exception:
            request_id = ""

        logger.info(
            "Agent reasoning | agent=%s purpose=%s version=%s request_id=%s",
            self.agent_id,
            prompt_purpose or "unspecified",
            prompt_version or "-",
            request_id or "-",
        )

        self.last_prompt_trace = {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "prompt_purpose": prompt_purpose or "Agent reasoning step",
            "prompt_version": prompt_version or "-",
            "temperature": self.temperature,
            "request_id": request_id or "-",
            "guardrails": self.guardrails,
            "message_bus_input": self._compact_trace_payload(message_bus_input or {}),
            "system_prompt": system_prompt.strip(),
            "user_prompt": user_prompt.strip(),
            # Real LLM call — NOT synthetic
            "_synthetic": False,
        }
        result = self.llm.chat_json(
            system_prompt,
            user_prompt,
            temperature=self.temperature,
            prompt_version=prompt_version,
            prompt_purpose=prompt_purpose,
        )
        self.last_prompt_trace["model_output"] = self._compact_trace_payload(
            result if isinstance(result, dict) else {"raw_output": result}
        )
        return result

    def attach_prompt_trace(
        self,
        result: Dict[str, Any],
        *,
        handoff: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        result = result if isinstance(result, dict) else {}
        result["prompt_trace"] = {
            **self.last_prompt_trace,
            "handoff": self._compact_trace_payload(handoff or {}),
        }
        return result

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _compact_trace_payload(self, value: Any, depth: int = 0) -> Any:
        if depth >= 3:
            if isinstance(value, dict):
                return {k: self._compact_trace_payload(v, depth + 1) for k, v in list(value.items())[:6]}
            if isinstance(value, list):
                return [self._compact_trace_payload(v, depth + 1) for v in value[:6]]
            return value
        if isinstance(value, dict):
            compact: Dict[str, Any] = {}
            for key, item in value.items():
                if key == "prompt_trace":
                    continue
                compact[key] = self._compact_trace_payload(item, depth + 1)
            return compact
        if isinstance(value, list):
            return [self._compact_trace_payload(item, depth + 1) for item in value[:8]]
        return value
