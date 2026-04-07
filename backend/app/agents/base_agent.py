from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from app.services.azure_openai_service import AzureOpenAIService


class BaseAgent(ABC):
    def __init__(
        self,
        agent_id: str,
        agent_name: str,
        llm: AzureOpenAIService,
        process_context: Dict,
        guardrails: List[str] = None,
    ):
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.llm = llm
        self.process_context = process_context
        self.guardrails = guardrails or []
        self.last_prompt_trace: Dict[str, Any] = {}

    def _context_available(self) -> bool:
        """Return True when process_context carries real Celonis data."""
        return bool(
            self.process_context
            and int(self.process_context.get("total_cases", 0) or 0) > 0
        )

    def _provenance_tag(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Stamp result with data provenance metadata."""
        result["_data_provenance"] = {
            "context_grounded": self._context_available(),
            "context_source": "celonis" if self._context_available() else "unavailable",
        }
        return result

    @abstractmethod
    def process(self, input_data: Dict) -> Dict:
        pass

    def reason_json(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        prompt_purpose: str = "",
        message_bus_input: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self.last_prompt_trace = {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "prompt_purpose": prompt_purpose or "Agent reasoning step",
            "guardrails": self.guardrails,
            "message_bus_input": self._compact_trace_payload(message_bus_input or {}),
            "system_prompt": system_prompt.strip(),
            "user_prompt": user_prompt.strip(),
        }
        result = self.llm.chat_json(system_prompt, user_prompt)
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
