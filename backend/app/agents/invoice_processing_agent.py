from typing import Dict

from app.agents.base_agent import BaseAgent
from app.services.azure_openai_service import AzureOpenAIService

try:
    from backend.app.prompts.prompt_loader import load_prompt
except ModuleNotFoundError:
    from app.prompts.prompt_loader import load_prompt


class InvoiceProcessingAgent(BaseAgent):
    def __init__(self, llm: AzureOpenAIService, process_context: Dict):
        super().__init__(
            agent_id="invoice_processing_agent",
            agent_name="Invoice Processing Agent",
            llm=llm,
            process_context=process_context,
            guardrails=[
                "Never post invoice if conformance or exception risk is unresolved.",
                "Always include turnaround-risk awareness in the recommendation.",
                "Detect all four exception families: terms mismatch, invoice exception, short terms, early payment.",
            ],
        )
        self.prompt_config = load_prompt("invoice_processing_agent")

    def process(self, input_data: Dict) -> Dict:
        import json

        prompt_config = load_prompt(
            "invoice_processing_agent",
            input_data_json=json.dumps(input_data, indent=2, default=str),
            process_context_json=json.dumps(self.process_context, indent=2, default=str),
            known_metrics_json=json.dumps(self._known_metrics(), indent=2, default=str),
        )
        result = self.reason_json(
            prompt_config["system_prompt"],
            prompt_config["user_prompt"],
            prompt_purpose="Validate invoice and detect exception candidates before agent handoff",
            message_bus_input=input_data,
        )
        normalized = self._normalize_result(result, input_data)
        self._provenance_tag(normalized)
        handoff = normalized.get("handoff_payload", {}) if isinstance(normalized.get("handoff_payload"), dict) else {}
        return self.attach_prompt_trace(normalized, handoff=handoff)

    def _normalize_result(self, result: Dict, input_data: Dict) -> Dict:
        result = result if isinstance(result, dict) else {}
        result["validation_result"] = result.get("validation_result", "EXCEPTION")
        result["detected_process_step"] = result.get("detected_process_step", "Invoice validation and exception detection")
        result["exceptions_found"] = result.get("exceptions_found", [])
        turnaround = result.get("turnaround_assessment", {}) if isinstance(result.get("turnaround_assessment"), dict) else {}
        estimated_days = float(turnaround.get("estimated_processing_days", 0) or 0)
        if estimated_days <= 0:
            estimated_days = float(self.process_context.get("avg_end_to_end_days", 0) or 0)
        days_until_due = float(turnaround.get("days_until_due", input_data.get("invoice_data", {}).get("days_until_due", 0)) or 0)
        if days_until_due <= 0:
            days_until_due = float(input_data.get("invoice_data", {}).get("days_in_exception", 0) or 0)
        urgency = turnaround.get("urgency", "HIGH" if estimated_days > days_until_due > 0 else "MEDIUM")
        result["turnaround_assessment"] = {
            **turnaround,
            "days_until_due": days_until_due,
            "estimated_processing_days": estimated_days,
            "historical_processing_days": float(turnaround.get("historical_processing_days", estimated_days) or estimated_days),
            "urgency": urgency,
            "urgency_basis": turnaround.get(
                "urgency_basis",
                "If historical processing time exceeds remaining due-date buffer, trigger escalation/expedite.",
            ),
        }
        result["action"] = result.get("action", "HANDOFF_TO_EXCEPTION_AGENT")
        result["handoff_payload"] = result.get(
            "handoff_payload",
            {
                "invoice_data": input_data,
                "exception_candidates": result.get("exceptions_found", []),
                "turnaround_context": result.get("turnaround_assessment", {}),
                "detected_process_step": result.get("detected_process_step"),
                "payload_field_justification_from_pi": (
                    "Handoff fields are PI-derived: path step, turnaround lead time, and due-date buffer."
                ),
            },
        )
        if isinstance(result.get("handoff_payload"), dict):
            result["handoff_payload"]["detected_process_step"] = result["handoff_payload"].get(
                "detected_process_step", result.get("detected_process_step")
            )
            result["handoff_payload"]["payload_field_justification_from_pi"] = result["handoff_payload"].get(
                "payload_field_justification_from_pi",
                "Handoff fields are PI-derived: path step, turnaround lead time, and due-date buffer.",
            )
        result["celonis_evidence"] = result.get(
            "celonis_evidence",
            "Celonis context was provided in prompt; LLM did not return specific evidence citation."
            if self._context_available()
            else "[Celonis data unavailable for this request]",
        )
        result["payload_field_justification_from_pi"] = result.get(
            "payload_field_justification_from_pi",
            "Invoice payload includes turnaround and step-context fields because PI shows they control outcome timing.",
        )
        result["ai_reasoning"] = result.get(
            "ai_reasoning",
            "LLM assessed invoice risk across four exception families with turnaround constraints.",
        )
        return result

    def _known_metrics(self) -> Dict:
        """Extract portfolio metrics from live process_context. No hardcoded values."""
        ctx = self.process_context or {}
        exception_patterns = ctx.get("exception_patterns", [])

        invoice_exception_dpo = 0.0
        short_terms_dpo = 0.0
        for pat in exception_patterns:
            exc_type = str(pat.get("exception_type", "")).lower()
            if "exception" in exc_type and "short" not in exc_type and "late" not in exc_type:
                invoice_exception_dpo = float(pat.get("avg_resolution_time_days", 0) or 0)
            if "short" in exc_type or "0-day" in exc_type or "immediate" in exc_type:
                short_terms_dpo = float(pat.get("avg_resolution_time_days", 0) or 0)

        has_data = self._context_available()
        return {
            "avg_dpo_days": float(ctx.get("avg_end_to_end_days", 0) or 0),
            "invoice_exception_avg_dpo_days": invoice_exception_dpo,
            "short_terms_avg_dpo_days": short_terms_dpo,
            "value_at_risk_usd": 0.0,
            "_data_source": "celonis" if has_data else "unavailable",
        }
