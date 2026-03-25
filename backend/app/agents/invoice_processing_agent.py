from typing import Dict

from app.agents.base_agent import BaseAgent
from app.services.azure_openai_service import AzureOpenAIService


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

    def process(self, input_data: Dict) -> Dict:
        import json

        system_prompt = """
You are the Invoice Processing Agent for AI-driven P2P automation.
You validate invoice payloads and detect exception risks using Celonis process context.

You must detect and reason about ALL four exception families:
1) payment_terms_mismatch
2) invoice_exception (tax/price/quantity/missing GR signals)
3) short_payment_terms (especially 0-day terms)
4) early_payment optimization opportunity

Turnaround awareness is mandatory:
- compare days_until_due vs estimated_processing_days
- if estimated_processing_days > days_until_due, urgency must be CRITICAL

Return strict JSON:
{
  "validation_result": "PASS|EXCEPTION",
  "detected_process_step": "...",
  "exceptions_found": [
    {
      "type": "payment_terms_mismatch|invoice_exception|short_payment_terms|early_payment",
      "description": "...",
      "severity": "LOW|MEDIUM|HIGH|CRITICAL",
      "value_at_risk": 0.0,
      "celonis_evidence": "..."
    }
  ],
  "turnaround_assessment": {
    "days_until_due": 0.0,
    "estimated_processing_days": 0.0,
    "historical_processing_days": 0.0,
    "urgency": "LOW|MEDIUM|HIGH|CRITICAL",
    "urgency_basis": "...",
    "recommendation": "...",
    "celonis_evidence": "..."
  },
  "action": "POST_INVOICE|HANDOFF_TO_EXCEPTION_AGENT|EXPEDITE|HOLD_FOR_OPTIMIZATION",
  "handoff_payload": {
    "invoice_data": {},
    "exception_candidates": [],
    "turnaround_context": {},
    "detected_process_step": "...",
    "payload_field_justification_from_pi": "..."
  },
  "celonis_evidence": "...",
  "ai_reasoning": "..."
}

Hard constraints:
- AI-driven decisions only, no deterministic rule scripts.
- Include Celonis evidence in each exception and overall output.
"""

        user_prompt = f"""
Invoice processing input:
{json.dumps(input_data, indent=2, default=str)}

Celonis process context:
{json.dumps(self.process_context, indent=2, default=str)}

Known portfolio-level signals to consider:
{json.dumps(self._known_metrics(), indent=2, default=str)}
"""
        result = self.llm.chat_json(system_prompt, user_prompt)
        return self._normalize_result(result, input_data)

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
            "Inference grounded in Celonis variants, exception rates, DPO behavior, and turnaround profile.",
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

    @staticmethod
    def _known_metrics() -> Dict:
        return {
            "avg_dpo_days": 36.52,
            "invoice_exception_avg_dpo_days": 80.83,
            "short_terms_avg_dpo_days": 1.21,
            "value_at_risk_usd": 5000000,
        }
