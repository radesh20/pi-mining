from typing import Dict

from app.agents.base_agent import BaseAgent
from app.services.azure_openai_service import AzureOpenAIService


class ExceptionAgent(BaseAgent):
    def __init__(self, llm: AzureOpenAIService, process_context: Dict):
        super().__init__(
            agent_id="exception_agent",
            agent_name="Exception Agent",
            llm=llm,
            process_context=process_context,
            guardrails=[
                "Every resolution path must include Celonis evidence and value/turnaround impact.",
                "Auto-correction requires high confidence from LLM reasoning and known historical pattern context.",
                "Escalate ambiguous, novel, or high-financial-impact cases for human review.",
            ],
        )

    def process(self, input_data: Dict) -> Dict:
        import json

        system_prompt = """
You are the Exception Agent for P2P invoice processing.
Resolve all four exception families using AI reasoning over Celonis context:
1) Payment Terms Mismatch
2) Invoices with Exception (tax/price/quantity/missing GR)
3) Short Payment Terms (0-day)
4) Early Payment Optimization

Return strict JSON:
{
  "resolved": true,
  "exception_type": "...",
  "detected_process_step": "...",
  "exception_classification": "...",
  "resolution_strategy": "AUTO_CORRECT|ESCALATE|EXPEDITE|OPTIMIZE|HUMAN_REQUIRED",
  "corrections": ["..."],
  "resolved_by": "...",
  "escalation_reason": "...",
  "estimated_resolution_days": 0.0,
  "urgency_trigger": "...",
  "payload_field_justification_from_pi": "...",
  "celonis_evidence": "...",
  "financial_impact": {
    "value_at_risk": 0.0,
    "potential_savings": 0.0,
    "dpo_impact_days": 0.0
  },
  "next_best_actions": [
    {
      "action": "...",
      "why": "...",
      "derived_from_process_steps": ["..."],
      "expected_impact": "..."
    }
  ],
  "prompt_for_next_agents": {
    "target_agents": ["..."],
    "handoff_intent": "...",
    "execution_prompt": "...",
    "required_payload_fields": ["..."],
    "pi_rationale": "..."
  },
  "ai_reasoning": "..."
}

Reasoning requirements:
- Must evaluate invoice_terms vs po_terms vs vendor_master_terms when available.
- Must assess vendor history and recurrence signals if present.
- Must include urgency for long queue cases (e.g., high DPO exception backlog).
- Must remain AI-driven and avoid deterministic rule execution language.
"""
        user_prompt = f"""
Exception handling input:
{json.dumps(input_data, indent=2, default=str)}

Celonis process context:
{json.dumps(self.process_context, indent=2, default=str)}

Known exception portfolio context:
{json.dumps(self._known_exception_facts(), indent=2, default=str)}
"""
        result = self.llm.chat_json(system_prompt, user_prompt)
        return self._normalize_result(result)

    def _normalize_result(self, result: Dict) -> Dict:
        result = result if isinstance(result, dict) else {}
        result["resolved"] = bool(result.get("resolved", False))
        result["exception_type"] = result.get("exception_type", result.get("exception_classification", "UNKNOWN"))
        result["exception_classification"] = result.get("exception_classification", result.get("exception_type", "UNKNOWN"))
        result["resolution_strategy"] = result.get("resolution_strategy", "ESCALATE")
        result["corrections"] = result.get("corrections", [])
        result["resolved_by"] = result.get("resolved_by", "")
        result["escalation_reason"] = result.get("escalation_reason", "")
        result["estimated_resolution_days"] = result.get("estimated_resolution_days", 0.0)
        result["detected_process_step"] = result.get("detected_process_step", "Exception triage and remediation")
        result["urgency_trigger"] = result.get(
            "urgency_trigger",
            "Escalate when PI historical resolution lead time threatens due-date buffer.",
        )
        result["payload_field_justification_from_pi"] = result.get(
            "payload_field_justification_from_pi",
            "Exception payload carries turnaround and path metadata because PI evidence indicates timing-sensitive resolution.",
        )
        result["celonis_evidence"] = result.get(
            "celonis_evidence",
            "Based on Celonis exception distribution, DPO delays, and role mappings from process context.",
        )
        result["financial_impact"] = result.get(
            "financial_impact",
            {"value_at_risk": 0.0, "potential_savings": 0.0, "dpo_impact_days": 0.0},
        )
        result["next_best_actions"] = result.get("next_best_actions", [])
        if not isinstance(result["next_best_actions"], list) or not result["next_best_actions"]:
            result["next_best_actions"] = [
                {
                    "action": "Route to exception resolution specialist",
                    "why": "Primary resolution path requires context-grounded remediation.",
                    "derived_from_process_steps": [result["detected_process_step"]],
                    "expected_impact": "Reduces turnaround risk for exception path.",
                }
            ]
        result["prompt_for_next_agents"] = result.get(
            "prompt_for_next_agents",
            {
                "target_agents": ["Invoice Processing Agent", "Human-in-the-Loop Agent"],
                "handoff_intent": "Exception resolution handoff",
                "execution_prompt": (
                    "Resolve exception using PI context: process step, turnaround risk, and detected root-cause signals."
                ),
                "required_payload_fields": [
                    "exception_type",
                    "detected_process_step",
                    "financial_impact",
                    "urgency_trigger",
                ],
                "pi_rationale": (
                    "Prompt includes process-step and cycle-time context because Celonis evidence shows timing-sensitive risk."
                ),
            },
        )
        result["ai_reasoning"] = result.get(
            "ai_reasoning",
            "GPT-4o inferred resolution strategy from exception signals, vendor context, and turnaround risk.",
        )
        return result

    @staticmethod
    def _known_exception_facts() -> Dict:
        return {
            "payment_terms_mismatch": {
                "affected_invoices": 37,
                "value_usd": 22500000,
                "avg_dpo_days": 36.52,
            },
            "invoice_exception_queue": {
                "affected_invoices": 19,
                "value_usd": 2480000,
                "avg_dpo_days": 80.83,
            },
            "short_payment_terms_0_days": {
                "affected_invoices": 25,
                "value_usd": 15300000,
                "avg_dpo_days": 1.21,
            },
            "early_payment_optimization": {
                "affected_invoices": 23,
                "value_usd": 19000000,
                "potential_dpo_days": 63,
            },
        }
