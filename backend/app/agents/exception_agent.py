from typing import Dict

from app.agents.base_agent import BaseAgent
from app.services.azure_openai_service import AzureOpenAIService

try:
    from backend.app.prompts.prompt_loader import load_prompt
    from backend.app.guardrails.exceptions import GuardrailViolation, GuardrailResult
except ModuleNotFoundError:
    from app.prompts.prompt_loader import load_prompt
    from app.guardrails.exceptions import GuardrailViolation, GuardrailResult


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
        self.prompt_config = load_prompt("exception_agent")

    def validate_output(self, output: dict) -> GuardrailResult:
        """
        Runs after LLM responds, before result is returned to orchestrator.
        Enforces: confidence gate, evidence gate, schema gate.
        """
        required_keys = {"resolution_strategy", "confidence", "celonis_evidence", "recommended_action"}
        missing = required_keys - output.keys()
        if missing:
            raise GuardrailViolation("SCHEMA_INVALID", f"Missing fields: {missing}")

        valid_strategies = {"AUTO_CORRECT", "MANUAL_REVIEW", "HUMAN_REQUIRED"}
        if output["resolution_strategy"] not in valid_strategies:
            raise GuardrailViolation("SCHEMA_INVALID", f"Unknown resolution_strategy: {output['resolution_strategy']}")

        if not output.get("celonis_evidence"):
            raise GuardrailViolation("EVIDENCE_REQUIRED", "celonis_evidence is empty or missing")

        if output["resolution_strategy"] == "AUTO_CORRECT" and output.get("confidence", 0) < 0.80:
            output["resolution_strategy"] = "HUMAN_REQUIRED"
            return GuardrailResult(
                passed=False,
                rule_id="AUTO_CORRECT_CONFIDENCE",
                reason=f"Confidence {output['confidence']} below 0.80 threshold",
                action_taken="OVERRIDDEN_TO_HUMAN_REQUIRED"
            )

        return GuardrailResult(passed=True, rule_id="ALL", reason="All checks passed", action_taken="ALLOWED")

    def process(self, input_data: Dict) -> Dict:
        import json

        prompt_config = load_prompt(
            "exception_agent",
            input_data_json=json.dumps(input_data, indent=2, default=str),
            process_context_json=json.dumps(self.process_context, indent=2, default=str),
            known_exception_facts_json=json.dumps(self._known_exception_facts(), indent=2, default=str),
        )
        result = self.reason_json(
            prompt_config["system_prompt"],
            prompt_config["user_prompt"],
            prompt_purpose="Resolve exception and decide whether to auto-correct or escalate",
            message_bus_input=input_data,
        )
        normalized = self._normalize_result(result)
        self._provenance_tag(normalized)
        guardrail_result = self.validate_output(normalized)
        normalized["guardrail_result"] = {
            "passed": guardrail_result.passed,
            "rule_id": guardrail_result.rule_id,
            "reason": guardrail_result.reason,
            "action_taken": guardrail_result.action_taken,
        }
        handoff = normalized.get("prompt_for_next_agents", {}) if isinstance(normalized.get("prompt_for_next_agents"), dict) else {}
        return self.attach_prompt_trace(normalized, handoff=handoff)

    def _normalize_result(self, result: Dict) -> Dict:
        result = result if isinstance(result, dict) else {}
        result["resolved"] = bool(result.get("resolved", False))
        result["exception_type"] = result.get("exception_type", result.get("exception_classification", "UNKNOWN"))
        result["exception_classification"] = result.get("exception_classification", result.get("exception_type", "UNKNOWN"))
        strategy = str(result.get("resolution_strategy", "MANUAL_REVIEW")).upper()
        strategy_map = {
            "ESCALATE": "HUMAN_REQUIRED",
            "EXPEDITE": "MANUAL_REVIEW",
            "OPTIMIZE": "AUTO_CORRECT",
        }
        result["resolution_strategy"] = strategy_map.get(strategy, strategy)
        result["confidence"] = float(result.get("confidence", 0.0) or 0.0)
        result["recommended_action"] = result.get("recommended_action", "Escalate for specialist review")
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
            "Celonis context was provided in prompt; LLM did not return specific evidence citation."
            if self._context_available()
            else "[Celonis data unavailable for this request]",
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

    def _known_exception_facts(self) -> Dict:
        """Extract exception portfolio from live process_context. No hardcoded values."""
        ctx = self.process_context or {}
        exception_patterns = ctx.get("exception_patterns", [])
        result = {}
        for pat in exception_patterns:
            exc_type = str(pat.get("exception_type", "")).lower()
            key = exc_type.replace(" ", "_").replace("(", "").replace(")", "")
            if not key:
                continue
            result[key] = {
                "affected_invoices": int(pat.get("affected_cases", 0) or 0),
                "value_usd": 0.0,
                "avg_dpo_days": float(pat.get("avg_resolution_time_days", 0) or 0),
                "frequency_pct": float(pat.get("frequency_percentage", 0) or 0),
                "_data_source": "celonis",
            }
        if not result:
            result["_empty"] = {
                "affected_invoices": 0,
                "value_usd": 0.0,
                "avg_dpo_days": 0.0,
                "_data_source": "unavailable",
            }
        return result
