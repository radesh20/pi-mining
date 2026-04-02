from typing import Dict

from app.agents.base_agent import BaseAgent
from app.services.azure_openai_service import AzureOpenAIService

try:
    from backend.app.prompts.prompt_loader import load_prompt
except ModuleNotFoundError:
    from app.prompts.prompt_loader import load_prompt


class HumanInLoopAgent(BaseAgent):
    def __init__(self, llm: AzureOpenAIService, process_context: Dict):
        super().__init__(
            agent_id="human_in_loop_agent",
            agent_name="Human-in-the-Loop Agent",
            llm=llm,
            process_context=process_context,
            guardrails=[
                "Case package must be decision-ready for a human reviewer.",
                "Include Celonis evidence in every recommendation field.",
                "Explicitly surface turnaround and financial impact risk.",
            ],
        )
        self.prompt_config = load_prompt("human_in_loop_agent")

    def process(self, input_data: Dict) -> Dict:
        import json

        prompt_config = load_prompt(
            "human_in_loop_agent",
            input_data_json=json.dumps(input_data, indent=2, default=str),
            process_context_json=json.dumps(self.process_context, indent=2, default=str),
        )
        result = self.reason_json(
            prompt_config["system_prompt"],
            prompt_config["user_prompt"],
            prompt_purpose="Prepare human review package and escalation recommendation",
            message_bus_input=input_data,
        )
        normalized = self._normalize_result(result)
        handoff = {
            "assigned_role": normalized.get("assigned_role"),
            "priority": normalized.get("priority"),
        }
        return self.attach_prompt_trace(normalized, handoff=handoff)

    def _normalize_result(self, result: Dict) -> Dict:
        result = result if isinstance(result, dict) else {}
        result["case_summary"] = result.get("case_summary", "Escalated case requiring expert review.")
        result["reason_for_review"] = result.get("reason_for_review", "Complex exception pattern with uncertainty.")
        result["ai_recommendation"] = result.get(
            "ai_recommendation",
            {
                "suggested_action": "Review and decide with specialist input.",
                "confidence": 0.0,
                "reasoning": "",
                "celonis_evidence": "",
            },
        )
        result["priority"] = result.get("priority", "HIGH")
        result["assigned_role"] = result.get("assigned_role", "")
        result["turnaround_risk"] = result.get(
            "turnaround_risk",
            {
                "days_remaining": 0.0,
                "estimated_processing_days": 0.0,
                "risk_assessment": "",
                "celonis_evidence": "",
            },
        )
        result["financial_impact"] = result.get(
            "financial_impact",
            {"value_at_risk": 0.0, "potential_savings": 0.0, "working_capital_impact": ""},
        )
        result["celonis_evidence"] = result.get(
            "celonis_evidence",
            "Grounded in Celonis role mappings, exception behavior, and turnaround metrics.",
        )
        result["ai_reasoning"] = result.get(
            "ai_reasoning",
            "GPT-4o prepared the review package using process evidence and financial/turnaround context.",
        )
        return result

    def process_and_notify(self, input_data: Dict) -> Dict:
        """
        Prepare human review package and send to Microsoft Teams.
        Uses lazy import for TeamsWebhookService to avoid startup-time dependency issues.
        """
        review_preparation = self.process(input_data)
        teams_payload = self._build_teams_review_payload(input_data, review_preparation)

        try:
            from app.services.teams_service import TeamsWebhookService

            teams_service = TeamsWebhookService()
            teams_notification = teams_service.send_human_review_card(teams_payload)
        except Exception as e:  # noqa: BLE001
            teams_notification = {
                "success": False,
                "status_code": 0,
                "response_text": f"Teams notification failed: {str(e)}",
                "payload_sent": teams_payload,
            }

        final_status = "SENT_TO_TEAMS" if teams_notification.get("success") else "FAILED_TO_SEND"
        return {
            "review_preparation": review_preparation,
            "teams_notification": teams_notification,
            "final_status": final_status,
        }

    def _build_teams_review_payload(self, input_data: Dict, review_preparation: Dict) -> Dict:
        invoice_data = self._as_dict(input_data.get("invoice_data"))
        exception_output = self._as_dict(input_data.get("exception_output"))
        exception_summary = self._as_dict(input_data.get("exception_summary"))
        turnaround_risk = self._as_dict(review_preparation.get("turnaround_risk"))
        financial_impact = self._as_dict(review_preparation.get("financial_impact"))
        ai_reco = self._as_dict(review_preparation.get("ai_recommendation"))

        exception_type = (
            exception_output.get("exception_type")
            or exception_summary.get("exception_type")
            or input_data.get("exception_type")
            or "N/A"
        )
        invoice_id = invoice_data.get("invoice_id") or input_data.get("invoice_id") or "N/A"
        vendor_id = invoice_data.get("vendor_id") or input_data.get("vendor_id") or "N/A"
        vendor_name = invoice_data.get("vendor_name") or input_data.get("vendor_name") or "N/A"
        invoice_value = invoice_data.get("invoice_amount") or financial_impact.get("value_at_risk") or 0

        summary = (
            review_preparation.get("case_summary")
            or review_preparation.get("reason_for_review")
            or "Escalated case requiring human review."
        )
        root_cause = (
            exception_output.get("reasoning")
            or exception_output.get("ai_reasoning")
            or review_preparation.get("reason_for_review")
            or "Root cause requires specialist validation."
        )
        ai_recommendation = (
            ai_reco.get("suggested_action")
            or ai_reco.get("reasoning")
            or review_preparation.get("reason_for_review")
            or "Review and decide with specialist input."
        )
        next_best_action = (
            self._as_dict(input_data.get("next_best_action")).get("action")
            or ai_reco.get("suggested_action")
            or "Specialist review and disposition."
        )
        celonis_evidence = (
            review_preparation.get("celonis_evidence")
            or ai_reco.get("celonis_evidence")
            or turnaround_risk.get("celonis_evidence")
            or "See attached process context from Celonis."
        )

        return {
            "exception_type": exception_type,
            "invoice_id": invoice_id,
            "vendor_id": vendor_id,
            "vendor_name": vendor_name,
            "invoice_data": {
                "invoice_id": invoice_id,
                "vendor_id": vendor_id,
                "vendor_name": vendor_name,
                "invoice_amount": invoice_value,
                "currency": invoice_data.get("currency", ""),
            },
            "financial_impact": {
                "value_at_risk": financial_impact.get("value_at_risk", invoice_value),
                "potential_savings": financial_impact.get("potential_savings", 0),
                "dpo_impact_days": financial_impact.get("dpo_impact_days", 0),
            },
            "summary": summary,
            "root_cause": root_cause,
            "ai_recommendation": {
                "suggested_action": ai_recommendation,
                "confidence": ai_reco.get("confidence", 0.0),
                "reasoning": ai_reco.get("reasoning", ""),
                "celonis_evidence": ai_reco.get("celonis_evidence", celonis_evidence),
            },
            "priority": review_preparation.get("priority", "HIGH"),
            "assigned_role": review_preparation.get("assigned_role", ""),
            "next_best_action": next_best_action,
            "turnaround_risk": {
                "days_remaining": turnaround_risk.get("days_remaining", 0),
                "estimated_processing_days": turnaround_risk.get("estimated_processing_days", 0),
                "risk_assessment": turnaround_risk.get("risk_assessment", ""),
                "celonis_evidence": turnaround_risk.get("celonis_evidence", celonis_evidence),
            },
            "celonis_evidence": celonis_evidence,
        }

    @staticmethod
    def _as_dict(value) -> Dict:
        return value if isinstance(value, dict) else {}
