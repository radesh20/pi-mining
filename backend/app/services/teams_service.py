import logging
from typing import Any, Dict, Optional

import requests
from requests import RequestException

from app.config import settings

logger = logging.getLogger(__name__)


class TeamsWebhookService:
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = (webhook_url or settings.TEAMS_WEBHOOK_URL or "").strip()

    def send_text_message(self, title: str, message: str) -> Dict[str, Any]:
        if self._is_power_automate_webhook():
            payload = {
                "title": title or "Notification",
                "message": message or "",
                "source": "process-mining-agents",
            }
        else:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": title or "Notification",
                "themeColor": "0078D7",
                "title": title or "Notification",
                "text": message or "",
            }
        return self._post_payload(payload)

    def send_human_review_card(self, review_payload: Dict[str, Any]) -> Dict[str, Any]:
        invoice_data = self._dict(review_payload.get("invoice_data"))
        ai_reco = self._dict(review_payload.get("ai_recommendation"))
        financial = self._dict(review_payload.get("financial_impact"))
        turnaround = self._dict(review_payload.get("turnaround_risk"))
        exception_summary = self._dict(review_payload.get("exception_summary"))

        invoice_id = self._pick(
            review_payload.get("invoice_id"),
            invoice_data.get("invoice_id"),
        )
        vendor_id = self._pick(
            review_payload.get("vendor_id"),
            invoice_data.get("vendor_id"),
        )
        vendor_name = self._pick(
            review_payload.get("vendor_name"),
            invoice_data.get("vendor_name"),
        )
        exception_type = self._pick(
            review_payload.get("exception_type"),
            exception_summary.get("exception_type"),
        )
        value_at_risk = self._pick(
            financial.get("value_at_risk"),
            review_payload.get("value_at_risk"),
            invoice_data.get("invoice_amount"),
        )
        dpo_or_turnaround = self._pick(
            financial.get("dpo_impact_days"),
            turnaround.get("estimated_processing_days"),
            review_payload.get("dpo"),
        )
        ai_recommendation = self._pick(
            ai_reco.get("suggested_action"),
            review_payload.get("next_best_action"),
        )
        priority = self._pick(
            review_payload.get("priority"),
            "N/A",
        )
        assigned_role = self._pick(
            review_payload.get("assigned_role"),
            review_payload.get("assigned_to_role"),
            "N/A",
        )
        celonis_evidence = self._pick(
            review_payload.get("celonis_evidence"),
            ai_reco.get("celonis_evidence"),
            turnaround.get("celonis_evidence"),
        )
        next_best_action = self._pick(
            review_payload.get("next_best_action"),
            ai_reco.get("suggested_action"),
        )

        if self._is_power_automate_webhook():
            payload = {
                "title": "P2P Human Review Required",
                "invoice_id": str(invoice_id or "N/A"),
                "vendor_id": str(vendor_id or "N/A"),
                "vendor_name": str(vendor_name or "N/A"),
                "exception_type": str(exception_type or "N/A"),
                "financial_value_or_risk": str(value_at_risk or "N/A"),
                "dpo_or_turnaround_risk": str(dpo_or_turnaround or "N/A"),
                "ai_recommendation": str(ai_recommendation or "N/A"),
                "priority": str(priority),
                "assigned_role": str(assigned_role),
                "celonis_evidence": str(celonis_evidence or "N/A"),
                "next_best_action": str(next_best_action or "N/A"),
                "review_payload": review_payload,
                "source": "process-mining-agents",
            }
        else:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": "Human Review Required",
                "themeColor": "D83B01",
                "title": "P2P Human Review Required",
                "sections": [
                    {
                        "activityTitle": "Invoice Exception Escalation",
                        "activitySubtitle": "AI + Celonis assisted review package",
                        "facts": [
                            {"name": "Invoice ID", "value": str(invoice_id or "N/A")},
                            {"name": "Vendor", "value": f"{vendor_id or 'N/A'} / {vendor_name or 'N/A'}"},
                            {"name": "Exception Type", "value": str(exception_type or "N/A")},
                            {"name": "Financial Value / Risk", "value": str(value_at_risk or "N/A")},
                            {"name": "DPO / Turnaround Risk", "value": str(dpo_or_turnaround or "N/A")},
                            {"name": "AI Recommendation", "value": str(ai_recommendation or "N/A")},
                            {"name": "Priority", "value": str(priority)},
                            {"name": "Assigned Role", "value": str(assigned_role)},
                            {"name": "Celonis Evidence", "value": str(celonis_evidence or "N/A")},
                            {"name": "Next Best Action", "value": str(next_best_action or "N/A")},
                        ],
                        "markdown": True,
                    }
                ],
            }
        return self._post_payload(payload)

    def _post_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.webhook_url:
            logger.error("TEAMS_WEBHOOK_URL is missing; cannot send Teams notification.")
            return {
                "success": False,
                "status_code": 0,
                "response_text": "Missing TEAMS_WEBHOOK_URL",
                "payload_sent": payload,
            }

        try:
            target_kind = "Power Automate webhook" if self._is_power_automate_webhook() else "Teams incoming webhook"
            logger.info("Sending Teams notification to %s.", target_kind)
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=20,
                headers={"Content-Type": "application/json"},
            )
            success = 200 <= response.status_code < 300
            if success:
                logger.info("Teams webhook message sent successfully (status=%s).", response.status_code)
            else:
                logger.error(
                    "Teams webhook returned error status=%s response=%s",
                    response.status_code,
                    response.text,
                )
            return {
                "success": success,
                "status_code": response.status_code,
                "response_text": response.text,
                "payload_sent": payload,
            }
        except RequestException as e:
            logger.exception("Network error while sending Teams webhook message.")
            return {
                "success": False,
                "status_code": 0,
                "response_text": f"Network error: {str(e)}",
                "payload_sent": payload,
            }
        except Exception as e:  # noqa: BLE001
            logger.exception("Unexpected error while sending Teams webhook message.")
            return {
                "success": False,
                "status_code": 0,
                "response_text": f"Unexpected error: {str(e)}",
                "payload_sent": payload,
            }

    @staticmethod
    def _dict(value: Any) -> Dict[str, Any]:
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _pick(*values: Any) -> Any:
        for value in values:
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == "":
                continue
            return value
        return None

    def _is_power_automate_webhook(self) -> bool:
        url = (self.webhook_url or "").lower()
        return "powerautomate" in url or "workflows/" in url
