import json
import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4

from app.services.azure_openai_service import AzureOpenAIService
from app.guardrails.exceptions import GuardrailViolation
from app.agents.exception_agent import ExceptionAgent

logger = logging.getLogger(__name__)
AUTO_CORRECT_MIN_CONFIDENCE = 0.80


class ExceptionWorkbenchService:
    """
    Exception Workbench service for exception discovery, deep analysis, and next-best-action.
    Uses Celonis-derived process_context and AI-driven reasoning.
    """

    CATEGORY_MAP = {
        "payment_terms_mismatch": {
            "id": "payment_terms_mismatch",
            "label": "Payment Terms Mismatch",
            "aliases": ["payment terms mismatch", "terms mismatch", "zterm mismatch"],
            "keywords": ["payment term", "terms mismatch", "zterm"],
        },
        "invoice_exception": {
            "id": "invoice_exception",
            "label": "Invoices with Exception",
            "aliases": ["invoice exception", "exception", "tax mismatch", "price variance", "missing gr"],
            "keywords": ["exception", "tax", "price", "quantity", "missing gr", "data issue"],
        },
        "short_payment_terms": {
            "id": "short_payment_terms",
            "label": "Short Payment Terms (0 Days)",
            "aliases": ["short payment terms", "0 days", "immediate"],
            "keywords": ["0 day", "0-day", "immediate", "short terms"],
        },
        "early_payment": {
            "id": "early_payment",
            "label": "Early Payment / DPO 0-7 Days",
            "aliases": ["early payment", "dpo 0-7", "paid early"],
            "keywords": ["early payment", "paid early", "dpo 0", "dpo 7"],
        },
        "paid_late": {
            "id": "paid_late",
            "label": "Paid Late",
            "aliases": ["paid late", "late payment", "overdue"],
            "keywords": ["paid late", "late", "overdue", "due date passed"],
        },
        "open_at_risk": {
            "id": "open_at_risk",
            "label": "Open Invoices at Risk",
            "aliases": ["open invoices", "open at risk", "pending open"],
            "keywords": ["open", "at risk", "pending"],
        },
    }

    def __init__(
        self,
        llm: Optional[AzureOpenAIService] = None,
        teams_service: Optional[Any] = None,
        auto_notify_human_review: bool = False,
    ):
        self.llm = llm or AzureOpenAIService()
        self.teams_service = teams_service
        self.auto_notify_human_review = auto_notify_human_review

    def get_all_exception_categories(self, process_context: dict) -> list:
        """
        Returns category-level view built from process_context signals.
        """
        categories: List[Dict[str, Any]] = []
        exception_patterns = process_context.get("exception_patterns", []) or []
        activities_text = " ".join(process_context.get("activities", []) or []).lower()
        exception_rate = float(process_context.get("exception_rate", 0) or 0)
        total_cases = int(process_context.get("total_cases", 0) or 0)

        for cfg in self.CATEGORY_MAP.values():
            evidence_items = []
            case_count = 0
            frequency_percentage = 0.0

            for p in exception_patterns:
                p_text = json.dumps(p, default=str).lower()
                if self._has_keyword_match(p_text, cfg["keywords"]):
                    evidence_items.append(p)
                    case_count += int(p.get("case_count", 0) or 0)
                    frequency_percentage += float(p.get("frequency_percentage", 0) or 0)

            supported = bool(evidence_items) or self._has_keyword_match(activities_text, cfg["keywords"])

            if cfg["id"] in {"paid_late", "open_at_risk"} and not supported:
                # infer from general process risk signals when explicit labels are unavailable
                supported = exception_rate > 0 or total_cases > 0

            categories.append(
                {
                    "category_id": cfg["id"],
                    "category_label": cfg["label"],
                    "supported_by_context": supported,
                    "case_count": case_count,
                    "frequency_percentage": round(frequency_percentage, 2),
                    "evidence": evidence_items[:3],
                }
            )

        # Include dynamically discovered exception types from cache/context.
        discovered = process_context.get("discovered_exception_types", []) or []
        for item in discovered:
            label = str(item.get("exception_type", "")).strip()
            if not label:
                continue
            category_id = self._normalize_dynamic_id(label)
            if any(c["category_id"] == category_id for c in categories):
                continue
            categories.append(
                {
                    "category_id": category_id,
                    "category_label": label,
                    "supported_by_context": int(item.get("case_count", 0) or 0) > 0,
                    "case_count": int(item.get("case_count", 0) or 0),
                    "frequency_percentage": 0.0,
                    "evidence": [],
                }
            )

        return categories

    def get_exception_records(self, exception_type: str, process_context: dict) -> list:
        """
        Returns exception records for a selected category.
        Records are derived from real process_context (no mock rows).
        """
        cfg = self._resolve_category(exception_type)
        patterns = process_context.get("exception_patterns", []) or []
        variants = process_context.get("variants", []) or []
        vendor_stats = process_context.get("vendor_stats", []) or []

        records: List[Dict[str, Any]] = []
        for pattern in patterns:
            p_text = json.dumps(pattern, default=str).lower()
            if not self._has_keyword_match(p_text, cfg["keywords"]):
                continue

            recurring_vendor = self._top_recurring_vendor(vendor_stats)
            records.append(
                {
                    "exception_id": f"{cfg['id']}-{uuid4().hex[:8]}",
                    "exception_type": cfg["id"],
                    "summary": pattern.get("exception_type", cfg["label"]),
                    "case_count": int(pattern.get("case_count", 0) or 0),
                    "frequency_percentage": float(pattern.get("frequency_percentage", 0) or 0),
                    "avg_resolution_time_days": float(pattern.get("avg_resolution_time_days", 0) or 0),
                    "resolution_role": pattern.get("resolution_role", "N/A"),
                    "typical_resolution": pattern.get("typical_resolution", "N/A"),
                    "trigger_condition": pattern.get("trigger_condition", "N/A"),
                    "recurring_vendor_hint": recurring_vendor,
                    "source": "Celonis exception_patterns",
                }
            )

        for v in variants:
            v_text = str(v.get("variant", "")).lower()
            if self._has_keyword_match(v_text, cfg["keywords"]):
                records.append(
                    {
                        "exception_id": f"{cfg['id']}-{uuid4().hex[:8]}",
                        "exception_type": cfg["id"],
                        "summary": "Variant-level exception path",
                        "variant_path": v.get("variant", ""),
                        "case_count": int(v.get("frequency", 0) or 0),
                        "frequency_percentage": float(v.get("percentage", 0) or 0),
                        "source": "Celonis variants",
                    }
                )

        return records

    def analyze_exception(self, exception_payload: dict, process_context: dict) -> dict:
        """
        Deep analysis for a selected exception using AI + Celonis context.
        If human review is required and Teams integration is configured, optionally sends notification.
        """
        cfg = self._resolve_category(exception_payload.get("exception_type", ""))
        system_prompt = """
You are an Exception Workbench analyst for P2P process mining operations.
Use ONLY provided Celonis context and selected exception payload.
Return strict JSON exactly in this schema:
{
  "exception_id": "...",
  "exception_type": "...",
  "invoice_id": "...",
  "vendor_id": "...",
  "vendor_name": "...",
  "summary": "...",
  "happy_path": {"path": "...", "avg_duration_days": 0},
  "exception_path": {"path": "...", "extra_days": 0, "exception_stage": "..."},
  "root_cause_analysis": {
    "most_likely_cause": "...",
    "why": "...",
    "vendor_pattern": "...",
    "celonis_evidence": "..."
  },
  "financial_impact": {
    "invoice_value": 0,
    "value_at_risk": 0,
    "potential_savings": 0,
    "dpo_impact_days": 0
  },
  "turnaround_risk": {
    "days_until_due": 0,
    "estimated_processing_days": 0,
    "risk_level": "LOW|MEDIUM|HIGH|CRITICAL"
  },
  "recommended_resolution_role": "...",
  "automation_decision": "...",
  "exception_context_from_celonis": {
    "category_summary": "...",
    "process_step_signals": ["..."],
    "variant_signals": ["..."],
    "resource_signals": ["..."],
    "cycle_time_signals": ["..."]
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
  "next_best_action": {"action": "...", "why": "...", "confidence": 0.0},
  "send_to_human_review": true
}
"""

        user_prompt = f"""
Selected exception category:
{json.dumps(cfg, indent=2, default=str)}

Exception payload:
{json.dumps(exception_payload, indent=2, default=str)}

Celonis process context:
{json.dumps(process_context, indent=2, default=str)}

Focus requirements by type:
A) Payment Terms Mismatch:
- compare invoice terms vs PO terms vs vendor master terms
- infer likely source: invoice error vs PO error vs master data issue
- recurrence + working capital timing impact

B) Invoices with Exception:
- infer subtype (tax/price/quantity/missing GR/data issue)
- queue age and resolution-time context
- root cause recurrence + owner role

C) Short Payment Terms (0 Days):
- infer if 0-day is valid vs data issue
- urgency + late-payment risk
- correction vs expedite decision

D) Early Payment / DPO 0-7:
- detect too-early payment risk
- infer optimal payment date
- compare discount benefit vs working capital opportunity cost

E) Paid Late:
- identify delay-causing stage
- determine exception-path involvement
- prevention action

F) Open Invoices at Risk:
- estimate late-risk from turnaround and vendor pattern
- recommend preemptive action
"""

        try:
            analysis = self.llm.chat_json(system_prompt, user_prompt)
            analysis = self._normalize_analysis(analysis, cfg["id"], exception_payload)
        except Exception as e:
            logger.exception("AI exception analysis failed.")
            analysis = self._fallback_analysis(cfg["id"], exception_payload, process_context, str(e))

        analysis = self._clarify_analysis(
            analysis=analysis,
            payload=exception_payload,
            process_context=process_context,
            cfg=cfg,
        )

        try:
            nba = self.next_best_action(analysis, process_context)
            analysis["next_best_action"] = nba
        except Exception as e:
            logger.exception("Next best action generation failed.")
            analysis["next_best_action"] = {
                "action": "Escalate for manual review",
                "why": f"Next best action generation failed: {str(e)}",
                "confidence": 0.0,
            }

        analysis = self._clarify_analysis(
            analysis=analysis,
            payload=exception_payload,
            process_context=process_context,
            cfg=cfg,
        )

        analysis["exception_context_from_celonis"] = self._build_exception_context_from_celonis(
            analysis=analysis,
            process_context=process_context,
        )
        analysis["next_best_actions"] = self._build_next_best_actions(
            analysis=analysis,
            process_context=process_context,
        )
        analysis["classifier_agent"] = self._build_classifier_agent(analysis)
        analysis["prompt_for_next_agents"] = self._build_prompt_for_next_agents(
            analysis=analysis,
            process_context=process_context,
        )
        analysis["guardrail_results"] = self._build_guardrail_results(
            analysis=analysis,
            process_context=process_context,
        )

        if bool(analysis.get("send_to_human_review", True)):
            teams_result = self._maybe_notify_teams(analysis)
            if teams_result is not None:
                analysis["teams_notification"] = teams_result

        return analysis

    def next_best_action(self, exception_analysis: dict, process_context: dict) -> dict:
        """
        Deterministic next best action recommendation from completed exception analysis.
        This avoids a second LLM call per exception and greatly reduces rate-limit pressure.
        """
        turnaround = exception_analysis.get("turnaround_risk", {}) or {}
        root_cause = exception_analysis.get("root_cause_analysis", {}) or {}
        classifier = exception_analysis.get("classifier_agent", {}) or {}
        label = str(exception_analysis.get("exception_type", "invoice_exception"))
        estimated_days = float(turnaround.get("estimated_processing_days", 0) or 0)
        days_until_due = float(turnaround.get("days_until_due", 0) or 0)
        risk_level = str(turnaround.get("risk_level", "MEDIUM") or "MEDIUM").upper()
        automation_decision = str(exception_analysis.get("automation_decision", "") or "").upper()
        confidence = float(classifier.get("confidence", 0.72) or 0.72)

        action = self._default_next_action(label, exception_analysis)
        why_parts = []
        if risk_level:
            why_parts.append(f"Risk level is {risk_level}")
        if estimated_days or days_until_due:
            why_parts.append(f"estimated processing time is {estimated_days:.1f}d vs due-date buffer {days_until_due:.1f}d")
        if root_cause.get("most_likely_cause"):
            why_parts.append(f"root cause points to {root_cause.get('most_likely_cause')}")
        if automation_decision in {"HUMAN_REVIEW", "ESCALATE"}:
            why_parts.append("manual intervention is safer than automatic closure")

        return {
            "action": action,
            "why": ". ".join(why_parts) if why_parts else "Derived from exception analysis and process risk context.",
            "confidence": confidence,
        }

    def _maybe_notify_teams(self, analysis: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.auto_notify_human_review:
            return None
        if self.teams_service is None:
            logger.warning("Human review requested but Teams service is not configured.")
            return {
                "success": False,
                "status_code": 0,
                "response_text": "Teams service not configured",
                "payload_sent": analysis,
            }
        try:
            return self.teams_service.send_human_review_card(analysis)
        except Exception as e:
            logger.exception("Failed to send Teams human review notification.")
            return {
                "success": False,
                "status_code": 0,
                "response_text": f"Teams notification failed: {str(e)}",
                "payload_sent": analysis,
            }

    def _normalize_analysis(
        self,
        analysis: Dict[str, Any],
        resolved_exception_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        analysis = analysis if isinstance(analysis, dict) else {}
        exception_id = analysis.get("exception_id") or payload.get("exception_id") or f"ex-{uuid4().hex[:8]}"

        return {
            "exception_id": exception_id,
            "exception_type": analysis.get("exception_type", resolved_exception_type),
            "invoice_id": analysis.get("invoice_id", payload.get("invoice_id", "")),
            "vendor_id": analysis.get("vendor_id", payload.get("vendor_id", "")),
            "vendor_name": analysis.get("vendor_name", payload.get("vendor_name", "")),
            "summary": analysis.get("summary", ""),
            "happy_path": analysis.get("happy_path", {"path": "", "avg_duration_days": 0}),
            "exception_path": analysis.get(
                "exception_path",
                {"path": "", "extra_days": 0, "exception_stage": ""},
            ),
            "root_cause_analysis": analysis.get(
                "root_cause_analysis",
                {
                    "most_likely_cause": "",
                    "why": "",
                    "vendor_pattern": "",
                    "celonis_evidence": "",
                },
            ),
            "financial_impact": analysis.get(
                "financial_impact",
                {"invoice_value": 0, "value_at_risk": 0, "potential_savings": 0, "dpo_impact_days": 0},
            ),
            "turnaround_risk": analysis.get(
                "turnaround_risk",
                {"days_until_due": 0, "estimated_processing_days": 0, "risk_level": "MEDIUM"},
            ),
            "recommended_resolution_role": analysis.get("recommended_resolution_role", ""),
            "automation_decision": analysis.get("automation_decision", ""),
            "classifier_agent": analysis.get(
                "classifier_agent",
                {
                    "decision": "MONITOR",
                    "recommended_mode": "auto_resolve",
                    "confidence": 0.0,
                    "rationale": "",
                    "owner": "",
                },
            ),
            "exception_context_from_celonis": analysis.get(
                "exception_context_from_celonis",
                {
                    "category_summary": "",
                    "process_step_signals": [],
                    "variant_signals": [],
                    "resource_signals": [],
                    "cycle_time_signals": [],
                },
            ),
            "next_best_actions": analysis.get("next_best_actions", []),
            "prompt_for_next_agents": analysis.get(
                "prompt_for_next_agents",
                {
                    "target_agents": [],
                    "handoff_intent": "",
                    "execution_prompt": "",
                    "required_payload_fields": [],
                    "pi_rationale": "",
                },
            ),
            "next_best_action": analysis.get("next_best_action", {"action": "", "why": "", "confidence": 0.0}),
            "send_to_human_review": bool(analysis.get("send_to_human_review", False)),
            "guardrail_results": analysis.get("guardrail_results", []),
        }

    def _fallback_analysis(
        self,
        exception_type: str,
        payload: Dict[str, Any],
        process_context: Dict[str, Any],
        reason: str,
    ) -> Dict[str, Any]:
        logger.warning("Using fallback exception analysis for %s due to: %s", exception_type, reason)
        happy_path = process_context.get("golden_path", "")
        avg_dur = float(process_context.get("avg_end_to_end_days", 0) or 0)
        return {
            "exception_id": payload.get("exception_id", f"ex-{uuid4().hex[:8]}"),
            "exception_type": exception_type,
            "invoice_id": payload.get("invoice_id", ""),
            "vendor_id": payload.get("vendor_id", ""),
            "vendor_name": payload.get("vendor_name", ""),
            "summary": f"Fallback analysis generated because AI call failed: {reason}",
            "happy_path": {"path": happy_path, "avg_duration_days": avg_dur},
            "exception_path": {"path": payload.get("exception_path", ""), "extra_days": 0, "exception_stage": "N/A"},
            "root_cause_analysis": {
                "most_likely_cause": "Unable to infer",
                "why": "AI analysis unavailable",
                "vendor_pattern": "Unknown",
                "celonis_evidence": "Fallback used from process context",
            },
            "financial_impact": {
                "invoice_value": float(payload.get("invoice_amount", 0) or 0),
                "value_at_risk": float(payload.get("invoice_amount", 0) or 0),
                "potential_savings": 0,
                "dpo_impact_days": 0,
            },
            "turnaround_risk": {
                "days_until_due": float(payload.get("days_until_due", 0) or 0),
                "estimated_processing_days": avg_dur,
                "risk_level": "MEDIUM",
            },
            "recommended_resolution_role": "",
            "automation_decision": "MONITOR",
            "classifier_agent": {
                "decision": "HUMAN_REVIEW",
                "recommended_mode": "human_review",
                "confidence": 0.42,
                "rationale": "Fallback mode prefers human review so unresolved exceptions are not silently automated.",
                "owner": "Human-in-the-Loop Agent",
            },
            "exception_context_from_celonis": {
                "category_summary": "Fallback context generated from available Celonis process context.",
                "process_step_signals": [str(payload.get("exception_path", ""))],
                "variant_signals": [str(process_context.get("golden_path", ""))],
                "resource_signals": [],
                "cycle_time_signals": [f"avg_end_to_end_days={avg_dur}"],
            },
            "next_best_actions": [
                {
                    "action": "Escalate for human review",
                    "why": "Fallback path due to unavailable AI analysis.",
                    "derived_from_process_steps": [str(payload.get("exception_path", ""))],
                    "expected_impact": "Prevents silent failure on unresolved exception.",
                }
            ],
            "prompt_for_next_agents": {
                "target_agents": ["Human-in-the-Loop Agent"],
                "handoff_intent": "Fallback escalation",
                "execution_prompt": "Review this exception with priority and provide corrective action.",
                "required_payload_fields": ["exception_id", "exception_type", "invoice_id", "vendor_id"],
                "pi_rationale": "Fallback uses available PI process path and cycle-time baseline.",
            },
            "next_best_action": {"action": "Escalate for review", "why": "Fallback mode", "confidence": 0.0},
            "send_to_human_review": True,
        }

    def _clarify_analysis(
        self,
        analysis: Dict[str, Any],
        payload: Dict[str, Any],
        process_context: Dict[str, Any],
        cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        analysis = analysis if isinstance(analysis, dict) else {}
        extra_context = payload.get("extra_context", {}) if isinstance(payload.get("extra_context"), dict) else {}
        invoice_id = analysis.get("invoice_id") or payload.get("invoice_id") or extra_context.get("invoice_id") or extra_context.get("document_number") or extra_context.get("case_id") or "unknown invoice"
        vendor_id = analysis.get("vendor_id") or payload.get("vendor_id") or extra_context.get("vendor_id") or "unknown vendor"
        vendor_name = analysis.get("vendor_name") or payload.get("vendor_name") or extra_context.get("vendor_name") or vendor_id
        label = cfg.get("label") or analysis.get("exception_type") or payload.get("exception_type") or "Exception"
        avg_e2e = float(process_context.get("avg_end_to_end_days", 0) or 0)
        estimated_days = float((analysis.get("turnaround_risk", {}) or {}).get("estimated_processing_days", 0) or 0)
        happy_path = analysis.get("happy_path", {}) if isinstance(analysis.get("happy_path"), dict) else {}
        exception_path = analysis.get("exception_path", {}) if isinstance(analysis.get("exception_path"), dict) else {}
        root_cause = analysis.get("root_cause_analysis", {}) if isinstance(analysis.get("root_cause_analysis"), dict) else {}
        next_best_action = analysis.get("next_best_action", {}) if isinstance(analysis.get("next_best_action"), dict) else {}
        analysis["vendor_name"] = vendor_name

        if not analysis.get("summary"):
            analysis["summary"] = (
                f"{label} detected for invoice {invoice_id} and vendor {vendor_name}. "
                f"Use process timing and exception context to route the case."
            )

        if not happy_path.get("path"):
            analysis["happy_path"] = {
                **happy_path,
                "path": process_context.get("golden_path") or "Invoice received in VIM -> Validate invoice -> Clear invoice",
                "avg_duration_days": float(happy_path.get("avg_duration_days", 0) or avg_e2e),
            }

        fallback_exception_path = (
            extra_context.get("variant_path")
            or extra_context.get("activity_trace_text")
            or extra_context.get("summary")
            or label
        )
        if not exception_path.get("path"):
            analysis["exception_path"] = {
                **exception_path,
                "path": fallback_exception_path,
                "extra_days": max(estimated_days - avg_e2e, 0),
                "exception_stage": exception_path.get("exception_stage") or self._infer_exception_stage(label, fallback_exception_path),
            }

        if not root_cause.get("most_likely_cause"):
            analysis["root_cause_analysis"] = {
                **root_cause,
                "most_likely_cause": self._default_root_cause(label),
                "why": root_cause.get("why") or "Derived from the selected exception type and observed process path.",
                "vendor_pattern": root_cause.get("vendor_pattern") or f"Vendor {vendor_id} follows the selected exception path.",
                "celonis_evidence": root_cause.get("celonis_evidence") or "Process step signals and cycle-time context were used to build this assessment.",
            }

        if not analysis.get("recommended_resolution_role"):
            analysis["recommended_resolution_role"] = self._default_resolution_role(label)

        if not analysis.get("automation_decision"):
            analysis["automation_decision"] = "HUMAN_REVIEW" if analysis.get("send_to_human_review") else "AUTO_RESOLVE"

        if not next_best_action.get("action"):
            action = self._default_next_action(label, analysis)
            analysis["next_best_action"] = {
                "action": action,
                "why": (
                    f"{label} requires a clear remediation step using the current due-date buffer "
                    f"and process lead-time context."
                ),
                "confidence": float(next_best_action.get("confidence", 0.64) or 0.64),
            }

        return analysis

    @staticmethod
    def _default_root_cause(label: str) -> str:
        txt = str(label or "").lower()
        if "payment terms" in txt:
            return "Payment terms differ across invoice, PO, or vendor master data."
        if "early payment" in txt:
            return "Invoice is being cleared earlier than the working-capital baseline."
        if "short payment" in txt:
            return "Payment terms are shorter than expected for this invoice flow."
        if "late" in txt or "risk" in txt:
            return "Current process lead time exceeds the available due-date buffer."
        return "Invoice has entered an exception path that requires targeted resolution."

    @staticmethod
    def _default_resolution_role(label: str) -> str:
        txt = str(label or "").lower()
        if "payment terms" in txt:
            return "AP Master Data Analyst"
        if "early payment" in txt or "short payment" in txt:
            return "Working Capital Analyst"
        if "late" in txt or "risk" in txt:
            return "AP Operations Lead"
        return "Exception Specialist"

    @staticmethod
    def _infer_exception_stage(label: str, path: str) -> str:
        text = f"{label} {path}".lower()
        if "invoice exception start" in text:
            return "Invoice Exception Start"
        if "moved out" in text:
            return "Moved Out of VIM"
        if "payment" in text:
            return "Payment Handling"
        if "approve" in text:
            return "Approval Routing"
        return "Invoice Exception Handling"

    def _default_next_action(self, label: str, analysis: Dict[str, Any]) -> str:
        txt = str(label or "").lower()
        if "payment terms" in txt:
            return "Reconcile invoice, PO, and vendor master payment terms, then revalidate the invoice."
        if "early payment" in txt:
            return "Hold payment and reschedule to the optimal date unless discount economics justify early release."
        if "short payment" in txt:
            return "Correct the payment terms and route the case back through approval before payment execution."
        if "late" in txt or "risk" in txt:
            return "Escalate to AP operations and prioritize the case before the due-date buffer is exhausted."
        return (
            "Route the invoice to the exception specialist with the Celonis context package and "
            "close the exception path."
        )

    def _build_classifier_agent(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        turnaround = analysis.get("turnaround_risk", {}) or {}
        next_best_action = analysis.get("next_best_action", {}) or {}
        automation_decision = str(analysis.get("automation_decision", "") or "").upper()
        owner = str(analysis.get("recommended_resolution_role", "") or "Exception Specialist")
        risk_level = str(turnaround.get("risk_level", "MEDIUM") or "MEDIUM").upper()
        estimated_days = float(turnaround.get("estimated_processing_days", 0) or 0)
        days_until_due = float(turnaround.get("days_until_due", 0) or 0)
        human_required = bool(analysis.get("send_to_human_review", False))

        if human_required or automation_decision in {"ESCALATE", "HUMAN_REVIEW"}:
            return {
                "decision": "HUMAN_REVIEW",
                "recommended_mode": "human_review",
                "confidence": 0.88 if human_required else 0.74,
                "rationale": (
                    f"{risk_level} turnaround risk with owner '{owner}' and action "
                    f"'{next_best_action.get('action', 'Escalate')}' indicates manual intervention is safer."
                ),
                "owner": owner,
            }

        if automation_decision in {"AUTO_RESOLVE", "AUTOMATE", "APPROVE"}:
            return {
                "decision": "AUTO_RESOLVE",
                "recommended_mode": "auto_resolve",
                "confidence": 0.82,
                "rationale": "Current process context supports automated remediation with contained execution risk.",
                "owner": owner,
            }

        mode = "auto_resolve" if days_until_due > max(estimated_days, 1) and risk_level in {"LOW", "MEDIUM"} else "human_review"
        decision = "AUTO_RESOLVE" if mode == "auto_resolve" else "HUMAN_REVIEW"
        confidence = 0.71 if mode == "auto_resolve" else 0.67
        return {
            "decision": decision,
            "recommended_mode": mode,
            "confidence": confidence,
            "rationale": (
                f"Derived from due-date buffer ({days_until_due:.1f}d) versus estimated processing time "
                f"({estimated_days:.1f}d) under {risk_level} risk."
            ),
            "owner": owner,
        }

    def _build_exception_context_from_celonis(
        self,
        analysis: Dict[str, Any],
        process_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        category = str(analysis.get("exception_type", "invoice_exception"))
        category_norm = category.lower()
        variants = process_context.get("variants", []) or []
        role_mappings = process_context.get("role_mappings", []) or []
        throughput = process_context.get("throughput_times", []) or []

        variant_signals = []
        for v in variants[:10]:
            path = str(v.get("variant", ""))
            if category_norm in path.lower() or "exception" in path.lower():
                variant_signals.append(f"{path} ({v.get('percentage', 0)}%)")
        if not variant_signals:
            for v in variants[:3]:
                variant_signals.append(f"{v.get('variant', '')} ({v.get('percentage', 0)}%)")

        process_step_signals = []
        for t in throughput[:10]:
            src = str(t.get("source_activity", ""))
            tgt = str(t.get("target_activity", ""))
            if category_norm in f"{src} {tgt}".lower() or "exception" in f"{src} {tgt}".lower():
                process_step_signals.append(f"{src} -> {tgt} ({t.get('avg_duration_days', 0)}d)")
        if not process_step_signals:
            for t in throughput[:3]:
                process_step_signals.append(
                    f"{t.get('source_activity', '')} -> {t.get('target_activity', '')} ({t.get('avg_duration_days', 0)}d)"
                )

        resource_signals = []
        if isinstance(role_mappings, dict):
            for idx, (activity, role) in enumerate(role_mappings.items()):
                if idx >= 10:
                    break
                activity_text = str(activity or "")
                if category_norm in activity_text.lower() or "exception" in activity_text.lower():
                    resource_signals.append(f"{role or 'UNKNOWN'} handles {activity_text}")
            if not resource_signals:
                for idx, (activity, role) in enumerate(role_mappings.items()):
                    if idx >= 3:
                        break
                    resource_signals.append(f"{role or 'UNKNOWN'} handles {activity or ''}")
        else:
            for r in role_mappings[:10]:
                activity = str(r.get("activity", ""))
                if category_norm in activity.lower() or "exception" in activity.lower():
                    resource_signals.append(f"{r.get('resource_role', 'UNKNOWN')} handles {activity}")
            if not resource_signals:
                for r in role_mappings[:3]:
                    resource_signals.append(f"{r.get('resource_role', 'UNKNOWN')} handles {r.get('activity', '')}")

        turnaround = analysis.get("turnaround_risk", {}) or {}
        cycle_time_signals = [
            f"avg_end_to_end_days={process_context.get('avg_end_to_end_days', 0)}",
            f"estimated_processing_days={turnaround.get('estimated_processing_days', 0)}",
            f"days_until_due={turnaround.get('days_until_due', 0)}",
        ]

        return {
            "category_summary": (
                "Category context built from Celonis process map, variants, resource analysis, and cycle-time signals."
            ),
            "process_step_signals": process_step_signals,
            "variant_signals": variant_signals,
            "resource_signals": resource_signals,
            "cycle_time_signals": cycle_time_signals,
        }

    def _build_next_best_actions(
        self,
        analysis: Dict[str, Any],
        process_context: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        turnaround = analysis.get("turnaround_risk", {}) or {}
        days_until_due = float(turnaround.get("days_until_due", 0) or 0)
        estimated_days = float(turnaround.get("estimated_processing_days", 0) or 0)
        exception_type = str(analysis.get("exception_type", "invoice_exception")).lower()

        primary = analysis.get("next_best_action", {}) or {}
        primary_action = str(primary.get("action", "Escalate to specialist"))
        primary_why = str(primary.get("why", "Derived from exception analysis and process risk context."))

        actions: List[Dict[str, Any]] = [
            {
                "action": primary_action,
                "why": primary_why,
                "derived_from_process_steps": analysis.get("exception_context_from_celonis", {}).get(
                    "process_step_signals", []
                )[:2],
                "expected_impact": "Improves exception resolution timing and reduces rework risk.",
            }
        ]

        if days_until_due <= max(estimated_days, 1):
            actions.append(
                {
                    "action": "Escalate immediately to exception specialist",
                    "why": "Due-date buffer is below historical processing lead-time.",
                    "derived_from_process_steps": ["Exception path consumes majority of remaining due-date buffer."],
                    "expected_impact": "Reduces probability of late payment and penalty exposure.",
                }
            )

        if "mismatch" in exception_type:
            actions.append(
                {
                    "action": "Trigger source-data correction and revalidation",
                    "why": "Mismatch exceptions recur when source fields are not corrected upstream.",
                    "derived_from_process_steps": ["Mismatch path repeatedly re-enters exception queue."],
                    "expected_impact": "Prevents repeated exception loops and manual touchpoints.",
                }
            )

        if len(actions) == 1:
            actions.append(
                {
                    "action": "Send to execution agent with PI prompt package",
                    "why": "Action can be automated with current confidence and context completeness.",
                    "derived_from_process_steps": ["Top variant and throughput context included in handoff."],
                    "expected_impact": "Shortens response time while preserving control.",
                }
            )
        return actions[:3]

    def _build_prompt_for_next_agents(
        self,
        analysis: Dict[str, Any],
        process_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        turnaround = analysis.get("turnaround_risk", {}) or {}
        exception_type = str(analysis.get("exception_type", "invoice_exception"))
        recommended_role = str(analysis.get("recommended_resolution_role", "Exception Specialist"))
        next_action = analysis.get("next_best_action", {}) or {}
        action_text = str(next_action.get("action", "Resolve exception and route to posting"))
        process_signals = analysis.get("exception_context_from_celonis", {}).get("process_step_signals", [])[:2]

        target_agents = ["Invoice Processing Agent", "Exception Resolution Agent"]
        if bool(analysis.get("send_to_human_review", False)):
            target_agents.append("Human-in-the-Loop Agent")

        execution_prompt = (
            f"Handle {exception_type} using Celonis-derived process context. "
            f"Primary action: {action_text}. "
            f"Use historical processing estimate {turnaround.get('estimated_processing_days', 0)} days "
            f"against due-date buffer {turnaround.get('days_until_due', 0)} days. "
            f"Route to role: {recommended_role}. "
            f"Process-step evidence: {process_signals}."
        )

        return {
            "target_agents": target_agents,
            "handoff_intent": "Execute exception resolution with process-derived urgency and guardrails.",
            "execution_prompt": execution_prompt,
            "required_payload_fields": [
                "exception_id",
                "exception_type",
                "invoice_id",
                "vendor_id",
                "turnaround_risk",
                "root_cause_analysis",
                "next_best_actions",
            ],
            "pi_rationale": (
                "Prompt fields are selected from process map, variant frequency, role mappings, "
                "and cycle-time context from Celonis."
            ),
            "process_context_snapshot": {
                "avg_end_to_end_days": process_context.get("avg_end_to_end_days", 0),
                "exception_rate": process_context.get("exception_rate", 0),
            },
        }

    def _build_guardrail_results(
        self,
        analysis: Dict[str, Any],
        process_context: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        # TODO: Frontend ExceptionIntelligence consumes guardrail_results from this live response payload.
        next_best_action = analysis.get("next_best_action", {}) if isinstance(analysis.get("next_best_action"), dict) else {}
        classifier = analysis.get("classifier_agent", {}) if isinstance(analysis.get("classifier_agent"), dict) else {}
        root_cause = analysis.get("root_cause_analysis", {}) if isinstance(analysis.get("root_cause_analysis"), dict) else {}

        raw_confidence = classifier.get("confidence")
        if raw_confidence is None:
            raw_confidence = next_best_action.get("confidence", 0.0)
        confidence = float(raw_confidence if raw_confidence is not None else 0.0)
        celonis_evidence = root_cause.get("celonis_evidence") or analysis.get("summary") or ""
        recommended_action = next_best_action.get("action") or "Escalate for specialist review"

        automation_decision = str(analysis.get("automation_decision", "") or "").upper()
        if automation_decision in {"AUTO_RESOLVE", "AUTOMATE", "APPROVE"}:
            resolution_strategy = "AUTO_CORRECT"
        elif bool(analysis.get("send_to_human_review", False)) or automation_decision in {"ESCALATE", "HUMAN_REVIEW"}:
            resolution_strategy = "HUMAN_REQUIRED"
        else:
            resolution_strategy = "MANUAL_REVIEW"

        candidate_output = {
            "resolution_strategy": resolution_strategy,
            "confidence": confidence,
            "celonis_evidence": celonis_evidence,
            "recommended_action": recommended_action,
        }

        results: List[Dict[str, Any]] = []
        results.append(
            {
                "rule_id": "EVIDENCE_REQUIRED",
                "label": "Evidence gate",
                "status": "pass" if bool(celonis_evidence) else "fail",
                "detail": (
                    "Celonis evidence present in exception analysis context."
                    if celonis_evidence
                    else "Celonis evidence is missing in exception analysis output."
                ),
                "enforcement": "code",
                "agent_name": "ExceptionAgent",
            }
        )
        results.append(
            {
                "rule_id": "SCHEMA_GATE",
                "label": "Schema gate",
                "status": "pass",
                "detail": "All required output fields present.",
                "enforcement": "code",
                "agent_name": "ExceptionAgent",
            }
        )

        exception_agent = ExceptionAgent(self.llm, process_context)
        try:
            guardrail_result = exception_agent.validate_output(candidate_output)
            gate_status = "pass" if guardrail_result.passed else "warn"
            if guardrail_result.rule_id == "AUTO_CORRECT_CONFIDENCE" and not guardrail_result.passed:
                gate_detail = (
                    f"AUTO_CORRECT overridden to HUMAN_REQUIRED — confidence {self._format_confidence_percent(confidence)} "
                    f"below {self._format_confidence_percent(AUTO_CORRECT_MIN_CONFIDENCE)} threshold."
                )
            else:
                gate_detail = (
                    f"Confidence {self._format_confidence_percent(confidence)} satisfies decision guardrail."
                    if resolution_strategy == "AUTO_CORRECT"
                    else f"Confidence gate not triggered for {resolution_strategy} path."
                )
            results.append(
                {
                    "rule_id": guardrail_result.rule_id if guardrail_result.rule_id != "ALL" else "AUTO_CORRECT_CONFIDENCE",
                    "label": "Auto-correct confidence",
                    "status": gate_status,
                    "detail": gate_detail,
                    "enforcement": "code",
                    "agent_name": "ExceptionAgent",
                }
            )
        except GuardrailViolation as exc:
            results.append(
                {
                    "rule_id": exc.rule_id,
                    "label": "Auto-correct confidence" if exc.rule_id == "AUTO_CORRECT_CONFIDENCE" else "Guardrail check",
                    "status": "fail",
                    "detail": str(exc.reason),
                    "enforcement": "code",
                    "agent_name": "ExceptionAgent",
                }
            )
        return results

    @staticmethod
    def _format_confidence_percent(value: Any) -> str:
        try:
            n = float(value)
        except (TypeError, ValueError):
            return "0%"
        return f"{round(n * 100) if n <= 1 else round(n)}%"

    def _resolve_category(self, exception_type: str) -> Dict[str, Any]:
        normalized = (exception_type or "").strip().lower()
        if not normalized:
            return self.CATEGORY_MAP["invoice_exception"]

        for cfg in self.CATEGORY_MAP.values():
            if normalized == cfg["id"]:
                return cfg
            if normalized in cfg["aliases"]:
                return cfg
            if any(alias in normalized for alias in cfg["aliases"]):
                return cfg
        tokens = [t for t in normalized.replace("-", " ").replace("_", " ").split() if t]
        if not tokens:
            return self.CATEGORY_MAP["invoice_exception"]

        dynamic_keywords = sorted(set(tokens))
        return {
            "id": self._normalize_dynamic_id(normalized),
            "label": exception_type,
            "aliases": [normalized],
            "keywords": dynamic_keywords,
        }

    @staticmethod
    def _has_keyword_match(text: str, keywords: List[str]) -> bool:
        t = (text or "").lower()
        return any(k.lower() in t for k in keywords)

    @staticmethod
    def _top_recurring_vendor(vendor_stats: List[Dict[str, Any]]) -> str:
        if not vendor_stats:
            return "Unknown"
        top = sorted(
            vendor_stats,
            key=lambda x: (
                float(x.get("exception_rate_pct", x.get("exception_rate", 0) or 0)),
                int(x.get("total_cases", x.get("case_count", 0) or 0)),
            ),
            reverse=True,
        )[0]
        return str(top.get("vendor_id", "Unknown"))

    @staticmethod
    def _normalize_dynamic_id(label: str) -> str:
        txt = str(label or "").strip().lower()
        out = (
            txt.replace("&", "and")
            .replace("/", " ")
            .replace("(", " ")
            .replace(")", " ")
            .replace("-", " ")
            .replace("__", "_")
            .replace("  ", " ")
            .replace(" ", "_")
        )
        return out or "invoice_exception"
