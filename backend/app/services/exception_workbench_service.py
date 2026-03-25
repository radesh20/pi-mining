import json
import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4

from app.services.azure_openai_service import AzureOpenAIService

logger = logging.getLogger(__name__)


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

        analysis["exception_context_from_celonis"] = self._build_exception_context_from_celonis(
            analysis=analysis,
            process_context=process_context,
        )
        analysis["next_best_actions"] = self._build_next_best_actions(
            analysis=analysis,
            process_context=process_context,
        )
        analysis["prompt_for_next_agents"] = self._build_prompt_for_next_agents(
            analysis=analysis,
            process_context=process_context,
        )

        if bool(analysis.get("send_to_human_review", False)):
            teams_result = self._maybe_notify_teams(analysis)
            if teams_result is not None:
                analysis["teams_notification"] = teams_result

        return analysis

    def next_best_action(self, exception_analysis: dict, process_context: dict) -> dict:
        """
        AI-driven next best action recommendation from completed exception analysis.
        """
        system_prompt = """
You are a decisioning assistant for P2P exception handling.
Given an exception analysis and Celonis context, produce the next best action.
Return strict JSON:
{
  "action": "...",
  "why": "...",
  "confidence": 0.0
}
"""
        user_prompt = f"""
Exception analysis:
{json.dumps(exception_analysis, indent=2, default=str)}

Celonis process context:
{json.dumps(process_context, indent=2, default=str)}
"""
        try:
            result = self.llm.chat_json(system_prompt, user_prompt)
            return {
                "action": result.get("action", "Escalate to specialist"),
                "why": result.get("why", "Derived from exception analysis and process risk context."),
                "confidence": float(result.get("confidence", 0.0) or 0.0),
            }
        except Exception as e:
            logger.exception("AI next best action failed.")
            return {
                "action": "Escalate to specialist",
                "why": f"Fallback due to AI error: {str(e)}",
                "confidence": 0.0,
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
