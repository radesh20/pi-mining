"""
app/services/chat_service.py
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from app.services.azure_openai_service import AzureOpenAIService
from app.services.celonis_service import CelonisService
from app.services.process_insight_service import ProcessInsightService
from app.services.agent_recommendation_service import AgentRecommendationService

logger = logging.getLogger(__name__)

SYSTEM_PROMPT_TEMPLATE = """You are an AI Process Intelligence Assistant embedded
in a Celonis-powered AP invoice management system.

Your role is to help operations teams understand, diagnose, and resolve AP invoice
process issues using the REAL process mining data injected below.

STRICT RULES:
1. Answer ONLY using the PI data provided — never guess or invent numbers.
2. If data for a specific question is missing, say so clearly.
3. Use exact activity names, case IDs, day counts, and agent names from the context.
4. For diagnosis → state current stage, days in stage, relevant agent flags, variant.
5. For prediction → use historical cycle-time patterns from the data.
6. For recommendations → lead with the next best action derived from agent evidence.
7. Keep answers concise and operational. Operations teams are time-poor.
8. When asked about conformance violations, reference them directly from the data.
9. When asked about exception types, list ALL of them with their exact frequencies.
10. When asked about vendors, use the vendor stats provided in the context.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LIVE PROCESS INTELLIGENCE SNAPSHOT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{pi_context}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


class ChatService:

    def __init__(
        self,
        llm: AzureOpenAIService,
        celonis: CelonisService,
        process_insight: ProcessInsightService,
        agent_recommendation: AgentRecommendationService,
    ):
        self.llm = llm
        self.celonis = celonis
        self.process_insight = process_insight
        self.agent_recommendation = agent_recommendation

    def chat(
        self,
        message: str,
        conversation_history: List[Dict[str, str]],
        case_id: Optional[str] = None,
        vendor_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        pi_context, context_used = self._build_context(case_id=case_id, vendor_id=vendor_id)
        system_prompt = SYSTEM_PROMPT_TEMPLATE.format(pi_context=pi_context)
        user_prompt   = self._build_user_prompt(message, conversation_history)

        try:
            reply = self.llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.2,
                max_tokens=2500,   # FIX 2 — raised from 1500 to avoid truncation
            )
            return {"success": True, "reply": reply, "context_used": context_used, "error": None}
        except Exception as exc:
            logger.error("ChatService LLM call failed: %s", str(exc))
            return {
                "success": False,
                "reply": "I'm unable to answer right now — the AI service returned an error.",
                "context_used": context_used,
                "error": str(exc),
            }

    def _build_context(
        self,
        case_id: Optional[str],
        vendor_id: Optional[str],
    ) -> Tuple[str, Dict[str, Any]]:
        context_used: Dict[str, Any] = {}
        sections: List[str] = []
        process_ctx = None

        # ── 1. Global process snapshot ───────────────────────────────────────
        try:
            process_ctx        = self.process_insight.build_process_context()
            bottleneck         = process_ctx.get("bottleneck", {})
            variants           = process_ctx.get("variants", [])
            exception_patterns = process_ctx.get("exception_patterns", [])
            conformance        = process_ctx.get("conformance_violations", [])
            decision_rules     = process_ctx.get("decision_rules", [])

            global_section = (
                f"GLOBAL PROCESS STATS\n"
                f"  Total cases            : {process_ctx.get('total_cases', 'N/A')}\n"
                f"  Total events           : {process_ctx.get('total_events', 'N/A')}\n"
                f"  Avg end-to-end (days)  : {process_ctx.get('avg_end_to_end_days', 'N/A')}\n"
                f"  Exception rate         : {process_ctx.get('exception_rate', 'N/A')}%\n"
                f"  Main bottleneck        : {bottleneck.get('activity', 'N/A')} "
                f"({bottleneck.get('duration_days', 'N/A')} days avg, "
                f"{bottleneck.get('case_count', 'N/A')} cases)\n"
                f"  Golden path            : {process_ctx.get('golden_path', 'N/A')} "
                f"({process_ctx.get('golden_path_percentage', 0)}% of cases)\n"
            )

            if variants:
                global_section += "  Process variants (top 5):\n"
                for v in variants[:5]:
                    global_section += f"    • {v['variant']} ({v['frequency']} cases, {v['percentage']}%)\n"

            # All exception patterns with full detail
            if exception_patterns:
                global_section += f"  Exception patterns ({len(exception_patterns)} found):\n"
                for p in exception_patterns:
                    global_section += (
                        f"    • {p['exception_type']}: "
                        f"{p['frequency_percentage']}% of cases, "
                        f"{p['case_count']} cases, "
                        f"avg resolution {p['avg_resolution_time_days']} days, "
                        f"trigger: {p['trigger_condition']}, "
                        f"resolved by: {p['typical_resolution']} (role: {p['resolution_role']})\n"
                    )
            else:
                global_section += "  Exception patterns: None detected in the data\n"

            # All conformance violations with full detail
            if conformance:
                global_section += f"  Conformance violations ({len(conformance)} detected):\n"
                for v in conformance:
                    global_section += (
                        f"    • VIOLATION: {v['rule']}\n"
                        f"      Rate: {v['violation_rate']}% of cases "
                        f"({v['affected_cases']} of {v['total_cases']} cases)\n"
                        f"      Description: {v['violation_description']}\n"
                    )
            else:
                global_section += "  Conformance violations: None detected in the data\n"

            if decision_rules:
                global_section += "  Decision rules mined:\n"
                for r in decision_rules:
                    global_section += f"    • {r['condition']} → {r['action']} (confidence: {r['confidence']})\n"

            sections.append(global_section)
            context_used["global"] = {
                "total_cases":          process_ctx.get("total_cases"),
                "avg_end_to_end_days":  process_ctx.get("avg_end_to_end_days"),
                "exception_rate":       process_ctx.get("exception_rate"),
                "bottleneck":           bottleneck,
                "top_variant":          variants[0] if variants else {},
                "conformance_count":    len(conformance),
                "exception_type_count": len(exception_patterns),
            }

        except Exception as exc:
            logger.warning("Could not load global process context: %s", str(exc))
            sections.append("GLOBAL PROCESS STATS\n  [Unavailable — Celonis connection issue]\n")

        # ── 2. Agent intelligence ────────────────────────────────────────────
        try:
            if process_ctx is None:
                process_ctx = self.process_insight.build_process_context()

            agent_result = self.agent_recommendation.recommend_agents(process_ctx)
            top_rec      = agent_result.get("top_recommendation", {})
            agents       = agent_result.get("recommended_agents", [])

            agent_section  = "ACTIVE AGENT RECOMMENDATIONS\n"
            agent_section += f"  Top agent  : {top_rec.get('agent_name', 'N/A')} (priority: {top_rec.get('priority', 'N/A')})\n"
            agent_section += f"  Reasoning  : {top_rec.get('reason', 'N/A')}\n"
            agent_section += f"  Action     : {top_rec.get('timing_decision', 'N/A')}\n"
            agent_section += f"  Impact     : {top_rec.get('action_impact', 'N/A')}\n"

            if agents:
                agent_section += "  All active agents:\n"
                for a in agents[:6]:
                    agent_section += (
                        f"    • {a['agent_name']} [{a['priority']}] — {a['purpose']}\n"
                        f"      Evidence: {a.get('process_mining_evidence', 'N/A')}\n"
                    )

            sections.append(agent_section)
            context_used["agents"] = {"top_recommendation": top_rec, "agent_count": len(agents)}

        except Exception as exc:
            logger.warning("Could not load agent context: %s", str(exc))
            sections.append("ACTIVE AGENT RECOMMENDATIONS\n  [Unavailable]\n")

        # ── 3. Global vendor summary (always included) ───────────────────────
        try:
            vendor_stats = self.celonis.get_vendor_stats_api()
            if vendor_stats:
                sorted_vendors = sorted(
                    vendor_stats,
                    key=lambda v: float(v.get("exception_rate", 0) or 0),
                    reverse=True,
                )
                vendor_section = f"VENDOR SUMMARY ({len(sorted_vendors)} vendors, ranked by exception rate)\n"
                for v in sorted_vendors:
                    vendor_section += (
                        f"  • Vendor {v['vendor_id']}: "
                        f"exception rate {v['exception_rate']}%, "
                        f"avg DPO {v['avg_dpo']} days, "
                        f"risk {v['risk_score']}, "
                        f"{v['total_cases']} cases\n"
                    )
                sections.append(vendor_section)
                context_used["vendor_summary"] = {
                    "total_vendors":            len(vendor_stats),
                    "highest_exception_vendor": sorted_vendors[0] if sorted_vendors else None,
                }
        except Exception as exc:
            logger.warning("Could not load global vendor summary: %s", str(exc))

        # ── 4. Case-specific context ─────────────────────────────────────────
        if case_id:
            try:
                event_log   = self.celonis.get_event_log()
                case_events = event_log[event_log["case_id"].astype(str) == str(case_id)]

                if not case_events.empty:
                    sorted_events = case_events.sort_values("timestamp")
                    activities    = sorted_events["activity"].tolist()
                    current_stage = activities[-1] if activities else "Unknown"
                    start_time    = sorted_events["timestamp"].min()
                    last_time     = sorted_events["timestamp"].max()
                    days_in_process = (
                        (last_time - start_time).total_seconds() / 86400
                        if hasattr(start_time, "timestamp") and hasattr(last_time, "timestamp")
                        else "N/A"
                    )
                    case_section = (
                        f"CASE DETAIL: {case_id}\n"
                        f"  Current stage      : {current_stage}\n"
                        f"  Days in process    : {round(days_in_process, 2) if isinstance(days_in_process, float) else days_in_process}\n"
                        f"  Total activities   : {len(activities)}\n"
                        f"  Full process path  : {' → '.join(activities)}\n"
                    )
                    sections.append(case_section)
                    context_used["case"] = {
                        "case_id": case_id, "current_stage": current_stage,
                        "days_in_process": days_in_process, "activity_count": len(activities),
                    }
                else:
                    sections.append(f"CASE DETAIL: {case_id}\n  [Case not found in event log]\n")

            except Exception as exc:
                logger.warning("Could not load case context for %s: %s", case_id, str(exc))

        # ── 5. Vendor-specific context ───────────────────────────────────────
        if vendor_id:
            try:
                vendor_stats = self.celonis.get_vendor_stats_api()
                vendor = next(
                    (v for v in vendor_stats if str(v.get("vendor_id", "")).upper() == str(vendor_id).upper()),
                    None,
                )
                if vendor:
                    vendor_section = (
                        f"VENDOR DETAIL: {vendor_id}\n"
                        f"  Total cases        : {vendor.get('total_cases', 'N/A')}\n"
                        f"  Exception rate     : {vendor.get('exception_rate', 'N/A')}%\n"
                        f"  Avg DPO (days)     : {vendor.get('avg_dpo', 'N/A')}\n"
                        f"  Risk score         : {vendor.get('risk_score', 'N/A')}\n"
                        f"  Total value        : {vendor.get('total_value', 'N/A')}\n"
                    )
                    sections.append(vendor_section)
                    context_used["vendor"] = vendor
                else:
                    sections.append(f"VENDOR DETAIL: {vendor_id}\n  [Vendor not found]\n")

            except Exception as exc:
                logger.warning("Could not load vendor context for %s: %s", vendor_id, str(exc))

        return "\n".join(sections), context_used

    @staticmethod
    def _build_user_prompt(message: str, history: List[Dict[str, str]]) -> str:
        if not history:
            return message
        history_text = "\n".join(
            f"[{turn['role'].upper()}]: {turn['content']}"
            for turn in history[-6:]
        )
        return f"CONVERSATION HISTORY:\n{history_text}\n\n[USER]: {message}"