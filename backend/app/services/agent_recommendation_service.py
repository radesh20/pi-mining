import json
from typing import Any, Dict, List
from app.services.azure_openai_service import AzureOpenAIService

SYSTEM_PROMPT = """
You are an enterprise process automation architect.
Given process mining insights extracted from Celonis for a Procure-to-Pay (P2P) process,
recommend the optimal set of AI agents needed to automate the end-to-end process.

CRITICAL: Your recommendations must be DERIVED FROM the actual process data provided.
Use the discovered activity sequences, variants, exception patterns, turnaround times,
role mappings, and conformance violations as evidence.

For each recommended agent provide:
1. agent_name — clear descriptive name
2. purpose — what it does and why
3. process_mining_evidence — specific data points that justify this agent
4. activities_covered — list of activities from the process model this agent handles
5. interacts_with — which other recommended agents it communicates with
6. priority — HIGH / MEDIUM / LOW based on case frequency and impact

Return valid JSON:
{
  "recommended_agents": [
    {
      "agent_id": "agent_1",
      "agent_name": "...",
      "purpose": "...",
      "process_mining_evidence": "...",
      "activities_covered": ["..."],
      "interacts_with": ["..."],
      "priority": "HIGH"
    }
  ],
  "rationale": "Overall explanation",
  "data_source": "Celonis Process Mining"
}
"""


class AgentRecommendationService:
    STAGE_DEFS = [
        {
            "stage": "PR_PO_CREATION",
            "agent_name": "PR/PO Creation Agent",
            "keywords": ["create purchase requisition", "create purchase order", "requisition", "purchase order"],
            "purpose": "Create and validate requisition/order initiation flow.",
        },
        {
            "stage": "APPROVAL_ROUTING",
            "agent_name": "Approval Routing Agent",
            "keywords": ["approve", "approval", "release"],
            "purpose": "Route approvals based on observed role and turnaround behavior.",
        },
        {
            "stage": "INVOICE_INTAKE",
            "agent_name": "Invoice Intake Agent",
            "keywords": ["invoice received", "record invoice", "incoming invoice", "vendor generates invoice"],
            "purpose": "Capture and validate invoice intake with process timing awareness.",
        },
        {
            "stage": "EXCEPTION_HANDLING",
            "agent_name": "Invoice Exception Agent",
            "keywords": ["exception", "block", "moved out", "mismatch", "short payment"],
            "purpose": "Resolve/route exception paths based on mined recurrence and resolution history.",
        },
        {
            "stage": "PAYMENT_TIMING",
            "agent_name": "Payment Timing Agent",
            "keywords": ["clear invoice", "payment", "due date", "post payment"],
            "purpose": "Optimize payment timing using turnaround and due-date risk context.",
        },
        {
            "stage": "HUMAN_ESCALATION",
            "agent_name": "Human Review Agent",
            "keywords": ["review", "escalate", "manual", "approval"],
            "purpose": "Prepare high-risk cases for human review with PI evidence.",
        },
    ]

    def __init__(self, llm: AzureOpenAIService):
        self.llm = llm

    def recommend_agents(self, process_context: dict) -> dict:
        user_prompt = f"""
Analyze the following REAL process mining insights from Celonis and recommend AI agents.

=== CELONIS PROCESS MINING DATA ===

CONNECTION: {json.dumps(process_context.get("connection_info", {}), indent=2)}

DISCOVERED ACTIVITIES ({len(process_context["activities"])} unique):
{json.dumps(process_context["activities"], indent=2)}

GOLDEN PATH ({process_context.get("golden_path_percentage", 0)}% of cases):
{process_context["golden_path"]}

PROCESS VARIANTS (top 15):
{json.dumps(process_context["variants"][:15], indent=2)}

TURNAROUND TIMES BETWEEN ACTIVITIES:
{json.dumps(process_context["activity_durations"], indent=2)}

BOTTLENECK:
{json.dumps(process_context["bottleneck"], indent=2)}

EXCEPTION PATTERNS DISCOVERED:
{json.dumps(process_context["exception_patterns"], indent=2)}
Overall Exception Rate: {process_context["exception_rate"]}%

DECISION RULES MINED:
{json.dumps(process_context["decision_rules"], indent=2)}

CONFORMANCE VIOLATIONS DETECTED:
{json.dumps(process_context["conformance_violations"], indent=2)}

ROLE-ACTIVITY MAPPINGS:
{json.dumps(process_context["role_mappings"], indent=2)}

PROCESS STATISTICS:
- Total Cases: {process_context["total_cases"]}
- Total Events: {process_context["total_events"]}
- Avg End-to-End Duration: {process_context["avg_end_to_end_days"]} days

=== END CELONIS DATA ===

Based on this REAL Celonis data, recommend the optimal set of process agents.
Each agent must be justified by specific Celonis process mining evidence.
"""
        try:
            llm_result = self.llm.chat_json(SYSTEM_PROMPT, user_prompt)
            return self._normalize_result(llm_result, process_context)
        except Exception as e:
            return self._fallback_result(process_context, str(e))

    def _normalize_result(self, result: Dict[str, Any], process_context: dict) -> Dict[str, Any]:
        agents = result.get("recommended_agents", [])
        if not isinstance(agents, list) or not agents:
            return self._fallback_result(process_context, "LLM returned empty agents list")

        lifecycle_defaults = self._build_lifecycle_agents(process_context)
        lifecycle_by_stage = {a["lifecycle_stage"]: a for a in lifecycle_defaults}

        normalized_agents: List[Dict[str, Any]] = []
        for idx, agent in enumerate(agents, start=1):
            if not isinstance(agent, dict):
                continue
            stage = self._infer_lifecycle_stage(
                activities=agent.get("activities_covered", []),
                fallback_name=agent.get("agent_name", ""),
            )
            stage_default = lifecycle_by_stage.get(stage, {})
            stage_detail = stage_default.get("stage_detail", {})
            normalized_agents.append(
                {
                    "agent_id": agent.get("agent_id", f"agent_{idx}"),
                    "agent_name": agent.get("agent_name", f"Process Agent {idx}"),
                    "purpose": agent.get("purpose", "Automate process step based on Celonis insights"),
                    "process_mining_evidence": agent.get(
                        "process_mining_evidence",
                        "Derived from Celonis variants, throughput, and conformance data",
                    ),
                    "activities_covered": agent.get("activities_covered", process_context.get("activities", [])[:5]),
                    "interacts_with": agent.get("interacts_with", []),
                    "priority": str(agent.get("priority", "MEDIUM")).upper(),
                    "lifecycle_stage": stage,
                    "pi_timing_context": agent.get(
                        "pi_timing_context",
                        stage_default.get("pi_timing_context", self._default_timing_context(process_context)),
                    ),
                    "why_bi_only_misses_this": agent.get(
                        "why_bi_only_misses_this",
                        "BI can show open/late status but cannot infer path-level turnaround and conformance context.",
                    ),
                    "pi_evidence": agent.get("pi_evidence", stage_detail.get("pi_evidence", "")),
                    "action_impact": agent.get("action_impact", stage_detail.get("action_impact", "")),
                    "timing_decision": agent.get("timing_decision", stage_detail.get("timing_decision", "")),
                    "expected_turnaround_days": agent.get(
                        "expected_turnaround_days",
                        stage_detail.get("expected_turnaround_days", process_context.get("avg_end_to_end_days", 0)),
                    ),
                    "top_transitions": stage_detail.get("top_transitions", []),
                    "variant_confidence": stage_detail.get("variant_confidence", 0),
                    "guardrail_trigger": stage_detail.get("guardrail_trigger", ""),
                }
            )

        if not normalized_agents:
            return self._fallback_result(process_context, "LLM agent payload was not usable")

        covered_stages = {a.get("lifecycle_stage") for a in normalized_agents}
        for default_agent in lifecycle_defaults:
            if default_agent["lifecycle_stage"] not in covered_stages:
                # 🐛 BUG FIXED from original code: removed leak of internal stage_detail object
                clean_default = default_agent.copy()
                clean_default.pop("stage_detail", None)
                normalized_agents.append(clean_default)

        normalized_agents = normalized_agents[:6]

        # --- New logic added here ---
        top_recommendation = self._get_top_recommendation_summary(normalized_agents, process_context)
        lifecycle_map = self._build_lifecycle_map(normalized_agents, process_context)

        return {
            "recommended_agents": normalized_agents,
            "top_recommendation": top_recommendation,
            "rationale": result.get(
                "rationale",
                "Recommendations derived from Celonis process mining statistics and flow patterns.",
            ),
            "data_source": result.get("data_source", "Celonis Process Mining"),
            "lifecycle_coverage": self._build_lifecycle_coverage(normalized_agents),
            "lifecycle_map": lifecycle_map,
            "pi_vs_bi_message": "Recommendations are based on observed turnaround and variant behavior, not only static exposure logic.",
            "critical_timing_scenario": self._critical_timing_scenario(process_context),
        }

    def _fallback_result(self, process_context: dict, reason: str) -> Dict[str, Any]:
        agents = self._build_lifecycle_agents(process_context)
        top_recommendation = self._get_top_recommendation_summary(agents, process_context)

        return {
            "recommended_agents": agents,
            "top_recommendation": top_recommendation,
            "rationale": (
                "Fallback recommendation generated from deterministic Celonis insights because the LLM response "
                f"was unavailable or invalid ({reason})."
            ),
            "data_source": "Celonis Process Mining",
            "lifecycle_coverage": self._build_lifecycle_coverage(agents),
            "lifecycle_map": self._build_lifecycle_map(agents, process_context),
            "pi_vs_bi_message": "Process-mining turnaround context drives agent design beyond BI exposure summaries.",
            "critical_timing_scenario": self._critical_timing_scenario(process_context),
        }

    def _build_lifecycle_agents(self, process_context: dict) -> List[Dict[str, Any]]:
        activities = process_context.get("activities", []) or []
        out: List[Dict[str, Any]] = []
        for idx, stage_def in enumerate(self.STAGE_DEFS, start=1):
            stage = stage_def["stage"]
            name = stage_def["agent_name"]
            keywords = stage_def["keywords"]
            covered = [a for a in activities if any(k in a.lower() for k in keywords)]
            if stage in {"PR_PO_CREATION", "APPROVAL_ROUTING", "INVOICE_INTAKE"} and not covered:
                covered = activities[:2]
            if stage == "PAYMENT_TIMING" and not covered:
                covered = [a for a in activities if "invoice" in a.lower()][:2]
            stage_detail = self._build_stage_detail(stage, process_context, covered)
            out.append(
                {
                    "agent_id": f"agent_{idx}_{stage.lower()}",
                    "agent_name": name,
                    "purpose": stage_def["purpose"],
                    "process_mining_evidence": stage_detail["pi_evidence"],
                    "activities_covered": covered[:6],
                    "interacts_with": [],
                    "priority": "HIGH" if stage in {"EXCEPTION_HANDLING", "PAYMENT_TIMING"} else "MEDIUM",
                    "lifecycle_stage": stage,
                    "pi_timing_context": stage_detail["pi_timing_context"],
                    "why_bi_only_misses_this": stage_detail["why_bi_only_misses_this"],
                    "pi_evidence": stage_detail["pi_evidence"],
                    "action_impact": stage_detail["action_impact"],
                    "timing_decision": stage_detail["timing_decision"],
                    "expected_turnaround_days": stage_detail["expected_turnaround_days"],
                    "top_transitions": stage_detail["top_transitions"],
                    "variant_confidence": stage_detail["variant_confidence"],
                    "guardrail_trigger": stage_detail["guardrail_trigger"],
                    "kpi_snapshot": stage_detail["kpi_snapshot"],
                    "stage_detail": stage_detail,
                }
            )
        return out

    def _build_stage_detail(self, stage: str, process_context: dict, covered: List[str]) -> Dict[str, Any]:
        avg_e2e = float(process_context.get("avg_end_to_end_days", 0) or 0)
        exception_rate = float(process_context.get("exception_rate", 0) or 0)
        bottleneck = process_context.get("bottleneck", {}) or {}
        stage_cfg = next((item for item in self.STAGE_DEFS if item["stage"] == stage), None)
        keywords = stage_cfg["keywords"] if stage_cfg else []
        top_transitions = self._top_transitions_for_keywords(process_context, keywords)
        stage_turnaround = self._stage_turnaround_days(process_context, keywords)
        golden_path_pct = self._golden_path_percentage(process_context)
        variant_confidence = self._variant_confidence(process_context, stage)

        pi_evidence = (
            f"Stage turnaround {stage_turnaround}d from Process Explorer transitions; "
            f"golden-path coverage {golden_path_pct}% and exception rate {round(exception_rate, 2)}%."
        )

        if stage == "EXCEPTION_HANDLING":
            pi_evidence = (
                f"Exception patterns {len(process_context.get('exception_patterns', []))} with {round(exception_rate, 2)}% rate; "
                f"historical resolution behavior drives exception routing."
            )
        if stage == "PAYMENT_TIMING":
            pi_evidence = (
                f"Payment timing uses measured turnaround {stage_turnaround}d vs due-date slack; "
                f"bottleneck transition {bottleneck.get('activity', 'N/A')}={bottleneck.get('duration_days', 0)}d."
            )

        timing_decision = (
            f"Act when remaining due-date buffer is below {max(stage_turnaround, 1)} days "
            f"to avoid late completion on this path."
        )
        if stage == "PAYMENT_TIMING":
            timing_decision = (
                f"Trigger pay/escalate if days-until-due <= {max(stage_turnaround, 1)}d "
                "because this path historically consumes that time."
            )

        return {
            "pi_evidence": pi_evidence,
            "action_impact": self._stage_action_impact(stage),
            "timing_decision": timing_decision,
            "expected_turnaround_days": stage_turnaround,
            "pi_timing_context": (
                f"Expected stage turnaround {stage_turnaround}d; "
                f"avg E2E {avg_e2e}d; bottleneck {bottleneck.get('activity', 'N/A')}."
            ),
            "why_bi_only_misses_this": (
                "BI exposes open/late amounts but misses path-specific transition durations, "
                "variant recurrence, and conformance-linked timing risk."
            ),
            "top_transitions": top_transitions,
            "variant_confidence": variant_confidence,
            "guardrail_trigger": self._stage_guardrail(stage, process_context),
            "kpi_snapshot": {
                "exception_rate": round(exception_rate, 2),
                "avg_end_to_end_days": avg_e2e,
                "transition_count": len(process_context.get("activity_durations", {}) or {}),
                "activities_covered": covered[:4],
            },
        }

    @staticmethod
    def _stage_action_impact(stage: str) -> str:
        return {
            "PR_PO_CREATION": "Reduces upstream delay compounding and prevents late downstream invoice handling.",
            "APPROVAL_ROUTING": "Cuts approval queue time by routing to historically fastest compliant role.",
            "INVOICE_INTAKE": "Improves first-pass invoice validation and shortens queue aging.",
            "EXCEPTION_HANDLING": "Lowers rework and aged-exception backlog using observed resolution patterns.",
            "PAYMENT_TIMING": "Prevents due-date misses by aligning payment action to historical processing lead time.",
            "HUMAN_ESCALATION": "Escalates only when PI evidence indicates high timing or conformance risk.",
        }.get(stage, "Improves lifecycle execution with PI-derived action timing.")

    @staticmethod
    def _stage_guardrail(stage: str, process_context: dict) -> str:
        conformance_count = len(process_context.get("conformance_violations", []) or [])
        if stage in {"PAYMENT_TIMING", "EXCEPTION_HANDLING"}:
            return f"Escalate when conformance-violation risk is high (violations observed: {conformance_count})."
        return "Require PI-evidence citation before autonomous execution."

    @staticmethod
    def _golden_path_percentage(process_context: dict) -> float:
        variants = process_context.get("variants", []) or []
        gp = variants[0] if variants else {}
        return float(gp.get("percentage", process_context.get("golden_path_percentage", 0)) or 0)

    def _top_transitions_for_keywords(self, process_context: dict, keywords: List[str]) -> List[Dict[str, Any]]:
        transitions = process_context.get("activity_durations", {}) or {}
        scored: List[Dict[str, Any]] = []
        for transition, days in transitions.items():
            transition_lower = str(transition).lower()
            if any(k in transition_lower for k in keywords):
                scored.append({"transition": transition, "avg_days": round(float(days or 0), 2)})
        scored.sort(key=lambda item: item["avg_days"], reverse=True)
        return scored[:3]

    def _stage_turnaround_days(self, process_context: dict, keywords: List[str]) -> float:
        transitions = process_context.get("activity_durations", {}) or {}
        values: List[float] = []
        for transition, days in transitions.items():
            transition_lower = str(transition).lower()
            if any(k in transition_lower for k in keywords):
                values.append(float(days or 0))
        if values:
            return round(sum(values) / len(values), 2)
        return round(float(process_context.get("avg_end_to_end_days", 0) or 0), 2)

    @staticmethod
    def _variant_confidence(process_context: dict, stage: str) -> float:
        variants = process_context.get("variants", []) or []
        if not variants:
            return 0.0
        baseline = float(variants[0].get("percentage", 0) or 0) / 100.0
        if stage == "EXCEPTION_HANDLING":
            return round(max(0.3, min(0.95, baseline + 0.2)), 2)
        return round(max(0.2, min(0.9, baseline + 0.05)), 2)

    @staticmethod
    def _stage_purpose(stage: str) -> str:
        return {
            "PR_PO_CREATION": "Create and validate requisition/order initiation flow.",
            "APPROVAL_ROUTING": "Route approvals based on observed role and turnaround behavior.",
            "INVOICE_INTAKE": "Capture and validate invoice intake with process timing awareness.",
            "EXCEPTION_HANDLING": "Resolve/route exception paths based on mined recurrence and resolution history.",
            "PAYMENT_TIMING": "Optimize payment timing using turnaround and due-date risk context.",
            "HUMAN_ESCALATION": "Prepare high-risk cases for human review with PI evidence.",
        }.get(stage, "Automate lifecycle stage using process-mining evidence.")

    @staticmethod
    def _infer_lifecycle_stage(activities: List[str], fallback_name: str) -> str:
        txt = " ".join([str(a).lower() for a in (activities or [])]) + " " + str(fallback_name).lower()
        if "requisition" in txt or "purchase order" in txt:
            return "PR_PO_CREATION"
        if "approval" in txt:
            return "APPROVAL_ROUTING"
        if "invoice received" in txt or "vendor generates invoice" in txt:
            return "INVOICE_INTAKE"
        if "exception" in txt or "block" in txt or "moved out" in txt:
            return "EXCEPTION_HANDLING"
        if "clear invoice" in txt or "due date" in txt or "payment" in txt:
            return "PAYMENT_TIMING"
        return "HUMAN_ESCALATION"

    @staticmethod
    def _default_timing_context(process_context: dict) -> str:
        b = process_context.get("bottleneck", {})
        return f"Avg E2E {process_context.get('avg_end_to_end_days', 0)}d; bottleneck {b.get('activity', 'N/A')}={b.get('duration_days', 0)}d."

    @staticmethod
    def _build_lifecycle_coverage(agents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "lifecycle_stage": a.get("lifecycle_stage", "UNSPECIFIED"),
                "agent_name": a.get("agent_name", ""),
                "activities_covered_count": len(a.get("activities_covered", []) or []),
            }
            for a in agents
        ]

    def _build_lifecycle_map(self, agents: List[Dict[str, Any]], process_context: dict) -> List[Dict[str, Any]]:
        by_stage = {a.get("lifecycle_stage"): a for a in agents}
        lifecycle_map: List[Dict[str, Any]] = []
        for stage_def in self.STAGE_DEFS:
            stage = stage_def["stage"]
            agent = by_stage.get(stage)
            if not agent:
                default = self._build_lifecycle_agents(process_context)
                agent = next((item for item in default if item.get("lifecycle_stage") == stage), {})
            lifecycle_map.append(
                {
                    "lifecycle_stage": stage,
                    "agent_name": agent.get("agent_name", stage_def["agent_name"]),
                    "pi_evidence": agent.get("pi_evidence", agent.get("process_mining_evidence", "")),
                    "expected_turnaround_days": agent.get(
                        "expected_turnaround_days",
                        process_context.get("avg_end_to_end_days", 0),
                    ),
                    "timing_decision": agent.get("timing_decision", ""),
                    "action_impact": agent.get("action_impact", ""),
                    "why_bi_only_misses_this": agent.get("why_bi_only_misses_this", ""),
                    "top_transitions": agent.get("top_transitions", []),
                    "guardrail_trigger": agent.get("guardrail_trigger", ""),
                }
            )
        return lifecycle_map

    def _critical_timing_scenario(self, process_context: dict) -> Dict[str, Any]:
        avg_e2e = float(process_context.get("avg_end_to_end_days", 0) or 0)
        payment_turnaround = self._stage_turnaround_days(
            process_context,
            ["clear invoice", "payment", "due date", "post payment"],
        )
        days_until_due = 7
        safe_buffer_days = max(2.0, payment_turnaround)
        return {
            "scenario": "Due in 7 days with historical processing lead-time context.",
            "days_until_due": days_until_due,
            "historical_processing_days": payment_turnaround,
            "pi_recommendation": (
                "Act now" if payment_turnaround >= 3 else "Monitor closely and execute within safe buffer"
            ),
            "timing_rationale": (
                f"Process Explorer shows this path needs ~{payment_turnaround} days "
                f"(avg E2E {avg_e2e} days), so waiting can compress execution margin."
            ),
            "why_bi_only_misses_this": (
                "BI sees due date and open amount, but PI contributes transition-level turnaround needed for action timing."
            ),
            "safe_buffer_days": safe_buffer_days,
        }

    def _get_top_recommendation_summary(self, agents: List[Dict[str, Any]], process_context: dict) -> Dict[str, Any]:
        """
        Extract the highest-priority agent as the top recommendation with condensed PI evidence.
        """
        if not agents:
            return {}

        # Sort by priority (HIGH > MEDIUM > LOW), then by variant_confidence
        priority_map = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        sorted_agents = sorted(
            agents,
            key=lambda a: (
                priority_map.get(a.get("priority", "MEDIUM"), 2),
                a.get("variant_confidence", 0)
            ),
            reverse=True
        )

        top_agent = sorted_agents[0]

        # Compute aggregate metrics
        variants = process_context.get("variants", []) or []
        exceptions = process_context.get("exception_patterns", []) or []
        total_cases = process_context.get("total_cases", 1)

        matching_cases = sum(v.get("frequency", 0) for v in variants[:3])  # Top 3 variants
        exc_cases = sum(e.get("case_count", 0) for e in exceptions)

        variant_frequency_pct = (matching_cases / max(total_cases, 1)) * 100
        exception_rate_pct = (exc_cases / max(total_cases, 1)) * 100

        bottleneck = process_context.get("bottleneck", {}) or {}

        return {
            "agent_name": top_agent.get("agent_name", ""),
            "agent_id": top_agent.get("agent_id", ""),
            "confidence_score": top_agent.get("variant_confidence", 0),
            "priority": top_agent.get("priority", "MEDIUM"),
            "reason": top_agent.get("purpose", ""),
            "pi_evidence": {
                "variant_frequency_pct": round(variant_frequency_pct, 1),
                "exception_rate_pct": round(exception_rate_pct, 1),
                "turnaround_bottleneck": bottleneck.get("activity", "N/A"),
                "bottleneck_duration_days": round(float(bottleneck.get("duration_days", 0) or 0), 2),
                "affected_case_count": int(matching_cases),
                "total_impact_value": float(process_context.get("total_invoice_value", 0.0) or 0.0),
                "expected_turnaround_days": top_agent.get("expected_turnaround_days", 0),
            },
            "timing_decision": top_agent.get("timing_decision", ""),
            "action_impact": top_agent.get("action_impact", ""),
        }
