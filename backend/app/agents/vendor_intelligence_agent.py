from typing import Any, Dict, List

from app.agents.base_agent import BaseAgent
from app.services.azure_openai_service import AzureOpenAIService

try:
    from backend.app.prompts.prompt_loader import load_prompt
except ModuleNotFoundError:
    from app.prompts.prompt_loader import load_prompt


class VendorIntelligenceAgent(BaseAgent):
    def __init__(self, llm: AzureOpenAIService, process_context: Dict):
        super().__init__(
            agent_id="vendor_intelligence_agent",
            agent_name="Vendor Intelligence Agent",
            llm=llm,
            process_context=process_context,
            guardrails=[
                "Vendor analysis must use process and vendor evidence from Celonis context.",
                "Risk score must include frequency, value exposure, DPO behavior, and payment behavior.",
                "Recommendations must be actionable and role-aware.",
            ],
        )
        self.prompt_config = load_prompt("vendor_intelligence_agent")

    def process(self, input_data: Dict) -> Dict:
        import json

        prompt_config = load_prompt(
            "vendor_intelligence_agent",
            input_data_json=json.dumps(input_data, indent=2, default=str),
            known_vendor_data_json=json.dumps(self._known_vendor_data(), indent=2, default=str),
            process_context_json=json.dumps(self.process_context, indent=2, default=str),
        )
        result = self.reason_json(
            prompt_config["system_prompt"],
            prompt_config["user_prompt"],
            prompt_purpose="Assess vendor behavior and risk from Celonis process context",
            message_bus_input=input_data,
        )
        normalized = self._normalize_result(result, input_data)
        return self.attach_prompt_trace(normalized)

    def _normalize_result(self, result: Dict, input_data: Dict) -> Dict:
        result = result if isinstance(result, dict) else {}
        target_vendor = input_data.get("vendor_id", "UNKNOWN")
        deterministic = self._build_deterministic_vendor_analysis(input_data)
        result["vendor_id"] = result.get("vendor_id", target_vendor)
        result["vendor_analysis"] = self._merge_vendor_analysis(
            deterministic,
            result.get("vendor_analysis", {}),
        )
        result["ai_recommendations"] = result.get("ai_recommendations") or deterministic.get("default_recommendations", [])
        result["celonis_evidence"] = result.get(
            "celonis_evidence",
            "Celonis context was provided in prompt; LLM did not return specific evidence citation."
            if self._context_available()
            else "[Celonis data unavailable for this request]",
        )
        result["ai_reasoning"] = result.get(
            "ai_reasoning",
            "GPT-4o synthesized vendor risk and optimization recommendations from process mining context.",
        )
        self._provenance_tag(result)
        return result

    def _merge_vendor_analysis(self, deterministic: Dict[str, Any], llm_analysis: Dict[str, Any]) -> Dict[str, Any]:
        llm_analysis = llm_analysis if isinstance(llm_analysis, dict) else {}
        merged = {
            "happy_path_percentage": float(
                llm_analysis.get("happy_path_percentage", deterministic.get("happy_path_percentage", 0.0)) or 0.0
            ),
            "exception_breakdown": deterministic.get("exception_breakdown", {}).copy(),
            "vendor_risk_score": llm_analysis.get("vendor_risk_score") or deterministic.get("vendor_risk_score", "MEDIUM"),
            "payment_behavior": deterministic.get("payment_behavior", {}).copy(),
        }

        llm_behavior = llm_analysis.get("payment_behavior", {})
        if isinstance(llm_behavior, dict):
            merged["payment_behavior"] = {
                **merged["payment_behavior"],
                **{k: v for k, v in llm_behavior.items() if v is not None},
            }
        return merged

    def _build_deterministic_vendor_analysis(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        vendor_context = input_data.get("vendor_context", {}) if isinstance(input_data.get("vendor_context"), dict) else {}
        vendor_paths = input_data.get("vendor_paths", {}) if isinstance(input_data.get("vendor_paths"), dict) else {}
        exception_breakdown = vendor_context.get("exception_breakdown") or {
            "payment_terms_mismatch": {"count": 0, "percentage": 0.0, "value": 0.0},
            "invoice_exception": {"count": 0, "percentage": 0.0, "avg_dpo": 0.0, "value": 0.0, "time_stuck_days": 0.0},
            "short_payment_terms": {"count": 0, "percentage": 0.0, "value": 0.0, "risk_level": "LOW"},
            "early_payment": {"count": 0, "percentage": 0.0, "optimization_value": 0.0, "value": 0.0},
        }
        payment_behavior = vendor_context.get("payment_behavior") or {"on_time_pct": 0.0, "late_pct": 0.0, "early_pct": 0.0, "open_pct": 0.0}

        happy_paths = vendor_paths.get("happy_paths", []) if isinstance(vendor_paths.get("happy_paths"), list) else []
        exception_paths = vendor_paths.get("exception_paths", []) if isinstance(vendor_paths.get("exception_paths"), list) else []
        happy_pct = 0.0
        if happy_paths:
            true_happy = [path for path in happy_paths if not path.get("derived_from")]
            source = true_happy if true_happy else happy_paths
            happy_pct = float(sum(float(path.get("percentage", 0) or 0) for path in source))
            if happy_pct > 100:
                happy_pct = 100.0

        risk_score = vendor_context.get("risk_score")
        if not risk_score:
            late_pct = float(payment_behavior.get("late_pct", 0) or 0)
            exception_pct = float(vendor_context.get("exception_rate", 0) or 0)
            avg_dpo = float(vendor_context.get("avg_dpo", 0) or 0)
            if exception_pct >= 60 or late_pct >= 50 or avg_dpo >= 60:
                risk_score = "CRITICAL"
            elif exception_pct >= 40 or late_pct >= 30 or avg_dpo >= 40:
                risk_score = "HIGH"
            elif exception_pct >= 20 or avg_dpo >= 20:
                risk_score = "MEDIUM"
            else:
                risk_score = "LOW"

        recommendations: List[str] = []
        if int(exception_breakdown.get("invoice_exception", {}).get("count", 0) or 0) > 0:
            recommendations.append("Prioritize invoice-exception handoff using Celonis turnaround evidence and exception-path frequency.")
        if int(exception_breakdown.get("payment_terms_mismatch", {}).get("count", 0) or 0) > 0:
            recommendations.append("Resolve payment-term mismatches by reconciling invoice, PO, and vendor-master terms before approval.")
        if int(exception_breakdown.get("short_payment_terms", {}).get("count", 0) or 0) > 0:
            recommendations.append("Correct short payment terms to prevent avoidable early cash outflow.")
        if float(payment_behavior.get("late_pct", 0) or 0) > 0:
            recommendations.append("Escalate this vendor earlier when due-date runway is shorter than historical processing time.")
        if not recommendations:
            recommendations.append("Maintain current flow and monitor for new exception variants.")

        return {
            "happy_path_percentage": round(happy_pct, 2),
            "exception_breakdown": exception_breakdown,
            "vendor_risk_score": risk_score,
            "payment_behavior": payment_behavior,
            "default_recommendations": recommendations,
            "exception_path_count": len(exception_paths),
        }

    def _known_vendor_data(self) -> Dict:
        """Extract vendor anchors from live process_context. No hardcoded values."""
        ctx = self.process_context or {}
        vendor_stats = ctx.get("vendor_stats", [])
        if not vendor_stats and isinstance(ctx, dict):
            vendor_stats = []
        vendors = []
        for vs in vendor_stats:
            if not isinstance(vs, dict):
                continue
            vendors.append({
                "vendor_id": vs.get("vendor_id", ""),
                "invoice_count": int(vs.get("total_cases", 0) or 0) or None,
                "value_usd": float(vs.get("total_value", 0) or 0) or None,
                "_data_source": "celonis",
            })
        if not vendors:
            return {"vendors": [], "_data_source": "unavailable"}
        return {"vendors": vendors, "_data_source": "celonis"}
