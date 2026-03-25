import logging
from typing import Dict, List, Tuple

import pandas as pd

from app.services.celonis_service import CelonisService

logger = logging.getLogger(__name__)


class ProcessInsightService:
    """
    Process intelligence engine that computes context from Celonis event logs.
    """

    VARIANT_ARROW = " → "
    EXCEPTION_KEYWORDS = {
        "Invoice Exception": ["exception"],
        "Late Processing / Past Due": ["due date passed"],
        "Blocked PO/Invoice": ["block"],
        "Invoice Moved Out of VIM": ["moved out"],
    }

    def __init__(self, celonis_service: CelonisService):
        self.celonis = celonis_service

    def build_process_context(self) -> Dict:
        logger.info("Building process context from Celonis event log...")
        events = self._prepare_event_log(self.celonis.get_event_log())
        total_cases = int(events["case_id"].nunique()) if not events.empty else 0

        activities = self._compute_activities(events)
        variants_df, case_variant_map = self._compute_variants(events)
        throughput_df = self._compute_throughput(events)
        case_duration_df = self._compute_case_durations(events)
        bottleneck = self._compute_bottleneck(throughput_df)
        role_mappings = self._compute_role_mappings(events)
        exception_patterns = self._compute_exception_patterns(events, total_cases)
        conformance_violations = self._compute_conformance_violations(events, total_cases)
        decision_rules = self._compute_decision_rules(events, total_cases)
        vendor_stats = self._compute_vendor_stats(
            events=events,
            case_duration_df=case_duration_df,
            case_variant_map=case_variant_map,
        )

        activity_durations = {
            f"{row['source_activity']}{self.VARIANT_ARROW}{row['target_activity']}": self._safe_round(
                row["avg_duration_days"]
            )
            for _, row in throughput_df.iterrows()
        }
        variants = variants_df.to_dict(orient="records")
        throughput_times = throughput_df.to_dict(orient="records")
        avg_end_to_end_days = self._safe_round(case_duration_df["duration_days"].mean()) if not case_duration_df.empty else 0.0

        golden_path = variants[0]["variant"] if variants else ""
        golden_path_percentage = variants[0]["percentage"] if variants else 0.0

        exception_case_ids = self._get_exception_case_ids(events)
        exception_rate = self._safe_round((len(exception_case_ids) / total_cases * 100) if total_cases else 0.0)

        context = {
            "total_cases": total_cases,
            "total_events": int(len(events)),
            "activities": activities,
            "golden_path": golden_path,
            "golden_path_percentage": golden_path_percentage,
            "variants": variants,
            "activity_durations": activity_durations,
            "throughput_times": throughput_times,
            "bottleneck": bottleneck,
            "avg_end_to_end_days": avg_end_to_end_days,
            "case_durations": case_duration_df.to_dict(orient="records"),
            "exception_patterns": exception_patterns,
            "exception_rate": exception_rate,
            "decision_rules": decision_rules,
            "conformance_violations": conformance_violations,
            "role_mappings": role_mappings,
            "vendor_stats": vendor_stats,
            "connection_info": self.celonis.get_connection_info(),
        }
        return context

    def _prepare_event_log(self, events: pd.DataFrame) -> pd.DataFrame:
        if events is None or events.empty:
            return pd.DataFrame(
                columns=[
                    "case_id",
                    "activity",
                    "timestamp",
                    "resource",
                    "resource_role",
                    "document_number",
                    "transaction_code",
                ]
            )

        df = events.copy()
        df["case_id"] = df["case_id"].astype(str)
        df["activity"] = df["activity"].fillna("UNKNOWN").astype(str)
        df["resource_role"] = df["resource_role"].fillna("UNKNOWN").astype(str)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.sort_values(["case_id", "timestamp"], na_position="last").reset_index(drop=True)
        return df

    def _compute_activities(self, events: pd.DataFrame) -> List[str]:
        if events.empty:
            return []
        freq = (
            events.groupby("activity")
            .size()
            .reset_index(name="frequency")
            .sort_values("frequency", ascending=False)
        )
        return freq["activity"].tolist()

    def _compute_variants(self, events: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if events.empty:
            empty_variants = pd.DataFrame(columns=["variant", "frequency", "percentage"])
            empty_case_map = pd.DataFrame(columns=["case_id", "variant"])
            return empty_variants, empty_case_map

        case_variant_map = (
            events.groupby("case_id")["activity"]
            .apply(lambda x: self.VARIANT_ARROW.join(x.astype(str).tolist()))
            .reset_index(name="variant")
        )
        variants = (
            case_variant_map.groupby("variant")
            .size()
            .reset_index(name="frequency")
            .sort_values("frequency", ascending=False)
            .reset_index(drop=True)
        )
        total = variants["frequency"].sum()
        variants["percentage"] = (
            (variants["frequency"] / total * 100).round(2) if total else 0.0
        )
        return variants, case_variant_map

    def _compute_throughput(self, events: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "source_activity",
            "target_activity",
            "avg_duration_days",
            "median_duration_days",
            "case_count",
        ]
        if events.empty:
            return pd.DataFrame(columns=columns)

        transitions: List[Dict] = []
        for case_id, group in events.groupby("case_id"):
            ordered = group.sort_values("timestamp").reset_index(drop=True)
            for idx in range(len(ordered) - 1):
                src = ordered.iloc[idx]
                tgt = ordered.iloc[idx + 1]
                duration_days = None
                if pd.notnull(src["timestamp"]) and pd.notnull(tgt["timestamp"]):
                    duration_days = (tgt["timestamp"] - src["timestamp"]).total_seconds() / 86400
                transitions.append(
                    {
                        "case_id": case_id,
                        "source_activity": src["activity"],
                        "target_activity": tgt["activity"],
                        "duration_days": duration_days,
                    }
                )

        if not transitions:
            return pd.DataFrame(columns=columns)

        trans_df = pd.DataFrame(transitions)
        result = (
            trans_df.groupby(["source_activity", "target_activity"])
            .agg(
                avg_duration_days=("duration_days", "mean"),
                median_duration_days=("duration_days", "median"),
                case_count=("case_id", "nunique"),
            )
            .reset_index()
            .sort_values("avg_duration_days", ascending=False, na_position="last")
            .reset_index(drop=True)
        )
        result["avg_duration_days"] = result["avg_duration_days"].round(4)
        result["median_duration_days"] = result["median_duration_days"].round(4)
        return result

    def _compute_bottleneck(self, throughput_df: pd.DataFrame) -> Dict:
        if throughput_df.empty:
            return {"activity": "N/A", "duration_days": 0.0}

        row = throughput_df.sort_values("avg_duration_days", ascending=False, na_position="last").iloc[0]
        return {
            "activity": f"{row['source_activity']}{self.VARIANT_ARROW}{row['target_activity']}",
            "duration_days": self._safe_round(row["avg_duration_days"]),
            "case_count": int(row["case_count"]),
        }

    def _compute_case_durations(self, events: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            return pd.DataFrame(columns=["case_id", "start_time", "end_time", "duration_days"])

        case_duration_df = (
            events.groupby("case_id")
            .agg(start_time=("timestamp", "min"), end_time=("timestamp", "max"))
            .reset_index()
        )
        case_duration_df["duration_days"] = (
            (case_duration_df["end_time"] - case_duration_df["start_time"]).dt.total_seconds() / 86400
        )
        case_duration_df["duration_days"] = case_duration_df["duration_days"].round(4)
        return case_duration_df

    def _compute_exception_patterns(self, events: pd.DataFrame, total_cases: int) -> List[Dict]:
        if events.empty or total_cases == 0:
            return []

        patterns: List[Dict] = []
        for exception_type, keywords in self.EXCEPTION_KEYWORDS.items():
            case_ids, resolution_events = self._extract_exception_resolution(events, keywords)
            case_count = len(case_ids)
            if case_count == 0:
                continue

            frequency = self._safe_round(case_count / total_cases * 100)
            typical_resolution = "N/A"
            resolution_role = "UNKNOWN"
            avg_resolution_time_days = 0.0

            if not resolution_events.empty:
                next_activity_mode = resolution_events["next_activity"].dropna().mode()
                next_role_mode = resolution_events["next_resource_role"].dropna().mode()
                typical_resolution = (
                    str(next_activity_mode.iloc[0]) if not next_activity_mode.empty else "N/A"
                )
                resolution_role = (
                    str(next_role_mode.iloc[0]) if not next_role_mode.empty else "UNKNOWN"
                )
                avg_resolution_time_days = self._safe_round(resolution_events["resolution_time_days"].mean())

            patterns.append(
                {
                    "exception_type": exception_type,
                    "trigger_condition": f"Activity contains one of: {', '.join(keywords)}",
                    "frequency_percentage": frequency,
                    "typical_resolution": typical_resolution,
                    "resolution_role": resolution_role,
                    "avg_resolution_time_days": avg_resolution_time_days,
                    "case_count": case_count,
                }
            )

        patterns.sort(key=lambda x: x["frequency_percentage"], reverse=True)
        return patterns

    def _extract_exception_resolution(self, events: pd.DataFrame, keywords: List[str]) -> Tuple[set, pd.DataFrame]:
        keyword_pattern = "|".join([k.lower() for k in keywords])
        records: List[Dict] = []
        case_ids = set()

        for case_id, group in events.groupby("case_id"):
            ordered = group.sort_values("timestamp").reset_index(drop=True)
            activities = ordered["activity"].str.lower().fillna("")
            exception_idx = ordered[activities.str.contains(keyword_pattern, regex=True, na=False)].index.tolist()
            if not exception_idx:
                continue

            case_ids.add(case_id)
            for idx in exception_idx:
                if idx + 1 >= len(ordered):
                    continue
                current = ordered.iloc[idx]
                nxt = ordered.iloc[idx + 1]
                duration = None
                if pd.notnull(current["timestamp"]) and pd.notnull(nxt["timestamp"]):
                    duration = (nxt["timestamp"] - current["timestamp"]).total_seconds() / 86400
                records.append(
                    {
                        "case_id": case_id,
                        "exception_activity": current["activity"],
                        "next_activity": nxt["activity"],
                        "next_resource_role": nxt["resource_role"],
                        "resolution_time_days": duration,
                    }
                )

        return case_ids, pd.DataFrame(records)

    def _compute_conformance_violations(self, events: pd.DataFrame, total_cases: int) -> List[Dict]:
        if events.empty or total_cases == 0:
            return []

        clear_before_received = set()
        invoice_before_gr = set()
        approval_skipped = set()

        for case_id, group in events.groupby("case_id"):
            acts = group.sort_values("timestamp")["activity"].astype(str).str.lower().tolist()

            clear_idx = self._first_idx(acts, ["clear invoice", "clear"])
            invoice_received_idx = self._first_idx(
                acts,
                [
                    "invoice received in vim",
                    "invoice received",
                    "record invoice receipt",
                    "document item incoming invoice",
                ],
            )
            goods_receipt_idx = self._first_idx(acts, ["record goods receipt", "goods receipt"])
            create_po_idx = self._first_idx(acts, ["create purchase order", "create purchase order item"])
            approval_idx = self._first_idx(acts, ["approval", "approve"])
            invoice_signal_idx = self._first_idx(
                acts,
                ["vendor generates invoice", "invoice received", "record invoice receipt", "document item incoming invoice"],
            )

            if clear_idx is not None and invoice_received_idx is not None and clear_idx < invoice_received_idx:
                clear_before_received.add(case_id)
            if goods_receipt_idx is not None and invoice_received_idx is not None and invoice_received_idx < goods_receipt_idx:
                invoice_before_gr.add(case_id)
            if create_po_idx is not None and invoice_signal_idx is not None and approval_idx is None and create_po_idx < invoice_signal_idx:
                approval_skipped.add(case_id)

        violations: List[Dict] = []
        violations.extend(
            self._build_violation_records(
                rule="Invoice cleared before invoice received",
                description="Clear invoice event appears before invoice receipt in the same case.",
                case_ids=clear_before_received,
                total_cases=total_cases,
            )
        )
        violations.extend(
            self._build_violation_records(
                rule="Invoice received before goods receipt",
                description="Invoice appears before goods receipt when both events exist.",
                case_ids=invoice_before_gr,
                total_cases=total_cases,
            )
        )
        violations.extend(
            self._build_violation_records(
                rule="PO approval skipped",
                description="Case includes create PO and invoice progression without an approval step.",
                case_ids=approval_skipped,
                total_cases=total_cases,
            )
        )
        return violations

    def _compute_decision_rules(self, events: pd.DataFrame, total_cases: int) -> List[Dict]:
        if events.empty or total_cases == 0:
            return []

        rules: List[Dict] = []

        approval_cases = events[
            events["activity"].str.contains("approval|approve", case=False, na=False)
        ]["case_id"].nunique()
        approval_rate = self._safe_round(approval_cases / total_cases * 100)
        rules.append(
            {
                "condition": f"Approval in {approval_rate}% of cases",
                "action": "Route through approval-aware flow",
                "confidence": self._safe_round(approval_rate / 100, ndigits=4),
                "source": f"{approval_cases}/{total_cases} cases contain approval activity.",
            }
        )

        exception_cases = len(self._get_exception_case_ids(events))
        exception_rate = self._safe_round(exception_cases / total_cases * 100)
        rules.append(
            {
                "condition": f"Exception in {exception_rate}% of cases",
                "action": "Invoke exception-oriented handling and escalation logic",
                "confidence": self._safe_round(exception_rate / 100, ndigits=4),
                "source": f"{exception_cases}/{total_cases} cases contain exception or disruption activity.",
            }
        )

        cases_with_gr_before_invoice = 0
        for _, group in events.groupby("case_id"):
            acts = group.sort_values("timestamp")["activity"].astype(str).str.lower().tolist()
            goods_receipt_idx = self._first_idx(acts, ["record goods receipt", "goods receipt"])
            invoice_received_idx = self._first_idx(
                acts,
                [
                    "invoice received in vim",
                    "invoice received",
                    "record invoice receipt",
                    "document item incoming invoice",
                ],
            )
            if goods_receipt_idx is not None and invoice_received_idx is not None and goods_receipt_idx < invoice_received_idx:
                cases_with_gr_before_invoice += 1

        gr_before_invoice_rate = self._safe_round(cases_with_gr_before_invoice / total_cases * 100)
        rules.append(
            {
                "condition": f"Goods receipt before invoice in {gr_before_invoice_rate}% of cases",
                "action": "Prefer invoice posting only when GR-first pattern is satisfied",
                "confidence": self._safe_round(gr_before_invoice_rate / 100, ndigits=4),
                "source": f"{cases_with_gr_before_invoice}/{total_cases} cases show goods receipt before invoice.",
            }
        )
        return rules

    def _compute_role_mappings(self, events: pd.DataFrame) -> Dict[str, str]:
        if events.empty:
            return {}

        mapping_df = (
            events.groupby(["activity", "resource_role"])
            .size()
            .reset_index(name="frequency")
            .sort_values(["activity", "frequency"], ascending=[True, False])
            .drop_duplicates(subset=["activity"])
        )
        return dict(zip(mapping_df["activity"], mapping_df["resource_role"]))

    def _compute_vendor_stats(
        self,
        events: pd.DataFrame,
        case_duration_df: pd.DataFrame,
        case_variant_map: pd.DataFrame,
    ) -> List[Dict]:
        try:
            enriched = self.celonis.get_event_log_with_vendor()
        except Exception as exc:
            logger.warning("Vendor-enriched event log unavailable: %s", str(exc))
            return []

        if enriched is None or enriched.empty:
            return []

        enriched = enriched.copy()
        enriched["case_id"] = enriched["case_id"].astype(str)
        enriched["vendor_id"] = enriched["vendor_id"].fillna("UNKNOWN").astype(str)
        enriched["activity"] = enriched["activity"].fillna("UNKNOWN").astype(str)
        enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], errors="coerce")
        enriched = enriched.sort_values(["case_id", "timestamp"], na_position="last")

        vendor_case_map = (
            enriched.groupby("case_id")["vendor_id"]
            .agg(lambda s: s.dropna().iloc[0] if not s.dropna().empty else "UNKNOWN")
            .reset_index()
        )
        case_variant_vendor = case_variant_map.merge(vendor_case_map, on="case_id", how="left")
        case_duration_vendor = case_duration_df.merge(vendor_case_map, on="case_id", how="left")

        all_exception_case_ids = self._get_exception_case_ids(events)
        exception_case_df = pd.DataFrame({"case_id": list(all_exception_case_ids), "is_exception": 1})
        vendor_exception = vendor_case_map.merge(exception_case_df, on="case_id", how="left")
        vendor_exception["is_exception"] = vendor_exception["is_exception"].fillna(0).astype(int)

        total_cases = vendor_case_map["case_id"].nunique()
        overall_exception_rate = (
            vendor_exception["is_exception"].sum() / total_cases * 100 if total_cases else 0.0
        )
        overall_avg_duration = case_duration_df["duration_days"].mean() if not case_duration_df.empty else 0.0

        rows: List[Dict] = []
        for vendor_id, group in vendor_case_map.groupby("vendor_id"):
            vendor_cases = group["case_id"].nunique()
            event_count = int(enriched[enriched["vendor_id"] == vendor_id].shape[0])
            vendor_exc_cases = int(
                vendor_exception[vendor_exception["vendor_id"] == vendor_id]["is_exception"].sum()
            )
            vendor_exception_rate = (vendor_exc_cases / vendor_cases * 100) if vendor_cases else 0.0

            duration_subset = case_duration_vendor[case_duration_vendor["vendor_id"] == vendor_id]
            avg_vendor_duration = duration_subset["duration_days"].mean() if not duration_subset.empty else 0.0

            variant_subset = case_variant_vendor[case_variant_vendor["vendor_id"] == vendor_id]
            most_common_variant = ""
            most_common_variant_case_count = 0
            if not variant_subset.empty:
                mode_df = (
                    variant_subset.groupby("variant")
                    .size()
                    .reset_index(name="case_count")
                    .sort_values("case_count", ascending=False)
                )
                most_common_variant = str(mode_df.iloc[0]["variant"])
                most_common_variant_case_count = int(mode_df.iloc[0]["case_count"])

            attrs = enriched[enriched["vendor_id"] == vendor_id][["payment_terms", "currency"]].dropna(how="all")
            payment_terms = str(attrs["payment_terms"].dropna().iloc[0]) if not attrs.empty and not attrs["payment_terms"].dropna().empty else ""
            currency = str(attrs["currency"].dropna().iloc[0]) if not attrs.empty and not attrs["currency"].dropna().empty else ""

            rows.append(
                {
                    "vendor_id": vendor_id,
                    "total_cases": int(vendor_cases),
                    "event_count": event_count,
                    "exception_case_count": vendor_exc_cases,
                    "exception_rate_pct": self._safe_round(vendor_exception_rate),
                    "avg_duration_days": self._safe_round(avg_vendor_duration),
                    "most_common_variant": most_common_variant,
                    "most_common_variant_case_count": most_common_variant_case_count,
                    "duration_vs_overall_days": self._safe_round(avg_vendor_duration - overall_avg_duration),
                    "exception_rate_vs_overall_pct": self._safe_round(vendor_exception_rate - overall_exception_rate),
                    "payment_terms": payment_terms,
                    "currency": currency,
                }
            )

        rows.sort(key=lambda x: x["total_cases"], reverse=True)
        return rows

    def _get_exception_case_ids(self, events: pd.DataFrame) -> set:
        if events.empty:
            return set()
        pattern = "|".join(
            sorted({kw for kws in self.EXCEPTION_KEYWORDS.values() for kw in kws})
        )
        exception_cases = events[
            events["activity"].str.lower().str.contains(pattern, regex=True, na=False)
        ]["case_id"].unique()
        return set(exception_cases.tolist())

    @staticmethod
    def _first_idx(activities: List[str], keywords: List[str]):
        for idx, activity in enumerate(activities):
            if any(keyword in activity for keyword in keywords):
                return idx
        return None

    @staticmethod
    def _safe_round(value, ndigits: int = 2) -> float:
        if value is None or pd.isna(value):
            return 0.0
        return round(float(value), ndigits)

    def _build_violation_records(
        self,
        rule: str,
        description: str,
        case_ids: set,
        total_cases: int,
    ) -> List[Dict]:
        if not case_ids:
            return []
        return [
            {
                "rule": rule,
                "violation_description": description,
                "violation_rate": self._safe_round(len(case_ids) / total_cases * 100),
                "affected_cases": len(case_ids),
                "total_cases": total_cases,
            }
        ]
