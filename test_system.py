#!/usr/bin/env python3
"""
End-to-end system test runner for the Celonis + AI agent stack.

Covers:
1. Celonis connection
2. Event log extraction
3. Vendor stats
4. Process insights
5. Vendor paths (D4)
6. Agent recommendation
7-10. Full 6-agent orchestration scenarios A-D
11. Vendor intelligence agent
12. Prompt writer agent
13. Automation policy agent (per exception type)

Usage:
  python test_system.py

Optional environment variables:
  BACKEND_URL=http://localhost:8000
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000").rstrip("/")
API_BASE = f"{BACKEND_URL}/api"
TIMEOUT_SECONDS = 120


SCENARIO_A = {
    "invoice_id": "INV-5700028038",
    "vendor_id": "D4",
    "vendor_name": "Supplier D4",
    "vendor_lifnr": "7003198830",
    "invoice_amount": 363000,
    "currency": "USD",
    "invoice_payment_terms": "NET10",
    "po_payment_terms": "NET30",
    "vendor_master_terms": "NET30",
    "payment_due_date": "2025-04-15",
    "goods_receipt_recorded": True,
    "company_code": "AC33",
    "scenario": "Payment terms mismatch",
}

SCENARIO_B = {
    "invoice_id": "INV-5700028040",
    "vendor_id": "F6",
    "vendor_name": "Supplier F6",
    "vendor_lifnr": "7003198928",
    "invoice_amount": 499000,
    "currency": "USD",
    "invoice_tax_code": "TAX_EXEMPT",
    "po_tax_code": "STANDARD_RATE",
    "invoice_payment_terms": "NET30",
    "po_payment_terms": "NET30",
    "payment_due_date": "2025-02-15",
    "goods_receipt_recorded": True,
    "days_in_exception": 80,
    "company_code": "AC33",
    "scenario": "Tax mismatch, stuck 80 days, payment overdue",
}

SCENARIO_C = {
    "invoice_id": "INV-5700028045",
    "vendor_id": "V22",
    "vendor_name": "Supplier V22",
    "vendor_lifnr": "7003204531",
    "invoice_amount": 775000,
    "currency": "USD",
    "invoice_payment_terms": "IMMEDIATE",
    "po_payment_terms": "NET30",
    "vendor_master_terms": "NET30",
    "payment_due_date": "2025-03-27",
    "goods_receipt_recorded": True,
    "company_code": "AC33",
    "scenario": "0-day payment terms, likely data error",
}

SCENARIO_D = {
    "invoice_id": "INV-5700028039",
    "vendor_id": "D4",
    "vendor_name": "Supplier D4",
    "vendor_lifnr": "7003198830",
    "invoice_amount": 1420000,
    "currency": "USD",
    "invoice_payment_terms": "NET60",
    "po_payment_terms": "NET60",
    "actual_dpo": 3,
    "potential_dpo": 63,
    "payment_due_date": "2025-05-27",
    "goods_receipt_recorded": True,
    "discount_terms": "2% if paid within 10 days",
    "company_code": "AC33",
    "scenario": "Paying 60 days early, 2% discount available",
}


@dataclass
class TestResult:
    name: str
    passed: bool
    details: str = ""


class TestRunner:
    def __init__(self) -> None:
        self.results: List[TestResult] = []

    def run(self, name: str, fn) -> None:
        self._print_header(name)
        try:
            passed, details = fn()
        except Exception as exc:  # noqa: BLE001
            passed = False
            details = f"Unhandled exception: {exc}\n{traceback.format_exc(limit=2)}"
        self.results.append(TestResult(name=name, passed=passed, details=details))
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {name}")
        if details:
            print(f"  {details}")
        print("")

    def summary(self) -> None:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        print("=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        print(f"Total: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        if failed:
            print("\nFailed tests:")
            for r in self.results:
                if not r.passed:
                    print(f"- {r.name}")
        print("")

    @staticmethod
    def _print_header(name: str) -> None:
        print("\n" + "=" * 80)
        print(f"TEST: {name}")
        print("=" * 80)


def _safe_json_loads(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return {"raw": raw}


def http_get(path: str, params: Optional[Dict[str, Any]] = None) -> Tuple[int, Any]:
    url = f"{BACKEND_URL}{path}" if path.startswith("/") else f"{API_BASE}/{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    req = Request(url=url, method="GET")
    try:
        with urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, _safe_json_loads(body)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, _safe_json_loads(body)
    except URLError as e:
        return 0, {"error": f"Connection error: {e}"}


def http_post(path: str, payload: Dict[str, Any]) -> Tuple[int, Any]:
    url = f"{BACKEND_URL}{path}" if path.startswith("/") else f"{API_BASE}/{path}"
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url=url,
        method="POST",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, _safe_json_loads(raw)
    except HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        return e.code, _safe_json_loads(raw)
    except URLError as e:
        return 0, {"error": f"Connection error: {e}"}


def pick_data(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    if "data" in payload:
        return payload["data"]
    return payload


def short_json(obj: Any, limit: int = 380) -> str:
    text = json.dumps(obj, default=str)
    if len(text) <= limit:
        return text
    return text[:limit] + "... [truncated]"


def extract_execution_trace(response: Dict[str, Any]) -> Dict[str, Any]:
    data = pick_data(response)
    if isinstance(data, dict) and "execution_trace" in data:
        return data["execution_trace"] or {}
    if isinstance(response, dict) and "execution_trace" in response:
        return response["execution_trace"] or {}
    return {}


def expect_success(status: int, payload: Any) -> bool:
    if status != 200:
        return False
    if isinstance(payload, dict) and payload.get("success") is False:
        return False
    return True


def test_health() -> Tuple[bool, str]:
    status, payload = http_get("/health")
    print(f"Input: GET /health")
    print(f"Output: HTTP {status}, body={short_json(payload)}")
    return (status == 200, f"HTTP {status}")


def test_celonis_connection() -> Tuple[bool, str]:
    status, payload = http_get("/api/process/connection")
    print("Input: GET /api/process/connection")
    print(f"Output: HTTP {status}, body={short_json(payload)}")
    ok = expect_success(status, payload)
    if ok and isinstance(payload, dict):
        connected = bool(pick_data(payload).get("connected", True))
        ok = connected
    return ok, "Connected to Celonis" if ok else "Connection failed"


def test_event_log() -> Tuple[bool, str]:
    status, payload = http_get("/api/process/test-event-log")
    print("Input: GET /api/process/test-event-log")
    print(f"Output: HTTP {status}, body={short_json(payload)}")
    if not expect_success(status, payload):
        return False, f"HTTP {status}"
    row_count = int(payload.get("row_count", 0))
    cols = payload.get("columns", [])
    required = {"case_id", "activity", "timestamp", "document_number"}
    ok = row_count > 0 and required.issubset(set(cols))
    return ok, f"Rows={row_count}, columns={len(cols)}"


def test_vendor_stats() -> Tuple[bool, str]:
    status, payload = http_get("/api/process/vendor-stats")
    print("Input: GET /api/process/vendor-stats")
    print(f"Output: HTTP {status}, body={short_json(payload)}")
    if not expect_success(status, payload):
        return False, f"HTTP {status}"
    data = pick_data(payload)
    rows = data if isinstance(data, list) else data.get("vendors", []) if isinstance(data, dict) else []
    ok = isinstance(rows, list)
    return ok, f"Vendor rows={len(rows)}"


def test_process_insights() -> Tuple[bool, str]:
    status, payload = http_get("/api/process/insights")
    print("Input: GET /api/process/insights")
    print(f"Output: HTTP {status}, body={short_json(payload)}")
    if not expect_success(status, payload):
        return False, f"HTTP {status}"
    data = pick_data(payload)
    ok = isinstance(data, dict) and "exception_patterns" in data and "variants" in data
    return ok, f"Keys={list(data.keys())[:8] if isinstance(data, dict) else 'N/A'}"


def test_vendor_paths_d4() -> Tuple[bool, str]:
    status, payload = http_get("/api/process/vendor/D4/paths")
    print("Input: GET /api/process/vendor/D4/paths")
    print(f"Output: HTTP {status}, body={short_json(payload)}")
    if not expect_success(status, payload):
        return False, f"HTTP {status}"
    data = pick_data(payload)
    has_happy = isinstance(data, dict) and ("happy_paths" in data or "happy" in data)
    has_exc = isinstance(data, dict) and ("exception_paths" in data or "exceptions" in data)
    return has_happy and has_exc, "Happy/exception paths present"


def test_agent_recommendation() -> Tuple[bool, str]:
    status, payload = http_get("/api/process/agents")
    print("Input: GET /api/process/agents")
    print(f"Output: HTTP {status}, body={short_json(payload)}")
    if not expect_success(status, payload):
        return False, f"HTTP {status}"
    data = pick_data(payload)
    has_agents = isinstance(data, dict) and (
        "recommended_agents" in data or "agents" in data
    )
    return has_agents, "Agent recommendation payload received"


def test_orchestration_scenario(
    scenario_name: str, payload: Dict[str, Any], expected_final_status: str
) -> Tuple[bool, str]:
    status, response = http_post("/api/agents/execute-invoice", payload)
    print(f"Input ({scenario_name}): {short_json(payload)}")
    print(f"Output: HTTP {status}, body={short_json(response)}")
    if not expect_success(status, response):
        return False, f"HTTP {status}"

    trace = extract_execution_trace(response)
    final_status = str(trace.get("final_status", "UNKNOWN"))
    match = final_status == expected_final_status
    detail = f"final_status={final_status}, expected={expected_final_status}"
    return match, detail


def test_vendor_intelligence_d4() -> Tuple[bool, str]:
    payload = {
        "vendor_id": "D4",
        "vendor_name": "Supplier D4",
        "vendor_lifnr": "7003198830",
        "company_code": "AC33",
    }
    status, response = http_post("/api/agents/vendor-intelligence", payload)
    print(f"Input: {short_json(payload)}")
    print(f"Output: HTTP {status}, body={short_json(response)}")
    if not expect_success(status, response):
        return False, f"HTTP {status}"
    data = pick_data(response)
    vendor_id = str(data.get("vendor_id", ""))
    ok = vendor_id in {"D4", "d4"} or "vendor_analysis" in data
    return ok, f"vendor_id={vendor_id or 'N/A'}"


def test_prompt_writer() -> Tuple[bool, str]:
    payload = {
        "target_agent": "Exception Agent",
        "scenario": "Generate prompts for all 4 exception categories",
    }
    status, response = http_post("/api/agents/write-prompts", payload)
    print(f"Input: {short_json(payload)}")
    print(f"Output: HTTP {status}, body={short_json(response)}")
    if not expect_success(status, response):
        return False, f"HTTP {status}"
    data = pick_data(response)
    gp = data.get("generated_prompts", {})
    ok = isinstance(gp, dict) and len(gp) > 0
    return ok, f"generated_prompt_keys={list(gp.keys())[:6]}"


def test_automation_policy_by_exception_type() -> Tuple[bool, str]:
    payloads = [
        {"case_type": "payment_terms_mismatch", "vendor_id": "D4", "invoice_amount": 363000},
        {"case_type": "invoice_exception", "vendor_id": "F6", "invoice_amount": 499000, "days_in_exception": 80},
        {"case_type": "short_payment_terms", "vendor_id": "V22", "invoice_amount": 775000},
        {"case_type": "early_payment", "vendor_id": "D4", "invoice_amount": 1420000, "actual_dpo": 3, "potential_dpo": 63},
    ]
    pass_count = 0
    details = []
    for p in payloads:
        status, response = http_post("/api/agents/automation-policy", p)
        print(f"Input: {short_json(p)}")
        print(f"Output: HTTP {status}, body={short_json(response)}")
        if not expect_success(status, response):
            details.append(f"{p['case_type']}: HTTP {status}")
            continue
        data = pick_data(response)
        decision = data.get("automation_decision")
        if decision:
            pass_count += 1
            details.append(f"{p['case_type']}: {decision}")
        else:
            details.append(f"{p['case_type']}: missing automation_decision")

    ok = pass_count == len(payloads)
    return ok, "; ".join(details)


def main() -> int:
    print("=" * 80)
    print("FULL SYSTEM TEST")
    print("=" * 80)
    print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
    print(f"Backend URL: {BACKEND_URL}")
    print("")

    runner = TestRunner()

    # Optional health precheck
    runner.run("Health endpoint", test_health)

    # Core backend checks
    runner.run("1) Celonis connection", test_celonis_connection)
    runner.run("2) Event log extraction from t_o_custom_VimHeader", test_event_log)
    runner.run("3) Vendor stats from t_o_custom_PurchasingDocumentHeader", test_vendor_stats)
    runner.run("4) Process insights computation", test_process_insights)
    runner.run("5) Vendor paths for D4 (happy vs exception)", test_vendor_paths_d4)
    runner.run("6) Agent recommendation from GPT-4o", test_agent_recommendation)

    # Orchestration scenarios
    runner.run(
        "7) SCENARIO A - Payment Terms Mismatch => POSTED",
        lambda: test_orchestration_scenario("Scenario A", SCENARIO_A, "POSTED"),
    )
    runner.run(
        "8) SCENARIO B - Invoice Exception 80 days => ESCALATED_TO_HUMAN",
        lambda: test_orchestration_scenario("Scenario B", SCENARIO_B, "ESCALATED_TO_HUMAN"),
    )
    runner.run(
        "9) SCENARIO C - Short Payment Terms => POSTED",
        lambda: test_orchestration_scenario("Scenario C", SCENARIO_C, "POSTED"),
    )
    runner.run(
        "10) SCENARIO D - Early Payment Optimization => APPROVED_EARLY_PAYMENT",
        lambda: test_orchestration_scenario("Scenario D", SCENARIO_D, "APPROVED_EARLY_PAYMENT"),
    )

    # Agent-level checks
    runner.run("11) Vendor Intelligence for D4", test_vendor_intelligence_d4)
    runner.run("12) Prompt Writer for Exception Agent", test_prompt_writer)
    runner.run("13) Automation Policy for each exception type", test_automation_policy_by_exception_type)

    runner.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
