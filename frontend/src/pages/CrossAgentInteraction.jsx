import React, { useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  Divider,
  FormControlLabel,
  Grid,
  Stack,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import InteractionFlow from "../components/InteractionFlow";
import LoadingSpinner from "../components/LoadingSpinner";
import { executeInvoiceFlow } from "../api/client";

const SCENARIOS = {
  payment_terms_mismatch: {
    label: "Payment Terms Mismatch",
    invoice_id: "INV-5700028038",
    vendor_id: "D4",
    vendor_name: "Supplier D4",
    vendor_lifnr: "7003198830",
    invoice_amount: 363000,
    currency: "USD",
    invoice_payment_terms: "NET10",
    po_payment_terms: "NET30",
    vendor_master_terms: "NET30",
    invoice_tax_code: "",
    po_tax_code: "",
    payment_due_date: "2025-04-15",
    goods_receipt_recorded: true,
    days_in_exception: 0,
    discount_terms: "",
    actual_dpo: 0,
    potential_dpo: 0,
    company_code: "AC33",
    scenario: "Payment terms mismatch",
  },
  invoice_exception: {
    label: "Invoice Exception (80 days stuck)",
    invoice_id: "INV-5700028040",
    vendor_id: "F6",
    vendor_name: "Supplier F6",
    vendor_lifnr: "7003198928",
    invoice_amount: 499000,
    currency: "USD",
    invoice_payment_terms: "NET30",
    po_payment_terms: "NET30",
    vendor_master_terms: "",
    invoice_tax_code: "TAX_EXEMPT",
    po_tax_code: "STANDARD_RATE",
    payment_due_date: "2025-02-15",
    goods_receipt_recorded: true,
    days_in_exception: 80,
    discount_terms: "",
    actual_dpo: 0,
    potential_dpo: 0,
    company_code: "AC33",
    scenario: "Tax mismatch, stuck 80 days, payment overdue",
  },
  short_payment_terms: {
    label: "Short Payment Terms (0 days)",
    invoice_id: "INV-5700028045",
    vendor_id: "V22",
    vendor_name: "Supplier V22",
    vendor_lifnr: "7003204531",
    invoice_amount: 775000,
    currency: "USD",
    invoice_payment_terms: "IMMEDIATE",
    po_payment_terms: "NET30",
    vendor_master_terms: "NET30",
    invoice_tax_code: "",
    po_tax_code: "",
    payment_due_date: "2025-03-27",
    goods_receipt_recorded: true,
    days_in_exception: 0,
    discount_terms: "",
    actual_dpo: 0,
    potential_dpo: 0,
    company_code: "AC33",
    scenario: "0-day payment terms, likely data error",
  },
  early_payment: {
    label: "Early Payment Optimization",
    invoice_id: "INV-5700028039",
    vendor_id: "D4",
    vendor_name: "Supplier D4",
    vendor_lifnr: "7003198830",
    invoice_amount: 1420000,
    currency: "USD",
    invoice_payment_terms: "NET60",
    po_payment_terms: "NET60",
    vendor_master_terms: "NET60",
    invoice_tax_code: "",
    po_tax_code: "",
    payment_due_date: "2025-05-27",
    goods_receipt_recorded: true,
    days_in_exception: 0,
    discount_terms: "2% if paid within 10 days",
    actual_dpo: 3,
    potential_dpo: 63,
    company_code: "AC33",
    scenario: "Paying 60 days early, 2% discount available",
  },
};

const METRICS = [
  "37 Total Invoices | 22.5M $ Value",
  "19 with Exception (80 day DPO)",
  "25 Short Terms | 23 Early Payment",
  "5M $ Value at Risk",
];

const NO_PM_COMPARE = {
  without: "Invoice due in 7 days. Keep monitoring and act later based on static threshold.",
  with: "PI says this path takes ~3+ days historically. Trigger now, escalate if due-date buffer drops below processing lead time.",
};

const numberFields = new Set([
  "invoice_amount",
  "days_in_exception",
  "actual_dpo",
  "potential_dpo",
]);

export default function CrossAgentInteraction() {
  const [invoice, setInvoice] = useState(SCENARIOS.payment_terms_mismatch);
  const [selectedScenario, setSelectedScenario] = useState("payment_terms_mismatch");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [showComparison, setShowComparison] = useState(false);

  const executionTrace = useMemo(() => {
    if (!result) return null;
    if (result.execution_trace) return result.execution_trace;
    if (result.data?.execution_trace) return result.data.execution_trace;
    return null;
  }, [result]);

  const setScenario = (scenarioKey) => {
    setSelectedScenario(scenarioKey);
    setInvoice(SCENARIOS[scenarioKey]);
    setResult(null);
    setError("");
  };

  const handleFieldChange = (field, value) => {
    setInvoice((prev) => ({
      ...prev,
      [field]: numberFields.has(field) ? Number(value || 0) : value,
    }));
  };

  const handleCheckbox = (field) => (_, checked) => {
    setInvoice((prev) => ({ ...prev, [field]: checked }));
  };

  const runOrchestration = async () => {
    setLoading(true);
    setError("");
    setResult(null);
    try {
      const res = await executeInvoiceFlow(invoice);
      setResult(res);
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || "Execution failed");
    } finally {
      setLoading(false);
    }
  };

  const statusColor = (status) => {
    const normalized = String(status || "").toUpperCase();
    if (normalized.includes("BLOCK")) return "error";
    if (normalized.includes("ESCAL")) return "warning";
    return "success";
  };

  return (
    <Box className="page-container">
      <Typography variant="h4" sx={{ fontWeight: 700, mb: 1, color: "#1f2937" }}>
        Cross-Agent Interaction
      </Typography>
      <Typography variant="body2" sx={{ color: "#6b7280", mb: 2 }}>
        Full 6-agent orchestration with PI-derived timing context, process-step detection, and evidence-based handoff decisions.
      </Typography>

      <Card sx={{ mb: 3, background: "#ffffff", border: "1px solid #e5e7eb" }}>
        <CardContent>
          <Stack direction={{ xs: "column", md: "row" }} spacing={1.5}>
            {METRICS.map((m) => (
              <Chip key={m} label={m} sx={{ color: "#1e3a8a", borderColor: "#93c5fd", background: "#eff6ff" }} variant="outlined" />
            ))}
          </Stack>
        </CardContent>
      </Card>

      <Grid container spacing={3}>
        <Grid item xs={12} md={5}>
          <Card sx={{ background: "#ffffff", border: "1px solid #e5e7eb" }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 1.5, color: "#1f2937", fontWeight: 700 }}>
                1) Scenario Selector
              </Typography>
              <Stack spacing={1} sx={{ mb: 2 }}>
                {Object.entries(SCENARIOS).map(([key, config]) => (
                  <Button
                    key={key}
                    variant={selectedScenario === key ? "contained" : "outlined"}
                    onClick={() => setScenario(key)}
                    sx={{ justifyContent: "flex-start", textTransform: "none" }}
                  >
                    {config.label}
                  </Button>
                ))}
              </Stack>

              <Divider sx={{ my: 2, borderColor: "#e5e7eb" }} />
              <Typography variant="h6" sx={{ mb: 1.5, color: "#1f2937", fontWeight: 700 }}>
                2) Invoice Input Form
              </Typography>

              <Grid container spacing={1.2}>
                {[
                  ["invoice_id", "Invoice ID"],
                  ["vendor_id", "Vendor ID"],
                  ["vendor_name", "Vendor Name"],
                  ["invoice_amount", "Invoice Amount", "number"],
                  ["currency", "Currency"],
                  ["invoice_payment_terms", "Invoice Payment Terms"],
                  ["po_payment_terms", "PO Payment Terms"],
                  ["vendor_master_terms", "Vendor Master Terms"],
                  ["invoice_tax_code", "Invoice Tax Code"],
                  ["po_tax_code", "PO Tax Code"],
                  ["payment_due_date", "Payment Due Date"],
                  ["days_in_exception", "Days in Exception", "number"],
                  ["discount_terms", "Discount Terms"],
                  ["actual_dpo", "Actual DPO", "number"],
                  ["potential_dpo", "Potential DPO", "number"],
                  ["company_code", "Company Code"],
                  ["scenario", "Scenario Notes"],
                ].map(([field, label, type]) => (
                  <Grid key={field} item xs={12} sm={field === "scenario" || field === "discount_terms" ? 12 : 6}>
                    <TextField
                      fullWidth
                      size="small"
                      label={label}
                      value={invoice[field] ?? ""}
                      type={type || "text"}
                      onChange={(e) => handleFieldChange(field, e.target.value)}
                    />
                  </Grid>
                ))}

                <Grid item xs={12}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={Boolean(invoice.goods_receipt_recorded)}
                        onChange={handleCheckbox("goods_receipt_recorded")}
                      />
                    }
                    label="Goods Receipt Recorded"
                  />
                </Grid>
              </Grid>

              <Button
                variant="contained"
                fullWidth
                onClick={runOrchestration}
                disabled={loading}
                sx={{ mt: 2, textTransform: "none", fontWeight: 600 }}
              >
                {loading ? "Executing..." : "Execute Flow"}
              </Button>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={7}>
          <Typography variant="h6" sx={{ mb: 1.2, color: "#1f2937", fontWeight: 700 }}>
            3) Execution Flow
          </Typography>

          {executionTrace?.turnaround_assessment && (
            <Card sx={{ mb: 1.6, background: "#ecfeff", border: "1px solid #67e8f9" }}>
              <CardContent>
                <Typography variant="subtitle2" sx={{ color: "#0e7490", fontWeight: 700, mb: 0.6 }}>
                  PI Timing Decision (Leadership Scenario)
                </Typography>
                <Typography variant="body2" sx={{ color: "#0f172a", mb: 0.4 }}>
                  Due in {executionTrace.turnaround_assessment.days_until_due ?? 0} days | Historical processing {executionTrace.turnaround_assessment.historical_processing_days ?? executionTrace.turnaround_assessment.estimated_processing_days ?? 0} days
                </Typography>
                <Typography variant="caption" sx={{ display: "block", color: "#1d4ed8", mb: 0.4 }}>
                  Urgency: {executionTrace.turnaround_assessment.urgency || "MEDIUM"}
                </Typography>
                <Typography variant="caption" sx={{ display: "block", color: "#7c2d12" }}>
                  {executionTrace.turnaround_assessment.urgency_basis || "Decision derived from PI turnaround vs due-date buffer."}
                </Typography>
              </CardContent>
            </Card>
          )}

          {loading && <LoadingSpinner message="Running 6-agent orchestration with Celonis context..." />}
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

          {executionTrace && <InteractionFlow executionTrace={executionTrace} />}

          {executionTrace && (
            <Card sx={{ mt: 2, background: "#ffffff", border: "1px solid #e5e7eb" }}>
              <CardContent>
                <Typography variant="h6" sx={{ color: "#1f2937", mb: 1, fontWeight: 700 }}>
                  4) Final Result
                </Typography>
                <Chip
                  label={executionTrace.final_status || "UNKNOWN"}
                  color={statusColor(executionTrace.final_status)}
                  sx={{ mb: 1.2 }}
                />
                <Typography variant="subtitle2" sx={{ color: "#1976d2", mt: 1, fontWeight: 700 }}>
                  Financial Summary
                </Typography>
                <pre className="json-display">
                  {JSON.stringify(executionTrace.financial_summary || {}, null, 2)}
                </pre>
                <Typography variant="subtitle2" sx={{ color: "#1976d2", mt: 1, fontWeight: 700 }}>
                  Turnaround Assessment
                </Typography>
                <pre className="json-display">
                  {JSON.stringify(executionTrace.turnaround_assessment || {}, null, 2)}
                </pre>
                <Typography variant="subtitle2" sx={{ color: "#1976d2", mt: 1, fontWeight: 700 }}>
                  Exception Summary
                </Typography>
                <pre className="json-display">
                  {JSON.stringify(executionTrace.exception_summary || {}, null, 2)}
                </pre>
              </CardContent>
            </Card>
          )}

          <Card sx={{ mt: 2, background: "#ffffff", border: "1px solid #e5e7eb" }}>
            <CardContent>
              <Typography variant="h6" sx={{ color: "#1f2937", mb: 1, fontWeight: 700 }}>
                5) Comparison
              </Typography>
              <Button variant="outlined" onClick={() => setShowComparison((s) => !s)} sx={{ textTransform: "none", mb: 1 }}>
                What would happen WITHOUT process mining?
              </Button>
              {showComparison && (
                <Stack spacing={1}>
                  <Alert severity="warning">
                    <strong>Without:</strong> {NO_PM_COMPARE.without}
                  </Alert>
                  <Alert severity="success">
                    <strong>With:</strong> {NO_PM_COMPARE.with}
                  </Alert>
                </Stack>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
}
