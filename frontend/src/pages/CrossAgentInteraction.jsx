import React, { useMemo, useState } from "react";
import { Alert, Box, Button, Card, CardContent, FormControlLabel, Grid, Stack, Switch, TextField, Typography } from "@mui/material";
import InteractionFlow from "../components/InteractionFlow";
import LoadingSpinner from "../components/LoadingSpinner";
import { executeInvoiceFlow } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const SCENARIOS = {
  payment_terms_mismatch: { label: "Payment Terms Mismatch", invoice_id: "INV-5700028038", vendor_id: "D4", vendor_name: "Supplier D4", vendor_lifnr: "7003198830", invoice_amount: 363000, currency: "USD", invoice_payment_terms: "NET10", po_payment_terms: "NET30", vendor_master_terms: "NET30", invoice_tax_code: "", po_tax_code: "", payment_due_date: "2025-04-15", goods_receipt_recorded: true, days_in_exception: 0, discount_terms: "", actual_dpo: 0, potential_dpo: 0, company_code: "AC33", scenario: "Payment terms mismatch" },
  invoice_exception: { label: "Invoice Exception (80 days stuck)", invoice_id: "INV-5700028040", vendor_id: "F6", vendor_name: "Supplier F6", vendor_lifnr: "7003198928", invoice_amount: 499000, currency: "USD", invoice_payment_terms: "NET30", po_payment_terms: "NET30", vendor_master_terms: "", invoice_tax_code: "TAX_EXEMPT", po_tax_code: "STANDARD_RATE", payment_due_date: "2025-02-15", goods_receipt_recorded: true, days_in_exception: 80, discount_terms: "", actual_dpo: 0, potential_dpo: 0, company_code: "AC33", scenario: "Tax mismatch, stuck 80 days" },
  short_payment_terms: { label: "Short Payment Terms (0 days)", invoice_id: "INV-5700028045", vendor_id: "V22", vendor_name: "Supplier V22", vendor_lifnr: "7003204531", invoice_amount: 775000, currency: "USD", invoice_payment_terms: "IMMEDIATE", po_payment_terms: "NET30", vendor_master_terms: "NET30", invoice_tax_code: "", po_tax_code: "", payment_due_date: "2025-03-27", goods_receipt_recorded: true, days_in_exception: 0, discount_terms: "", actual_dpo: 0, potential_dpo: 0, company_code: "AC33", scenario: "0-day payment terms" },
  early_payment: { label: "Early Payment Optimization", invoice_id: "INV-5700028039", vendor_id: "D4", vendor_name: "Supplier D4", vendor_lifnr: "7003198830", invoice_amount: 1420000, currency: "USD", invoice_payment_terms: "NET60", po_payment_terms: "NET60", vendor_master_terms: "NET60", invoice_tax_code: "", po_tax_code: "", payment_due_date: "2025-05-27", goods_receipt_recorded: true, days_in_exception: 0, discount_terms: "2% if paid within 10 days", actual_dpo: 3, potential_dpo: 63, company_code: "AC33", scenario: "Early payment discount available" },
};

const METRICS = ["37 Total Invoices | 22.5M $", "19 with Exception (80 day DPO)", "25 Short Terms | 23 Early Payment", "5M $ Value at Risk"];
const NO_PM = { without: "Invoice due in 7 days. Keep monitoring and act later based on static threshold.", with: "PI says this path takes ~3+ days historically. Trigger now, escalate if due-date buffer drops below processing lead time." };
const numberFields = new Set(["invoice_amount", "days_in_exception", "actual_dpo", "potential_dpo"]);

const SCENARIO_STYLES = {
  payment_terms_mismatch: { color: "#A05A10", bg: "#FEF3DC", border: "#F0C870" },
  invoice_exception: { color: "#B03030", bg: "#FAEAEA", border: "#E0A0A0" },
  short_payment_terms: { color: "#1E4E8C", bg: "#EBF2FC", border: "#90B8E8" },
  early_payment: { color: "#1A6B5E", bg: "#DCF0EB", border: "#8FCFC5" },
};

const FORM_FIELDS = [["invoice_id", "Invoice ID"], ["vendor_id", "Vendor ID"], ["vendor_name", "Vendor Name"], ["invoice_amount", "Invoice Amount", "number"], ["currency", "Currency"], ["invoice_payment_terms", "Invoice Payment Terms"], ["po_payment_terms", "PO Payment Terms"], ["vendor_master_terms", "Vendor Master Terms"], ["invoice_tax_code", "Invoice Tax Code"], ["po_tax_code", "PO Tax Code"], ["payment_due_date", "Payment Due Date"], ["days_in_exception", "Days in Exception", "number"], ["discount_terms", "Discount Terms"], ["actual_dpo", "Actual DPO", "number"], ["potential_dpo", "Potential DPO", "number"], ["company_code", "Company Code"], ["scenario", "Scenario Notes"]];

export default function CrossAgentInteraction() {
  const [invoice, setInvoice] = useState(SCENARIOS.payment_terms_mismatch);
  const [selectedScenario, setSelectedScenario] = useState("payment_terms_mismatch");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [showComparison, setShowComparison] = useState(false);

  const executionTrace = useMemo(() => result?.execution_trace || result?.data?.execution_trace || null, [result]);

  const setScenario = (key) => { setSelectedScenario(key); setInvoice(SCENARIOS[key]); setResult(null); setError(""); };
  const handleField = (field, value) => setInvoice(p => ({ ...p, [field]: numberFields.has(field) ? Number(value || 0) : value }));

  const runOrchestration = async () => {
    setLoading(true); setError(""); setResult(null);
    try { setResult(await executeInvoiceFlow(invoice)); }
    catch (e) { setError(e?.response?.data?.detail || e.message || "Execution failed"); }
    finally { setLoading(false); }
  };

  return (
    <div className="page-container">
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.5 }}>
          Cross-Agent Interaction
        </Typography>
        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
          Full 6-agent orchestration with PI-derived timing context, process-step detection, and evidence-based handoff decisions.
        </Typography>
      </Box>

      {/* Metrics Strip */}
      <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap", mb: 3 }}>
        {METRICS.map(m => (
          <Box key={m} sx={{ background: "#EBF2FC", border: "1px solid #90B8E8", px: 1.5, py: 0.5, borderRadius: "99px" }}>
            <Typography sx={{ fontSize: "0.72rem", fontWeight: 600, color: "#1E4E8C", fontFamily: G }}>{m}</Typography>
          </Box>
        ))}
      </Box>

      <Grid container spacing={2.5}>
        {/* LEFT */}
        <Grid item xs={12} md={5}>
          <Card>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>
                <span style={{ color: "#9C9690", fontSize: "0.69rem", fontFamily: G, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", display: "block", marginBottom: "6px" }}>01</span>
                Scenario Selector
              </Typography>
              <Stack spacing={0.8} sx={{ mb: 2.5 }}>
                {Object.entries(SCENARIOS).map(([key, cfg]) => {
                  const active = selectedScenario === key;
                  const st = SCENARIO_STYLES[key];
                  return (
                    <Box key={key} onClick={() => setScenario(key)} sx={{ p: 1.4, borderRadius: "10px", border: active ? `2px solid ${st.color}` : "1px solid #E8E3DA", background: active ? st.bg : "#FDFCFA", cursor: "pointer", transition: "all 0.15s", "&:hover": { background: active ? st.bg : "#F5F2EC", borderColor: active ? st.color : "#C4BDB0" } }}>
                      <Typography sx={{ fontSize: "0.82rem", fontWeight: active ? 600 : 400, color: active ? st.color : "#5C5650", fontFamily: G }}>{cfg.label}</Typography>
                    </Box>
                  );
                })}
              </Stack>

              <Box sx={{ height: "1px", background: "#E8E3DA", mb: 2 }} />
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>
                <span style={{ color: "#9C9690", fontSize: "0.69rem", fontFamily: G, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", display: "block", marginBottom: "6px" }}>02</span>
                Invoice Input Form
              </Typography>
              <Grid container spacing={1.2}>
                {FORM_FIELDS.map(([field, label, type]) => (
                  <Grid key={field} item xs={12} sm={field === "scenario" || field === "discount_terms" ? 12 : 6}>
                    <TextField fullWidth size="small" label={label} value={invoice[field] ?? ""} type={type || "text"} onChange={e => handleField(field, e.target.value)} />
                  </Grid>
                ))}
                <Grid item xs={12}>
                  <FormControlLabel
                    control={<Switch checked={Boolean(invoice.goods_receipt_recorded)} onChange={(_, v) => setInvoice(p => ({ ...p, goods_receipt_recorded: v }))} size="small" />}
                    label={<Typography sx={{ fontSize: "0.82rem", fontFamily: G }}>Goods Receipt Recorded</Typography>}
                  />
                </Grid>
              </Grid>
              <Button variant="contained" fullWidth onClick={runOrchestration} disabled={loading} sx={{ mt: 2 }}>
                {loading ? "Executing…" : "Execute Flow"}
              </Button>
            </CardContent>
          </Card>
        </Grid>

        {/* RIGHT */}
        <Grid item xs={12} md={7}>
          <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.5 }}>
            <span style={{ color: "#9C9690", fontSize: "0.69rem", fontFamily: G, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", display: "block", marginBottom: "6px" }}>03</span>
            Execution Flow
          </Typography>

          {executionTrace?.turnaround_assessment && (
            <Card sx={{ mb: 2, background: "#EBF2FC !important", border: "1px solid #90B8E8 !important", borderLeft: "3px solid #1E4E8C !important" }}>
              <CardContent sx={{ py: "14px !important" }}>
                <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1E4E8C", fontFamily: G, mb: 0.8 }}>PI Timing Decision</Typography>
                <Box sx={{ display: "flex", gap: 1, mb: 0.8, flexWrap: "wrap" }}>
                  {[`Due in ${executionTrace.turnaround_assessment.days_until_due ?? 0}d`, `Historical path ${executionTrace.turnaround_assessment.historical_processing_days ?? executionTrace.turnaround_assessment.estimated_processing_days ?? 0}d`, `Urgency: ${executionTrace.turnaround_assessment.urgency || "MEDIUM"}`].map(t => (
                    <Box key={t} sx={{ background: "#D0E4F8", px: 1.2, py: 0.2, borderRadius: "99px" }}>
                      <Typography sx={{ fontSize: "0.7rem", fontWeight: 600, color: "#1E4E8C", fontFamily: G }}>{t}</Typography>
                    </Box>
                  ))}
                </Box>
                <Typography sx={{ fontSize: "0.78rem", color: "#2E5090", fontFamily: G }}>{executionTrace.turnaround_assessment.urgency_basis || "Decision derived from PI turnaround vs due-date buffer."}</Typography>
              </CardContent>
            </Card>
          )}

          {loading && <LoadingSpinner message="Running 6-agent orchestration with Celonis context..." />}
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          {executionTrace && <InteractionFlow executionTrace={executionTrace} />}

          {executionTrace && (
            <Card sx={{ mt: 2 }}>
              <CardContent>
                <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.5 }}>
                  <span style={{ color: "#9C9690", fontSize: "0.69rem", fontFamily: G, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", display: "block", marginBottom: "6px" }}>04</span>
                  Final Result
                </Typography>
                {(() => {
                  const v = String(executionTrace.final_status || "").toUpperCase();
                  const s = v.includes("BLOCK") ? { bg: "#FAEAEA", c: "#B03030", b: "#E0A0A0" } : v.includes("ESCAL") ? { bg: "#FEF3DC", c: "#A05A10", b: "#F0C870" } : { bg: "#DCF0EB", c: "#1A6B5E", b: "#8FCFC5" };
                  return <Box sx={{ display: "inline-block", background: s.bg, color: s.c, border: `1px solid ${s.b}`, px: 1.5, py: 0.4, borderRadius: "99px", mb: 1.5 }}><Typography sx={{ fontSize: "0.75rem", fontWeight: 700, fontFamily: G }}>{executionTrace.final_status || "UNKNOWN"}</Typography></Box>;
                })()}
                {[["Financial Summary", executionTrace.financial_summary], ["Turnaround Assessment", executionTrace.turnaround_assessment], ["Exception Summary", executionTrace.exception_summary]].map(([lbl, data]) => (
                  <Box key={lbl} sx={{ mb: 1.5 }}>
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.5 }}>{lbl}</Typography>
                    <pre className="json-display">{JSON.stringify(data || {}, null, 2)}</pre>
                  </Box>
                ))}
              </CardContent>
            </Card>
          )}

          <Card sx={{ mt: 2 }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>
                <span style={{ color: "#9C9690", fontSize: "0.69rem", fontFamily: G, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", display: "block", marginBottom: "6px" }}>05</span>
                Process Mining Value
              </Typography>
              <Button variant="outlined" size="small" onClick={() => setShowComparison(s => !s)}>
                {showComparison ? "Collapse" : "What would happen WITHOUT process mining?"}
              </Button>
              {showComparison && (
                <Stack spacing={1.5} sx={{ mt: 1.5 }}>
                  {[["Without Process Mining", "#B03030", "#FAEAEA", "#E0A0A0", NO_PM.without], ["With Process Mining", "#1A6B5E", "#DCF0EB", "#8FCFC5", NO_PM.with]].map(([title, c, bg, b, text]) => (
                    <Box key={title} sx={{ p: 1.5, background: bg, border: `1px solid ${b}`, borderLeft: `3px solid ${c}`, borderRadius: "0 10px 10px 0" }}>
                      <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: c, fontFamily: G, mb: 0.5 }}>{title}</Typography>
                      <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G }}>{text}</Typography>
                    </Box>
                  ))}
                </Stack>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </div>
  );
}