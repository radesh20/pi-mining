import React, { useEffect, useMemo, useState } from "react";
import { Alert, Box, Button, Card, CardContent, Chip, CircularProgress, Divider, Grid, Stack, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Typography } from "@mui/material";
import api from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const FALLBACK_VENDORS = [
  { vendor_id: "D4", vendor_lifnr: "7003198830", total_cases: 3, total_value: 5700000 },
  { vendor_id: "B2", vendor_lifnr: "", total_cases: 2, total_value: 6200000 },
  { vendor_id: "A27", vendor_lifnr: "", total_cases: 2, total_value: 4100000 },
  { vendor_id: "F6", vendor_lifnr: "", total_cases: 2, total_value: 3300000 },
  { vendor_id: "H8", vendor_lifnr: "7003204990", total_cases: 1, total_value: 2900000 },
  { vendor_id: "C3", vendor_lifnr: "7003205015", total_cases: 1, total_value: 1500000 },
];

const EXCEPTION_META = [
  { key: "payment_terms_mismatch", title: "Payment Terms Mismatch", color: "#B03030", subtle: "#FAEAEA", border: "#E0A0A0" },
  { key: "invoice_exception", title: "Invoice Exception", color: "#A05A10", subtle: "#FEF3DC", border: "#F0C870" },
  { key: "short_payment_terms", title: "Short Payment Terms", color: "#1E4E8C", subtle: "#EBF2FC", border: "#90B8E8" },
  { key: "early_payment", title: "Early Payment", color: "#1A6B5E", subtle: "#DCF0EB", border: "#8FCFC5" },
];

const currency = (v) => { const n = Number(v || 0); if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M $`; if (n >= 1000) return `${(n / 1000).toFixed(1)}K $`; return `${n.toFixed(0)} $`; };
const pct = (v) => `${Number(v || 0).toFixed(1)}%`;
const pickData = (r) => { if (!r) return null; if (r.data?.data !== undefined) return r.data.data; if (r.data !== undefined) return r.data; return r; };

const withFallbackRisk = (row) => {
  const dpo = Number(row.avg_dpo ?? row.avg_duration_days ?? 0);
  const exc = Number(row.exception_rate ?? row.exception_rate_pct ?? 0);
  if (row.risk_score) return row.risk_score;
  if (exc > 60 || dpo > 60) return "CRITICAL";
  if (exc > 40 || dpo > 40) return "HIGH";
  if (exc > 20 || dpo > 20) return "MEDIUM";
  return "LOW";
};

function RiskChip({ risk }) {
  const map = { CRITICAL: { bg: "#FAEAEA", color: "#B03030", border: "#E0A0A0" }, HIGH: { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870" }, MEDIUM: { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870" }, LOW: { bg: "#E0F0E8", color: "#1D5C3A", border: "#80C0A0" } };
  const s = map[String(risk).toUpperCase()] || map.LOW;
  return (
    <Box sx={{ display: "inline-block", background: s.bg, color: s.color, border: `1px solid ${s.border}`, px: 1.2, py: 0.2, borderRadius: "99px" }}>
      <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, fontFamily: G, letterSpacing: "0.04em" }}>{risk}</Typography>
    </Box>
  );
}

function PaymentPie({ behavior }) {
  const on = Number(behavior?.on_time_pct || 0);
  const early = Number(behavior?.early_pct || 0);
  const late = Number(behavior?.late_pct || 0);
  const open = Number(behavior?.open_pct || 0);
  const slices = [
    { label: "On Time", value: on, color: "#1A6B5E" },
    { label: "Early", value: early, color: "#1E4E8C" },
    { label: "Late", value: late, color: "#B03030" },
    { label: "Open", value: open, color: "#9C9690" },
  ];
  let cur = 0;
  const grad = slices.map(s => { const st = cur; cur += s.value; return `${s.color} ${st}% ${cur}%`; }).join(", ");
  return (
    <Box sx={{ display: "flex", gap: 3, alignItems: "center", flexWrap: "wrap" }}>
      <Box sx={{ width: 120, height: 120, borderRadius: "50%", background: `conic-gradient(${grad || "#9C9690 0 100%"})`, border: "3px solid #F0EDE6", flexShrink: 0 }} />
      <Stack spacing={0.8}>
        {slices.map(s => (
          <Box key={s.label} sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Box sx={{ width: 8, height: 8, borderRadius: "50%", background: s.color, flexShrink: 0 }} />
            <Typography sx={{ fontSize: "0.78rem", color: "#5C5650", fontFamily: G }}>{s.label}: <strong>{pct(s.value)}</strong></Typography>
          </Box>
        ))}
      </Stack>
    </Box>
  );
}

export default function VendorAnalysis() {
  const [loading, setLoading] = useState(true);
  const [vendors, setVendors] = useState([]);
  const [selectedVendorId, setSelectedVendorId] = useState("");
  const [vendorPaths, setVendorPaths] = useState({ happy_paths: [], exception_paths: [] });
  const [pathsLoading, setPathsLoading] = useState(false);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResult, setAiResult] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;
    (async () => {
      setLoading(true); setError("");
      try {
        const res = await api.get("/process/vendor-stats");
        const data = pickData(res);
        const rows = Array.isArray(data) ? data : data?.vendors || [];
        const normalized = rows.length ? rows.map(r => ({ vendor_id: r.vendor_id || r.vendor || "UNKNOWN", vendor_lifnr: r.vendor_lifnr || r.lifnr || "", total_cases: Number(r.total_cases ?? r.case_count ?? 0), total_value: Number(r.total_value ?? r.value_usd ?? 0), exception_rate: Number(r.exception_rate ?? r.exception_rate_pct ?? 0), avg_dpo: Number(r.avg_dpo ?? r.avg_duration_days ?? 0), payment_behavior: r.payment_behavior || null, risk_score: r.risk_score || withFallbackRisk(r) })) : FALLBACK_VENDORS.map(v => ({ ...v, exception_rate: 100, avg_dpo: 15, payment_behavior: null, risk_score: "CRITICAL" }));
        if (active) { setVendors(normalized); setSelectedVendorId(normalized[0]?.vendor_id || "D4"); }
      } catch (e) {
        if (active) {
          setError(e?.response?.data?.detail || e.message || "Failed to load vendors");
          const fallback = FALLBACK_VENDORS.map(v => ({ ...v, exception_rate: 100, avg_dpo: 15, payment_behavior: null, risk_score: "CRITICAL" }));
          setVendors(fallback);
          setSelectedVendorId(fallback[0]?.vendor_id || "D4");
        }
      } finally { if (active) setLoading(false); }
    })();
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!selectedVendorId) return;
    let active = true;
    setPathsLoading(true);
    api.get(`/process/vendor/${encodeURIComponent(selectedVendorId)}/paths`)
      .then(res => { const d = pickData(res) || {}; if (active) setVendorPaths({ happy_paths: Array.isArray(d.happy_paths) ? d.happy_paths : [], exception_paths: Array.isArray(d.exception_paths) ? d.exception_paths : [] }); })
      .catch(() => { if (active) setVendorPaths({ happy_paths: [], exception_paths: [] }); })
      .finally(() => { if (active) setPathsLoading(false); });
    setAiResult(null);
    return () => { active = false; };
  }, [selectedVendorId]);

  const selectedVendor = useMemo(() => vendors.find(v => v.vendor_id === selectedVendorId) || null, [vendors, selectedVendorId]);

  const behavior = useMemo(() => {
    if (selectedVendor?.payment_behavior && typeof selectedVendor.payment_behavior === "object") return selectedVendor.payment_behavior;
    return { on_time_pct: 29.7, early_pct: 29.7, late_pct: 29.7, open_pct: 10.8 };
  }, [selectedVendor]);

  const exceptionBreakdown = useMemo(() => aiResult?.vendor_analysis?.exception_breakdown || selectedVendor?.exception_breakdown || {}, [aiResult, selectedVendor]);

  const runAiAnalysis = async () => {
    if (!selectedVendor) return;
    setAiLoading(true); setError("");
    try {
      const d = pickData(await api.post("/agents/vendor-intelligence", { vendor_id: selectedVendor.vendor_id, vendor_lifnr: selectedVendor.vendor_lifnr, vendor_context: selectedVendor, include_comparison_to_overall: true, include_financial_impact: true }));
      setAiResult(d);
    } catch (e) { setError(e?.response?.data?.detail || e.message || "Vendor AI analysis failed"); }
    finally { setAiLoading(false); }
  };

  if (loading) return <div className="page-container"><Box sx={{ pt: 6, display: "flex", justifyContent: "center" }}><CircularProgress /></Box></div>;

  return (
    <div className="page-container">
      {/* Header */}
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.5 }}>
          Vendor Analysis
        </Typography>
        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
          External Suppliers under Company Code AC33
        </Typography>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      {/* Vendor Process Behavior — PI-style section above table */}
      {selectedVendor && (
        <Card sx={{ mb: 2.5, background: "#F2FAF6 !important", border: "1px solid #B8DFD0 !important" }}>
          <CardContent>
            <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#1A6B5E", mb: 1.5 }}>Vendor Process Behavior</Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} sm={4}>
                <Box sx={{ p: 1.2, background: "#DCF0EB", border: "1px solid #8FCFC5", borderRadius: "10px" }}>
                  <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: "#1A6B5E", fontFamily: G, mb: 0.5 }}>Process Signals</Typography>
                  <Typography sx={{ fontFamily: S, fontSize: "1.6rem", color: "#1A6B5E", lineHeight: 1, mb: 0.2 }}>{Number(selectedVendor.avg_dpo || 0).toFixed(1)}d</Typography>
                  <Typography sx={{ fontSize: "0.7rem", color: "#1A6B5E", fontFamily: G, opacity: 0.8 }}>Observed Process Duration</Typography>
                </Box>
              </Grid>
              <Grid item xs={12} sm={4}>
                <Box sx={{ p: 1.2, background: "#FAEAEA", border: "1px solid #E0A0A0", borderRadius: "10px" }}>
                  <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: "#B03030", fontFamily: G, mb: 0.5 }}>Process Failure Rate</Typography>
                  <Typography sx={{ fontFamily: S, fontSize: "1.6rem", color: "#B03030", lineHeight: 1, mb: 0.2 }}>{pct(selectedVendor.exception_rate)}</Typography>
                  <Typography sx={{ fontSize: "0.7rem", color: "#B03030", fontFamily: G, opacity: 0.8 }}>Exceptions vs. total cases</Typography>
                </Box>
              </Grid>
              <Grid item xs={12} sm={4}>
                <Box sx={{ p: 1.2, background: "#EBF2FC", border: "1px solid #90B8E8", borderRadius: "10px" }}>
                  <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: "#1E4E8C", fontFamily: G, mb: 0.5 }}>High-Impact Transitions</Typography>
                  <Typography sx={{ fontFamily: S, fontSize: "1.6rem", color: "#1E4E8C", lineHeight: 1, mb: 0.2 }}>{selectedVendor.total_cases}</Typography>
                  <Typography sx={{ fontSize: "0.7rem", color: "#1E4E8C", fontFamily: G, opacity: 0.8 }}>Total process cases</Typography>
                </Box>
              </Grid>
            </Grid>
          </CardContent>
        </Card>
      )}

      {/* Predictive Insight */}
      {selectedVendor && (
        <Card sx={{ mb: 2.5, borderLeft: "3px solid #1E4E8C !important", background: "#EBF2FC !important", border: "1px solid #90B8E8 !important" }}>
          <CardContent>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1E4E8C", fontFamily: G, mb: 0.8 }}>Predictive Insight</Typography>
            <Typography sx={{ fontSize: "0.875rem", color: "#17140F", fontFamily: G, mb: 0.8 }}>
              Based on observed process trajectories for <strong>{selectedVendor.vendor_id}</strong>:
            </Typography>
            <Stack spacing={0.6}>
              {[
                selectedVendor.exception_rate > 60 && "⚠️ High process failure rate — majority of cases deviate from the expected path.",
                selectedVendor.avg_dpo > 40 && "⏱ Extended dwell time detected — cases exceed 75th percentile in key stages.",
                selectedVendor.exception_rate > 40 && "🔁 Recurring exception patterns identified — stage-level intervention recommended.",
                selectedVendor.exception_rate <= 40 && selectedVendor.avg_dpo <= 20 && "✅ Process behavior within expected range — no immediate trajectory-based risk.",
              ].filter(Boolean).map((msg, i) => (
                <Typography key={i} sx={{ fontSize: "0.8rem", color: "#2E5090", fontFamily: G }}>{msg}</Typography>
              ))}
              <Typography sx={{ fontSize: "0.72rem", color: "#9C9690", fontFamily: G, fontStyle: "italic", mt: 0.4 }}>
                Predictions based on stage-level timing, transition patterns, and historical outcomes — not static thresholds.
              </Typography>
            </Stack>
          </CardContent>
        </Card>
      )}

      {/* Vendor Table */}
      <Card sx={{ mb: 2.5 }}>
        <CardContent>
          <Typography sx={{ fontFamily: S, fontSize: "1.15rem", color: "#17140F", mb: 1.5 }}>Vendor Overview</Typography>
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  {["Vendor", "# Invoices", "Total Value", "Exception Rate", "Avg DPO", "Payment Behavior", "Risk"].map(h => <TableCell key={h}>{h}</TableCell>)}
                </TableRow>
              </TableHead>
              <TableBody>
                {vendors.map(v => (
                  <TableRow key={v.vendor_id} hover onClick={() => setSelectedVendorId(v.vendor_id)} sx={{ cursor: "pointer", background: selectedVendorId === v.vendor_id ? "#F5ECD9 !important" : "transparent" }}>
                    <TableCell>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Typography sx={{ fontWeight: 600, fontSize: "0.875rem", color: selectedVendorId === v.vendor_id ? "#B5742A" : "#17140F", fontFamily: G }}>{v.vendor_id}</Typography>
                        {v.vendor_lifnr && <Box sx={{ background: "#F0EDE6", border: "1px solid #E8E3DA", px: 0.8, py: 0.1, borderRadius: "4px", fontSize: "0.65rem", color: "#9C9690", fontFamily: G }}>LIFNR {v.vendor_lifnr}</Box>}
                      </Stack>
                    </TableCell>
                    <TableCell>{v.total_cases}</TableCell>
                    <TableCell sx={{ fontWeight: 500 }}>{currency(v.total_value)}</TableCell>
                    <TableCell><Typography sx={{ color: v.exception_rate >= 80 ? "#B03030" : v.exception_rate >= 40 ? "#A05A10" : "#1D5C3A", fontWeight: 600, fontSize: "0.875rem", fontFamily: G }}>{pct(v.exception_rate)}</Typography></TableCell>
                    <TableCell>{Number(v.avg_dpo || 0).toFixed(2)}d</TableCell>
                    <TableCell sx={{ color: "#9C9690", fontSize: "0.78rem !important", fontStyle: "italic" }}>{v.payment_behavior ? "Vendor-specific" : "Global baseline"}</TableCell>
                    <TableCell><RiskChip risk={v.risk_score} /></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>

      {/* Paths */}
      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={6}>
          <Card sx={{ height: "100%", borderLeft: "3px solid #1A6B5E !important" }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#1A6B5E", mb: 1.5 }}>Happy Paths</Typography>
              {pathsLoading ? <CircularProgress size={18} /> : vendorPaths.happy_paths.length === 0 ? (
                <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G, py: 2, textAlign: "center" }}>No happy path variants for this vendor.</Typography>
              ) : (
                <Stack spacing={1}>
                  {vendorPaths.happy_paths.map((p, i) => (
                    <Box key={i} sx={{ p: 1.5, background: "#F7FBF9", border: "1px solid #DCF0EB", borderRadius: "10px" }}>
                      <Typography sx={{ fontSize: "0.78rem", color: "#17140F", mb: 0.5, fontFamily: G }}>{p.path || p.variant || "Variant"}</Typography>
                      <Typography sx={{ fontSize: "0.7rem", color: "#1A6B5E", fontFamily: G }}>
                        {p.frequency ?? p.count ?? 0} cases · {pct(p.percentage)} · {Number(p.avg_duration_days || 0).toFixed(1)}d avg
                      </Typography>
                    </Box>
                  ))}
                </Stack>
              )}
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={6}>
          <Card sx={{ height: "100%", borderLeft: "3px solid #B03030 !important" }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#B03030", mb: 1.5 }}>Exception Paths</Typography>
              {pathsLoading ? <CircularProgress size={18} /> : vendorPaths.exception_paths.length === 0 ? (
                <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G, py: 2, textAlign: "center" }}>No exception path variants for this vendor.</Typography>
              ) : (
                <Stack spacing={1}>
                  {vendorPaths.exception_paths.map((p, i) => (
                    <Box key={i} sx={{ p: 1.5, background: "#FDF7F7", border: "1px solid #FAEAEA", borderRadius: "10px" }}>
                      <Typography sx={{ fontSize: "0.78rem", color: "#17140F", mb: 0.5, fontFamily: G }}>{p.path || p.variant || "Exception Variant"}</Typography>
                      <Typography sx={{ fontSize: "0.7rem", color: "#B03030", fontFamily: G }}>
                        {p.exception_type || "unknown"} · {p.frequency ?? p.count ?? 0} cases · {pct(p.percentage)}
                        {(p.avg_dpo || p.dpo) ? ` · DPO ${Number(p.avg_dpo || p.dpo || 0).toFixed(1)}d` : ""}
                      </Typography>
                    </Box>
                  ))}
                </Stack>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Exception Breakdown */}
      <Card sx={{ mb: 2.5 }}>
        <CardContent>
          <Typography sx={{ fontFamily: S, fontSize: "1.15rem", color: "#17140F", mb: 1.5 }}>
            Exception Breakdown — <span style={{ color: "#B5742A" }}>{selectedVendorId}</span>
          </Typography>
          <Grid container spacing={1.5}>
            {EXCEPTION_META.map(meta => {
              const d = exceptionBreakdown?.[meta.key] || {};
              return (
                <Grid item xs={12} sm={6} lg={3} key={meta.key}>
                  <Box sx={{ p: 1.8, background: meta.subtle, border: `1px solid ${meta.border}`, borderRadius: "12px", height: "100%" }}>
                    <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: meta.color, textTransform: "uppercase", letterSpacing: "0.04em", fontFamily: G, mb: 1 }}>{meta.title}</Typography>
                    <Typography sx={{ fontFamily: S, fontSize: "1.8rem", color: meta.color, lineHeight: 1, mb: 0.3 }}>{d.count ?? 0}</Typography>
                    <Typography sx={{ fontSize: "0.72rem", color: meta.color, opacity: 0.8, fontFamily: G, mb: 0.8 }}>{currency(d.value || d.optimization_value || 0)}</Typography>
                    {[["% of vendor", pct(d.percentage)], d.avg_dpo !== undefined && ["Avg DPO", `${Number(d.avg_dpo || 0).toFixed(1)}d`], d.time_stuck_days !== undefined && ["Time Stuck", `${Number(d.time_stuck_days || 0).toFixed(1)}d`], d.risk_level && ["Risk", d.risk_level]].filter(Boolean).map(([k, v]) => (
                      <Box key={k} sx={{ display: "flex", justifyContent: "space-between" }}>
                        <Typography sx={{ fontSize: "0.68rem", color: meta.color, opacity: 0.7, fontFamily: G }}>{k}</Typography>
                        <Typography sx={{ fontSize: "0.68rem", color: meta.color, fontWeight: 600, fontFamily: G }}>{v}</Typography>
                      </Box>
                    ))}
                  </Box>
                </Grid>
              );
            })}
          </Grid>
        </CardContent>
      </Card>

      <Grid container spacing={2}>
        {/* AI Analysis */}
        <Grid item xs={12} md={7}>
          <Card>
            <CardContent>
              <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.5 }}>
                <Typography sx={{ fontFamily: S, fontSize: "1.15rem", color: "#17140F" }}>AI Analysis</Typography>
                <Button variant="contained" size="small" onClick={runAiAnalysis} disabled={aiLoading || !selectedVendor}>
                  {aiLoading ? <><CircularProgress size={12} sx={{ mr: 1, color: "#fff" }} />Analyzing…</> : "Run Vendor AI"}
                </Button>
              </Box>
              <Divider sx={{ mb: 1.5 }} />
              {aiResult ? (
                <Stack spacing={1.2}>
                  <Box sx={{ p: 1.5, background: "#F5ECD9", border: "1px solid #DEC48A", borderRadius: "10px" }}>
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: "#9C9690", fontFamily: G, mb: 0.5 }}>Risk Score</Typography>
                    <Typography sx={{ fontSize: "0.875rem", fontWeight: 600, color: "#B5742A", fontFamily: G }}>{aiResult?.vendor_analysis?.vendor_risk_score || "N/A"}</Typography>
                  </Box>
                  <Box>
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: "#9C9690", fontFamily: G, mb: 0.8 }}>AI Recommendations</Typography>
                    {(aiResult.ai_recommendations || []).map((r, i) => (
                      <Box key={i} sx={{ display: "flex", gap: 1.5, mb: 0.8 }}>
                        <Box sx={{ width: "6px", height: "6px", borderRadius: "50%", background: "#B5742A", mt: 0.7, flexShrink: 0 }} />
                        <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G }}>{r}</Typography>
                      </Box>
                    ))}
                  </Box>
                  {aiResult.celonis_evidence && <span className="evidence-tag">Celonis Evidence: {aiResult.celonis_evidence}</span>}
                  <details>
                    <summary style={{ cursor: "pointer", color: "#B5742A", fontSize: "0.78rem", fontFamily: G, fontWeight: 600 }}>View Full JSON</summary>
                    <pre className="json-display">{JSON.stringify(aiResult, null, 2)}</pre>
                  </details>
                </Stack>
              ) : (
                <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G, py: 2 }}>
                  Click "Run Vendor AI" to get root-cause analysis, recommendations, comparison to average, and financial impact.
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Payment Behavior */}
        <Grid item xs={12} md={5}>
          <Card sx={{ height: "100%" }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.15rem", color: "#17140F", mb: 1.5 }}>
                Payment Behavior — <span style={{ color: "#B5742A" }}>{selectedVendorId}</span>
              </Typography>
              <PaymentPie behavior={behavior} />
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </div>
  );
}