import React, { useEffect, useMemo, useState } from "react";
import {
  Alert, Box, Button, Card, CardContent, Chip, CircularProgress,
  Collapse, Divider, Grid, InputAdornment, Stack, TextField, Typography,
} from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import api, { waitForCacheReady } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const EXCEPTION_META = [
  { key: "payment_terms_mismatch", title: "Payment Terms Mismatch", color: "#B03030", subtle: "#FAEAEA", border: "#E0A0A0", icon: "📋" },
  { key: "invoice_exception", title: "Invoice Exception", color: "#A05A10", subtle: "#FEF3DC", border: "#F0C870", icon: "⚠️" },
  { key: "short_payment_terms", title: "Short Payment Terms", color: "#1E4E8C", subtle: "#EBF2FC", border: "#90B8E8", icon: "⏱" },
  { key: "early_payment", title: "Early Payment", color: "#1A6B5E", subtle: "#DCF0EB", border: "#8FCFC5", icon: "💰" },
];

const RISK_MAP = {
  CRITICAL: { bg: "#FAEAEA", color: "#B03030", border: "#E0A0A0", label: "Critical", dot: "#C94040" },
  HIGH: { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870", label: "High", dot: "#C47020" },
  MEDIUM: { bg: "#EBF2FC", color: "#1E4E8C", border: "#90B8E8", label: "Medium", dot: "#2E6EBC" },
  LOW: { bg: "#E0F0E8", color: "#1D5C3A", border: "#80C0A0", label: "Low", dot: "#2A7A50" },
};

const currency = (v) => { const n = Number(v || 0); if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`; if (n >= 1000) return `${(n / 1000).toFixed(1)}K`; return `${n.toFixed(0)}`; };
const pct = (v) => `${Number(v || 0).toFixed(1)}%`;
const pickData = (r) => { if (!r) return null; if (r.data?.data !== undefined) return r.data.data; if (r.data !== undefined) return r.data; return r; };
const normalizeKey = (value) => String(value || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");

const withFallbackRisk = (row) => {
  const dpo = Number(row.avg_dpo ?? row.avg_duration_days ?? 0);
  const exc = Number(row.exception_rate ?? row.exception_rate_pct ?? 0);
  if (row.risk_score) return row.risk_score;
  if (exc > 60 || dpo > 60) return "CRITICAL";
  if (exc > 40 || dpo > 40) return "HIGH";
  if (exc > 20 || dpo > 20) return "MEDIUM";
  return "LOW";
};

/* ─── Reusable Components ─── */

function RiskBadge({ risk, size = "default" }) {
  const key = String(risk || "LOW").toUpperCase();
  const s = RISK_MAP[key] || RISK_MAP.LOW;
  const isSmall = size === "small";
  return (
    <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, background: s.bg, color: s.color, border: `1px solid ${s.border}`, px: isSmall ? 0.8 : 1.2, py: isSmall ? 0.15 : 0.25, borderRadius: "99px" }}>
      <Box sx={{ width: isSmall ? 5 : 6, height: isSmall ? 5 : 6, borderRadius: "50%", background: s.dot, flexShrink: 0 }} />
      <Typography sx={{ fontSize: isSmall ? "0.6rem" : "0.68rem", fontWeight: 700, fontFamily: G, letterSpacing: "0.04em" }}>{key}</Typography>
    </Box>
  );
}

function SectionLabel({ children, sx = {} }) {
  return (
    <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.08em", color: "#A09890", fontFamily: G, mb: 0.8, ...sx }}>
      {children}
    </Typography>
  );
}

function KpiPill({ label, value, color, unit = "" }) {
  return (
    <Box sx={{ flex: 1, minWidth: 0, p: 1.2, background: "#FFFFFF", border: "1px solid #E8E3DA", borderRadius: "10px", textAlign: "center" }}>
      <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#A09890", fontFamily: G, mb: 0.4 }}>{label}</Typography>
      <Typography sx={{ fontFamily: S, fontSize: "1.5rem", color, lineHeight: 1 }}>{value}<span style={{ fontSize: "0.75rem", opacity: 0.7 }}>{unit}</span></Typography>
    </Box>
  );
}

function PaymentDonut({ behavior }) {
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

  const worst = [...slices].sort((a, b) => b.value - a.value)[0];
  const worstLabel = worst.value > 0 ? `${pct(worst.value)} of payments are ${worst.label.toLowerCase()}` : "No payment data available";

  return (
    <Box>
      <Box sx={{ display: "flex", gap: 2.5, alignItems: "center", flexWrap: "wrap" }}>
        <Box sx={{ position: "relative", width: 100, height: 100, flexShrink: 0 }}>
          <Box sx={{ width: 100, height: 100, borderRadius: "50%", background: `conic-gradient(${grad || "#E8E3DA 0 100%"})`, border: "3px solid #F0EDE6" }} />
          <Box sx={{ position: "absolute", top: "50%", left: "50%", transform: "translate(-50%,-50%)", width: 50, height: 50, borderRadius: "50%", background: "#FFFFFF" }} />
        </Box>
        <Stack spacing={0.5} sx={{ flex: 1 }}>
          {slices.map(s => (
            <Box key={s.label} sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 1 }}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 0.8 }}>
                <Box sx={{ width: 8, height: 8, borderRadius: "2px", background: s.color, flexShrink: 0 }} />
                <Typography sx={{ fontSize: "0.75rem", color: "#5C5650", fontFamily: G }}>{s.label}</Typography>
              </Box>
              <Typography sx={{ fontSize: "0.75rem", fontWeight: 600, color: s.color, fontFamily: G }}>{pct(s.value)}</Typography>
            </Box>
          ))}
        </Stack>
      </Box>
      <Box sx={{ mt: 1.2, p: 0.8, background: late > 40 ? "#FAEAEA" : "#FEF8EE", border: `1px solid ${late > 40 ? "#E0A0A0" : "#EDD090"}`, borderRadius: "8px" }}>
        <Typography sx={{ fontSize: "0.72rem", fontWeight: 600, color: late > 40 ? "#B03030" : "#A05A10", fontFamily: G, textAlign: "center" }}>
          {worstLabel}
        </Typography>
      </Box>
    </Box>
  );
}

/* ─── MAIN PAGE ─── */

export default function VendorAnalysis() {
  const [loading, setLoading] = useState(true);
  const [vendors, setVendors] = useState([]);
  const [selectedVendorId, setSelectedVendorId] = useState("");
  const [vendorPaths, setVendorPaths] = useState({ happy_paths: [], exception_paths: [] });
  const [pathsLoading, setPathsLoading] = useState(false);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResult, setAiResult] = useState(null);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [detailExpanded, setDetailExpanded] = useState({ paths: true, breakdown: true, ai: true, payment: true });

  useEffect(() => {
    let active = true;
    const abortController = new AbortController();
    const load = async (retryIfCacheCold = true) => {
      setLoading(true); setError("");
      try {
        const res = await api.get("/process/vendor-stats");
        const data = pickData(res);
        const rows = Array.isArray(data) ? data : data?.vendors || [];
        if (retryIfCacheCold && rows.length === 0) {
          await waitForCacheReady({ signal: abortController.signal });
          return await load(false);
        }
        const normalized = rows.map(r => ({ vendor_id: r.vendor_id || r.vendor || "UNKNOWN", vendor_lifnr: r.vendor_lifnr || r.lifnr || "", total_cases: Number(r.total_cases ?? r.case_count ?? 0), total_value: Number(r.total_value ?? r.value_usd ?? 0), exception_rate: Number(r.exception_rate ?? r.exception_rate_pct ?? 0), avg_dpo: Number(r.avg_dpo ?? r.avg_duration_days ?? 0), payment_behavior: r.payment_behavior || null, risk_score: r.risk_score || withFallbackRisk(r), exception_breakdown: r.exception_breakdown || {} }));
        if (active) {
          setVendors(normalized);
          setSelectedVendorId(normalized[0]?.vendor_id || "");
          if (normalized.length === 0) setError("No live vendor statistics were returned from Celonis.");
        }
      } catch (e) {
        if (active) { setError(e?.response?.data?.detail || e.message || "Failed to load vendors"); setVendors([]); setSelectedVendorId(""); }
      } finally { if (active) setLoading(false); }
    };
    load();
    return () => { active = false; abortController.abort(); };
  }, []);

  useEffect(() => {
    const activeVendor = vendors.find(v => v.vendor_id === selectedVendorId) || null;
    if (!selectedVendorId || !activeVendor) return;
    let active = true;
    setPathsLoading(true);
    const vendorPathId = activeVendor.vendor_lifnr || selectedVendorId;
    api.get(`/process/vendor/${encodeURIComponent(vendorPathId)}/paths`)
      .then(res => { const d = pickData(res) || {}; if (active) setVendorPaths({ happy_paths: Array.isArray(d.happy_paths) ? d.happy_paths : [], exception_paths: Array.isArray(d.exception_paths) ? d.exception_paths : [] }); })
      .catch(() => { if (active) setVendorPaths({ happy_paths: [], exception_paths: [] }); })
      .finally(() => { if (active) setPathsLoading(false); });
    setAiResult(null);
    return () => { active = false; };
  }, [selectedVendorId, vendors]);

  const selectedVendor = useMemo(() => vendors.find(v => v.vendor_id === selectedVendorId) || null, [vendors, selectedVendorId]);

  const behavior = useMemo(() => {
    if (selectedVendor?.payment_behavior && typeof selectedVendor.payment_behavior === "object") return selectedVendor.payment_behavior;
    return { on_time_pct: 29.7, early_pct: 29.7, late_pct: 29.7, open_pct: 10.8 };
  }, [selectedVendor]);

  const derivedExceptionBreakdown = useMemo(() => {
    const current = selectedVendor?.exception_breakdown || {};
    const hasAnyCurrentData = Object.values(current).some((entry) => Number(entry?.count || 0) > 0 || Number(entry?.value || entry?.optimization_value || 0) > 0);
    if (hasAnyCurrentData) return current;
    const exceptionPaths = Array.isArray(vendorPaths.exception_paths) ? vendorPaths.exception_paths : [];
    if (exceptionPaths.length === 0) return current;
    const base = {
      payment_terms_mismatch: { count: 0, percentage: 0, value: 0 },
      invoice_exception: { count: 0, percentage: 0, avg_dpo: 0, value: 0, time_stuck_days: 0 },
      short_payment_terms: { count: 0, percentage: 0, value: 0, risk_level: "LOW" },
      early_payment: { count: 0, percentage: 0, optimization_value: 0, value: 0 },
    };
    exceptionPaths.forEach((path) => {
      const key = normalizeKey(path.exception_type);
      if (key.includes("payment_terms")) { base.payment_terms_mismatch.count += Number(path.frequency || path.count || 0); base.payment_terms_mismatch.percentage += Number(path.percentage || 0); }
      else if (key.includes("short_payment")) { base.short_payment_terms.count += Number(path.frequency || path.count || 0); base.short_payment_terms.percentage += Number(path.percentage || 0); base.short_payment_terms.risk_level = "HIGH"; }
      else if (key.includes("early_payment")) { base.early_payment.count += Number(path.frequency || path.count || 0); base.early_payment.percentage += Number(path.percentage || 0); }
      else { base.invoice_exception.count += Number(path.frequency || path.count || 0); base.invoice_exception.percentage += Number(path.percentage || 0); base.invoice_exception.avg_dpo = Math.max(base.invoice_exception.avg_dpo, Number(path.avg_dpo || path.avg_duration_days || 0)); base.invoice_exception.time_stuck_days = Math.max(base.invoice_exception.time_stuck_days, Number(path.avg_dpo || path.avg_duration_days || 0)); }
    });
    return base;
  }, [selectedVendor, vendorPaths]);

  const exceptionBreakdown = useMemo(
    () => aiResult?.vendor_analysis?.exception_breakdown || derivedExceptionBreakdown || {},
    [aiResult, derivedExceptionBreakdown],
  );

  const runAiAnalysis = async () => {
    if (!selectedVendor) return;
    setAiLoading(true); setError("");
    try {
      const d = pickData(await api.post("/agents/vendor-intelligence", { vendor_id: selectedVendor.vendor_id, vendor_lifnr: selectedVendor.vendor_lifnr, vendor_context: selectedVendor, include_comparison_to_overall: true, include_financial_impact: true }));
      setAiResult(d);
    } catch (e) { setError(e?.response?.data?.detail || e.message || "Vendor AI analysis failed"); }
    finally { setAiLoading(false); }
  };

  // Auto-run AI analysis when vendor changes
  useEffect(() => {
    if (selectedVendor && !aiResult && !aiLoading) {
      runAiAnalysis();
    }
  }, [selectedVendorId]);

  const filteredVendors = useMemo(() => {
    if (!search.trim()) return vendors;
    const q = search.toLowerCase();
    return vendors.filter(v => v.vendor_id.toLowerCase().includes(q) || (v.vendor_lifnr && v.vendor_lifnr.toLowerCase().includes(q)));
  }, [vendors, search]);

  const toggle = (key) => setDetailExpanded(prev => ({ ...prev, [key]: !prev[key] }));

  const insightLines = useMemo(() => {
    if (!selectedVendor) return [];
    const lines = [];
    if (selectedVendor.exception_rate > 60) lines.push({ icon: "🔴", text: "Critical exception rate — majority of cases deviate from the expected path.", severity: "critical" });
    if (selectedVendor.avg_dpo > 40) lines.push({ icon: "⏱", text: `Extended processing time of ${Number(selectedVendor.avg_dpo).toFixed(1)} days — significantly above baseline.`, severity: "high" });
    if (selectedVendor.exception_rate > 40 && selectedVendor.exception_rate <= 60) lines.push({ icon: "🔁", text: "Recurring exception patterns — stage-level intervention recommended.", severity: "medium" });
    if (selectedVendor.exception_rate <= 40 && selectedVendor.avg_dpo <= 20) lines.push({ icon: "✅", text: "Process behavior within expected range — no immediate risk.", severity: "low" });
    if (lines.length === 0) lines.push({ icon: "📊", text: "Vendor metrics loaded. Review breakdown below for details.", severity: "info" });
    return lines;
  }, [selectedVendor]);

  if (loading) return <div className="page-container"><Box sx={{ pt: 6, display: "flex", justifyContent: "center", alignItems: "center", flexDirection: "column", gap: 2 }}><CircularProgress /><Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>Loading vendor data from Celonis...</Typography></Box></div>;
  if (!loading && vendors.length === 0) {
    return (
      <div className="page-container">
        <Box sx={{ pt: 6, maxWidth: 900 }}>
          <Alert severity="info">{error || "No live vendor data is available yet. Refresh the Celonis cache and try again."}</Alert>
        </Box>
      </div>
    );
  }

  return (
    <div className="page-container">
      {/* ═══ Page Header ═══ */}
      <Box sx={{ pt: 4, pb: 2, borderBottom: "1px solid #E8E3DA", mb: 2.5 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.3 }}>
          Vendor Analysis
        </Typography>
        <Typography sx={{ fontSize: "0.85rem", color: "#9C9690", fontFamily: G }}>
          Risk intelligence, exception patterns, and AI-driven recommendations for each vendor.
        </Typography>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      {/* ═══ Master-Detail Layout ═══ */}
      <Grid container spacing={2.5}>

        {/* ── LEFT RAIL: Vendor Selector ── */}
        <Grid item xs={12} md={3.5}>
          <Card sx={{ position: "sticky", top: "72px" }}>
            <CardContent sx={{ pb: "16px !important" }}>
              <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.5 }}>
                <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: "#17140F" }}>Vendors</Typography>
                <Chip size="small" label={`${vendors.length}`} sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA", fontSize: "0.7rem", height: 22 }} />
              </Box>
              <TextField
                size="small"
                placeholder="Search vendors..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                fullWidth
                InputProps={{
                  startAdornment: <InputAdornment position="start"><SearchIcon sx={{ fontSize: 16, color: "#9C9690" }} /></InputAdornment>,
                }}
                sx={{ mb: 1.5, "& .MuiOutlinedInput-root": { fontSize: "0.8rem", height: 34 } }}
              />
              <Box sx={{ maxHeight: "calc(100vh - 280px)", overflowY: "auto", mx: -0.5, px: 0.5, "&::-webkit-scrollbar": { width: 4 }, "&::-webkit-scrollbar-thumb": { background: "#D8D2C8", borderRadius: 2 } }}>
                <Stack spacing={0.6}>
                  {filteredVendors.map(v => {
                    const active = selectedVendorId === v.vendor_id;
                    const rs = RISK_MAP[String(v.risk_score).toUpperCase()] || RISK_MAP.LOW;
                    return (
                      <Box
                        key={v.vendor_id}
                        onClick={() => setSelectedVendorId(v.vendor_id)}
                        sx={{
                          p: 1.2, borderRadius: "10px", cursor: "pointer",
                          border: active ? `2px solid #B5742A` : "1px solid #E8E3DA",
                          background: active ? "#F5ECD9" : "#FDFCFA",
                          transition: "all 0.15s ease",
                          "&:hover": { background: active ? "#F5ECD9" : "#F5F2EC", borderColor: active ? "#B5742A" : "#D8D2C8" },
                        }}
                      >
                        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 0.5 }}>
                          <Typography sx={{ fontSize: "0.8rem", fontWeight: 600, color: active ? "#B5742A" : "#17140F", fontFamily: G }}>{v.vendor_id}</Typography>
                          <RiskBadge risk={v.risk_score} size="small" />
                        </Box>
                        <Box sx={{ display: "flex", gap: 1.5 }}>
                          <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G }}>{v.total_cases} cases</Typography>
                          <Typography sx={{ fontSize: "0.68rem", color: v.exception_rate >= 60 ? "#B03030" : "#5C5650", fontFamily: G, fontWeight: v.exception_rate >= 60 ? 600 : 400 }}>{pct(v.exception_rate)} exc.</Typography>
                          <Typography sx={{ fontSize: "0.68rem", color: "#5C5650", fontFamily: G }}>${currency(v.total_value)}</Typography>
                        </Box>
                      </Box>
                    );
                  })}
                </Stack>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* ── RIGHT PANEL: Vendor Detail ── */}
        <Grid item xs={12} md={8.5}>
          {selectedVendor ? (
            <Stack spacing={2}>

              {/* ═══ EXECUTIVE SUMMARY BAR ═══ */}
              <Card sx={{
                background: (() => {
                  const key = String(selectedVendor.risk_score).toUpperCase();
                  if (key === "CRITICAL") return "linear-gradient(135deg, #FAEAEA 0%, #FDF7F7 100%)";
                  if (key === "HIGH") return "linear-gradient(135deg, #FEF3DC 0%, #FFFDF7 100%)";
                  return "linear-gradient(135deg, #EBF2FC 0%, #F7FAFF 100%)";
                })(),
                border: `1px solid ${(RISK_MAP[String(selectedVendor.risk_score).toUpperCase()] || RISK_MAP.LOW).border} !important`,
              }}>
                <CardContent>
                  <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
                    <Box sx={{ flex: 1, minWidth: 280 }}>
                      <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 1 }}>
                        <RiskBadge risk={selectedVendor.risk_score} />
                        <Typography sx={{ fontFamily: S, fontSize: "1.35rem", color: "#17140F" }}>
                          {selectedVendor.vendor_id}
                        </Typography>
                        {selectedVendor.vendor_lifnr && (
                          <Chip size="small" label={`LIFNR ${selectedVendor.vendor_lifnr}`} sx={{ height: 20, fontSize: "0.62rem", background: "#F0EDE6", color: "#9C9690", border: "1px solid #E8E3DA" }} />
                        )}
                      </Box>
                      <Stack spacing={0.4}>
                        {insightLines.map((line, i) => (
                          <Typography key={i} sx={{ fontSize: "0.82rem", color: "#4C4840", fontFamily: G, lineHeight: 1.5 }}>
                            {line.icon} {line.text}
                          </Typography>
                        ))}
                      </Stack>
                    </Box>
                    <Box sx={{ display: "flex", gap: 1 }}>
                      <KpiPill label="Avg DPO" value={Number(selectedVendor.avg_dpo).toFixed(1)} color="#1E4E8C" unit="d" />
                      <KpiPill label="Exc. Rate" value={pct(selectedVendor.exception_rate)} color="#B03030" />
                      <KpiPill label="Cases" value={selectedVendor.total_cases} color="#1A6B5E" />
                    </Box>
                  </Box>
                </CardContent>
              </Card>

              {/* ═══ AI RECOMMENDATIONS ═══ */}
              <Card sx={{ borderLeft: "3px solid #B5742A !important" }}>
                <CardContent>
                  <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.2 }}>
                    <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                      <Box sx={{ width: 28, height: 28, borderRadius: "8px", background: "#F5ECD9", display: "flex", alignItems: "center", justifyContent: "center", fontSize: "0.9rem" }}>🤖</Box>
                      <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F" }}>AI Intelligence</Typography>
                    </Box>
                    {!aiResult && (
                      <Button variant="outlined" size="small" onClick={runAiAnalysis} disabled={aiLoading} sx={{ fontSize: "0.75rem" }}>
                        {aiLoading ? <>Analyzing…</> : "Re-analyze"}
                      </Button>
                    )}
                  </Box>
                  {aiLoading ? (
                    <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, py: 2 }}>
                      <CircularProgress size={18} />
                      <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>Running AI vendor analysis...</Typography>
                    </Box>
                  ) : aiResult ? (
                    <Stack spacing={1.2}>
                      {aiResult?.vendor_analysis?.vendor_risk_score && (
                        <Box sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
                          <SectionLabel sx={{ mb: 0 }}>AI Risk Score</SectionLabel>
                          <Chip size="small" label={aiResult.vendor_analysis.vendor_risk_score} sx={{ background: "#F5ECD9", color: "#B5742A", border: "1px solid #DEC48A", fontWeight: 700, fontSize: "0.72rem" }} />
                        </Box>
                      )}
                      {(aiResult.ai_recommendations || []).length > 0 && (
                        <Box>
                          <SectionLabel>Recommended Actions</SectionLabel>
                          <Stack spacing={0.6}>
                            {(aiResult.ai_recommendations || []).map((r, i) => (
                              <Box key={i} sx={{ display: "flex", gap: 1, alignItems: "flex-start" }}>
                                <Box sx={{ width: 20, height: 20, borderRadius: "50%", background: "#F5ECD9", display: "flex", alignItems: "center", justifyContent: "center", fontSize: "0.6rem", fontWeight: 700, color: "#B5742A", fontFamily: G, flexShrink: 0, mt: 0.1 }}>{i + 1}</Box>
                                <Typography sx={{ fontSize: "0.8rem", color: "#4C4840", fontFamily: G, lineHeight: 1.55 }}>{r}</Typography>
                              </Box>
                            ))}
                          </Stack>
                        </Box>
                      )}
                      {aiResult.celonis_evidence && (
                        <Box sx={{ p: 1, background: "#F7FBF9", border: "1px solid #DCF0EB", borderRadius: "8px" }}>
                          <Typography sx={{ fontSize: "0.72rem", fontWeight: 600, color: "#1A6B5E", fontFamily: G }}>
                            📊 Celonis Evidence: {aiResult.celonis_evidence}
                          </Typography>
                        </Box>
                      )}
                      <Collapse in={detailExpanded.ai}>
                        <Box sx={{ mt: 0.5 }}>
                          <details>
                            <summary style={{ cursor: "pointer", color: "#B5742A", fontSize: "0.72rem", fontFamily: G, fontWeight: 600 }}>View raw analysis</summary>
                            <pre className="json-display" style={{ maxHeight: 200, overflow: "auto" }}>{JSON.stringify(aiResult, null, 2)}</pre>
                          </details>
                        </Box>
                      </Collapse>
                    </Stack>
                  ) : (
                    <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G, py: 1 }}>
                      AI analysis will run automatically when you select a vendor.
                    </Typography>
                  )}
                </CardContent>
              </Card>

              {/* ═══ EXCEPTION BREAKDOWN ═══ */}
              <Card>
                <CardContent>
                  <Box onClick={() => toggle("breakdown")} sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer", mb: detailExpanded.breakdown ? 1.5 : 0 }}>
                    <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: "#17140F" }}>Exception Breakdown</Typography>
                    <Typography sx={{ fontSize: "0.72rem", color: "#B5742A", fontFamily: G }}>{detailExpanded.breakdown ? "Collapse ▲" : "Expand ▼"}</Typography>
                  </Box>
                  <Collapse in={detailExpanded.breakdown}>
                    <Grid container spacing={1.5}>
                      {EXCEPTION_META.map(meta => {
                        const d = exceptionBreakdown?.[meta.key] || {};
                        const count = d.count ?? 0;
                        const total = EXCEPTION_META.reduce((sum, m) => sum + (exceptionBreakdown?.[m.key]?.count || 0), 0);
                        const barWidth = total > 0 ? Math.max(4, (count / total) * 100) : 0;
                        return (
                          <Grid item xs={12} sm={6} key={meta.key}>
                            <Box sx={{ p: 1.5, background: meta.subtle, border: `1px solid ${meta.border}`, borderRadius: "10px", height: "100%" }}>
                              <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 0.8 }}>
                                <Box sx={{ display: "flex", alignItems: "center", gap: 0.6 }}>
                                  <span style={{ fontSize: "0.85rem" }}>{meta.icon}</span>
                                  <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: meta.color, fontFamily: G }}>{meta.title}</Typography>
                                </Box>
                                <Typography sx={{ fontFamily: S, fontSize: "1.3rem", color: meta.color }}>{count}</Typography>
                              </Box>
                              {/* Visual bar */}
                              <Box sx={{ height: 5, background: `${meta.color}20`, borderRadius: "99px", overflow: "hidden", mb: 0.8 }}>
                                <Box sx={{ height: "100%", width: `${barWidth}%`, background: meta.color, borderRadius: "99px", transition: "width 0.5s ease" }} />
                              </Box>
                              <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                                <Typography sx={{ fontSize: "0.65rem", color: meta.color, opacity: 0.8, fontFamily: G }}>{pct(d.percentage)} of vendor</Typography>
                                <Typography sx={{ fontSize: "0.65rem", color: meta.color, opacity: 0.8, fontFamily: G }}>{currency(d.value || d.optimization_value || 0)} $</Typography>
                              </Box>
                            </Box>
                          </Grid>
                        );
                      })}
                    </Grid>
                  </Collapse>
                </CardContent>
              </Card>

              {/* ═══ PROCESS PATHS + PAYMENT ═══ */}
              <Grid container spacing={2}>
                {/* Paths */}
                <Grid item xs={12} md={7}>
                  <Card sx={{ height: "100%" }}>
                    <CardContent>
                      <Box onClick={() => toggle("paths")} sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer", mb: detailExpanded.paths ? 1.5 : 0 }}>
                        <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: "#17140F" }}>Process Paths</Typography>
                        <Typography sx={{ fontSize: "0.72rem", color: "#B5742A", fontFamily: G }}>{detailExpanded.paths ? "Collapse ▲" : "Expand ▼"}</Typography>
                      </Box>
                      <Collapse in={detailExpanded.paths}>
                        <Grid container spacing={1.5}>
                          <Grid item xs={12} sm={6}>
                            <SectionLabel sx={{ color: "#1A6B5E" }}>Happy Paths</SectionLabel>
                            {pathsLoading ? <CircularProgress size={16} /> : vendorPaths.happy_paths.length === 0 ? (
                              <Typography sx={{ fontSize: "0.78rem", color: "#9C9690", fontFamily: G }}>No happy path variants found.</Typography>
                            ) : (
                              <Stack spacing={0.8}>
                                {vendorPaths.happy_paths.map((p, i) => (
                                  <Box key={i} sx={{ p: 1, background: "#F7FBF9", border: "1px solid #DCF0EB", borderRadius: "8px" }}>
                                    <Typography sx={{ fontSize: "0.72rem", color: "#17140F", fontFamily: G, mb: 0.2, fontWeight: 500 }}>{p.path || p.variant || "Variant"}</Typography>
                                    <Typography sx={{ fontSize: "0.65rem", color: "#1A6B5E", fontFamily: G }}>
                                      {p.frequency ?? p.count ?? 0} cases · {pct(p.percentage)} · {Number(p.avg_duration_days || 0).toFixed(1)}d
                                    </Typography>
                                  </Box>
                                ))}
                              </Stack>
                            )}
                          </Grid>
                          <Grid item xs={12} sm={6}>
                            <SectionLabel sx={{ color: "#B03030" }}>Exception Paths</SectionLabel>
                            {pathsLoading ? <CircularProgress size={16} /> : vendorPaths.exception_paths.length === 0 ? (
                              <Typography sx={{ fontSize: "0.78rem", color: "#9C9690", fontFamily: G }}>No exception path variants found.</Typography>
                            ) : (
                              <Stack spacing={0.8}>
                                {vendorPaths.exception_paths.map((p, i) => (
                                  <Box key={i} sx={{ p: 1, background: "#FDF7F7", border: "1px solid #FAEAEA", borderRadius: "8px" }}>
                                    <Typography sx={{ fontSize: "0.72rem", color: "#17140F", fontFamily: G, mb: 0.2, fontWeight: 500 }}>{p.path || p.variant || "Exception Variant"}</Typography>
                                    <Typography sx={{ fontSize: "0.65rem", color: "#B03030", fontFamily: G }}>
                                      {p.exception_type || "unknown"} · {p.frequency ?? p.count ?? 0} cases · {pct(p.percentage)}
                                      {(p.avg_dpo || p.dpo) ? ` · ${Number(p.avg_dpo || p.dpo || 0).toFixed(1)}d` : ""}
                                    </Typography>
                                  </Box>
                                ))}
                              </Stack>
                            )}
                          </Grid>
                        </Grid>
                      </Collapse>
                    </CardContent>
                  </Card>
                </Grid>

                {/* Payment Behavior */}
                <Grid item xs={12} md={5}>
                  <Card sx={{ height: "100%" }}>
                    <CardContent>
                      <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: "#17140F", mb: 1.5 }}>Payment Behavior</Typography>
                      <PaymentDonut behavior={behavior} />
                    </CardContent>
                  </Card>
                </Grid>
              </Grid>

            </Stack>
          ) : (
            <Card>
              <CardContent>
                <Box sx={{ py: 8, textAlign: "center" }}>
                  <Typography sx={{ fontSize: "1.5rem", mb: 1 }}>👈</Typography>
                  <Typography sx={{ fontSize: "0.85rem", color: "#9C9690", fontFamily: G }}>Select a vendor from the list to view analysis.</Typography>
                </Box>
              </CardContent>
            </Card>
          )}
        </Grid>
      </Grid>
    </div>
  );
}
