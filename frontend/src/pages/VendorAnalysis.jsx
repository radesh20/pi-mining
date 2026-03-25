import React, { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Divider,
  Grid,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import api from "../api/client";

const FALLBACK_VENDORS = [
  { vendor_id: "D4", vendor_lifnr: "7003198830", total_cases: 3, total_value: 5700000 },
  { vendor_id: "B2", vendor_lifnr: "", total_cases: 2, total_value: 6200000 },
  { vendor_id: "A27", vendor_lifnr: "", total_cases: 2, total_value: 4100000 },
  { vendor_id: "F6", vendor_lifnr: "", total_cases: 2, total_value: 3300000 },
  { vendor_id: "H8", vendor_lifnr: "7003204990", total_cases: 1, total_value: 2900000 },
  { vendor_id: "C3", vendor_lifnr: "7003205015", total_cases: 1, total_value: 1500000 },
  { vendor_id: "I9", vendor_lifnr: "7003255948", total_cases: 1, total_value: 0 },
  { vendor_id: "V22", vendor_lifnr: "7003204531", total_cases: 1, total_value: 0 },
];

const GLOBAL_PAYMENT_BEHAVIOR = {
  open: { count: 4, value: 2960000, pct: 10.8, color: "#90a4ae" },
  paid_early: { count: 11, value: 9550000, pct: 29.7, color: "#42a5f5" },
  paid_late: { count: 11, value: 5410000, pct: 29.7, color: "#ef5350" },
  paid_on_time: { count: 11, value: 4590000, pct: 29.7, color: "#66bb6a" },
};

const EXCEPTION_META = [
  { key: "payment_terms_mismatch", title: "Payment Terms Mismatch", color: "#f44336" },
  { key: "invoice_exception", title: "Invoice Exception", color: "#ff9800" },
  { key: "short_payment_terms", title: "Short Payment Terms", color: "#fdd835" },
  { key: "early_payment", title: "Early Payment", color: "#42a5f5" },
];

const currency = (value) => {
  const num = Number(value || 0);
  if (num >= 1000000) return `${(num / 1000000).toFixed(2)}M $`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K $`;
  return `${num.toFixed(0)} $`;
};

const pct = (value) => `${Number(value || 0).toFixed(1)}%`;

const pickData = (res) => {
  if (!res) return null;
  if (res.data?.data !== undefined) return res.data.data;
  if (res.data !== undefined) return res.data;
  return res;
};

const withFallbackRisk = (row) => {
  const dpo = Number(row.avg_dpo ?? row.avg_duration_days ?? 0);
  const exc = Number(row.exception_rate ?? row.exception_rate_pct ?? 0);
  if (row.risk_score) return row.risk_score;
  if (exc > 60 || dpo > 60) return "CRITICAL";
  if (exc > 40 || dpo > 40) return "HIGH";
  if (exc > 20 || dpo > 20) return "MEDIUM";
  return "LOW";
};

const inferPaymentBehavior = (vendorRow) => {
  if (vendorRow?.payment_behavior && typeof vendorRow.payment_behavior === "object") {
    return vendorRow.payment_behavior;
  }
  return {
    on_time_pct: GLOBAL_PAYMENT_BEHAVIOR.paid_on_time.pct,
    early_pct: GLOBAL_PAYMENT_BEHAVIOR.paid_early.pct,
    late_pct: GLOBAL_PAYMENT_BEHAVIOR.paid_late.pct,
    open_pct: GLOBAL_PAYMENT_BEHAVIOR.open.pct,
  };
};

function PaymentBehaviorPie({ behavior }) {
  const onTime = Number(behavior?.on_time_pct || 0);
  const early = Number(behavior?.early_pct || 0);
  const late = Number(behavior?.late_pct || 0);
  const open = Number(behavior?.open_pct || 0);

  const slices = [
    { label: "Paid on Time", value: onTime, color: GLOBAL_PAYMENT_BEHAVIOR.paid_on_time.color },
    { label: "Paid Early", value: early, color: GLOBAL_PAYMENT_BEHAVIOR.paid_early.color },
    { label: "Paid Late", value: late, color: GLOBAL_PAYMENT_BEHAVIOR.paid_late.color },
    { label: "Open", value: open, color: GLOBAL_PAYMENT_BEHAVIOR.open.color },
  ];

  let cursor = 0;
  const gradient = slices
    .map((s) => {
      const start = cursor;
      cursor += s.value;
      return `${s.color} ${start}% ${cursor}%`;
    })
    .join(", ");

  return (
    <Box sx={{ display: "flex", gap: 2, alignItems: "center", flexWrap: "wrap" }}>
      <Box
        sx={{
          width: 150,
          height: 150,
          borderRadius: "50%",
          background: `conic-gradient(${gradient || "#90a4ae 0 100%"})`,
          border: "1px solid #dbe3ee",
        }}
      />
      <Stack spacing={0.7}>
        {slices.map((s) => (
          <Box key={s.label} sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Box sx={{ width: 10, height: 10, borderRadius: "50%", background: s.color }} />
            <Typography variant="body2" sx={{ color: "#1f2937" }}>
              {s.label}: {pct(s.value)}
            </Typography>
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
    const loadVendorStats = async () => {
      setLoading(true);
      setError("");
      try {
        const res = await api.get("/process/vendor-stats");
        const data = pickData(res);
        const rows = Array.isArray(data) ? data : data?.vendors || [];
        const normalized = rows.length
          ? rows.map((r) => ({
              vendor_id: r.vendor_id || r.vendor || "UNKNOWN",
              vendor_lifnr: r.vendor_lifnr || r.lifnr || "",
              total_cases: Number(r.total_cases ?? r.case_count ?? r.invoices ?? 0),
              total_value: Number(r.total_value ?? r.value_usd ?? r.value ?? 0),
              exception_rate: Number(r.exception_rate ?? r.exception_rate_pct ?? 0),
              avg_dpo: Number(r.avg_dpo ?? r.avg_duration_days ?? 0),
              payment_behavior: r.payment_behavior || null,
              risk_score: r.risk_score || withFallbackRisk(r),
            }))
          : FALLBACK_VENDORS.map((v) => ({
              ...v,
              exception_rate: 0,
              avg_dpo: 0,
              payment_behavior: null,
              risk_score: "MEDIUM",
            }));

        if (active) {
          setVendors(normalized);
          if (!normalized.some((v) => v.vendor_id === selectedVendorId)) {
            setSelectedVendorId(normalized[0]?.vendor_id || "D4");
          }
        }
      } catch (e) {
        if (active) {
          setError(e?.response?.data?.detail || e.message || "Failed to load vendor stats");
          setVendors(
            FALLBACK_VENDORS.map((v) => ({
              ...v,
              exception_rate: 0,
              avg_dpo: 0,
              payment_behavior: null,
              risk_score: "MEDIUM",
            }))
          );
        }
      } finally {
        if (active) setLoading(false);
      }
    };
    loadVendorStats();
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!selectedVendorId) return;
    let active = true;
    const loadPaths = async () => {
      setPathsLoading(true);
      try {
        const res = await api.get(`/process/vendor/${encodeURIComponent(selectedVendorId)}/paths`);
        const data = pickData(res) || {};
        const happy = Array.isArray(data.happy_paths) ? data.happy_paths : data.happy || [];
        const exception = Array.isArray(data.exception_paths) ? data.exception_paths : data.exceptions || [];
        if (active) {
          setVendorPaths({
            happy_paths: happy,
            exception_paths: exception,
          });
        }
      } catch {
        if (active) {
          setVendorPaths({
            happy_paths: [],
            exception_paths: [],
          });
        }
      } finally {
        if (active) setPathsLoading(false);
      }
    };
    loadPaths();
    setAiResult(null);
    return () => {
      active = false;
    };
  }, [selectedVendorId]);

  const selectedVendor = useMemo(
    () => vendors.find((v) => v.vendor_id === selectedVendorId) || null,
    [vendors, selectedVendorId]
  );

  const behavior = useMemo(() => inferPaymentBehavior(selectedVendor), [selectedVendor]);

  const exceptionBreakdown = useMemo(() => {
    if (aiResult?.vendor_analysis?.exception_breakdown) {
      return aiResult.vendor_analysis.exception_breakdown;
    }
    if (selectedVendor?.exception_breakdown) {
      return selectedVendor.exception_breakdown;
    }
    return {
      payment_terms_mismatch: { count: 0, percentage: 0, value: 0 },
      invoice_exception: { count: 0, percentage: 0, avg_dpo: 0, value: 0, time_stuck_days: 0 },
      short_payment_terms: { count: 0, percentage: 0, value: 0, risk_level: "N/A" },
      early_payment: { count: 0, percentage: 0, optimization_value: 0, value: 0 },
    };
  }, [aiResult, selectedVendor]);

  const runAiAnalysis = async () => {
    if (!selectedVendor) return;
    setAiLoading(true);
    setError("");
    try {
      const res = await api.post("/agents/vendor-intelligence", {
        vendor_id: selectedVendor.vendor_id,
        vendor_lifnr: selectedVendor.vendor_lifnr,
        vendor_context: selectedVendor,
        include_comparison_to_overall: true,
        include_financial_impact: true,
      });
      const data = pickData(res);
      setAiResult(data);
    } catch (e) {
      setError(e?.response?.data?.detail || e.message || "Vendor AI analysis failed");
      setAiResult(null);
    } finally {
      setAiLoading(false);
    }
  };

  if (loading) {
    return (
      <Box className="page-container" sx={{ p: 2 }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box className="page-container">
      <Typography variant="h4" sx={{ fontWeight: 700, mb: 1, color: "#1f2937" }}>
        Vendor Analysis
      </Typography>
      <Typography variant="body2" sx={{ color: "#6b7280", mb: 2 }}>
        External Suppliers under Company Code AC33
      </Typography>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      <Card sx={{ background: "#ffffff", border: "1px solid #e5e7eb", mb: 2 }}>
        <CardContent>
          <Typography variant="h6" sx={{ color: "#1f2937", mb: 1.2, fontWeight: 700 }}>
            1) Vendor Overview
          </Typography>
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell sx={{ color: "#6b7280", fontWeight: 700 }}>Vendor</TableCell>
                  <TableCell sx={{ color: "#6b7280", fontWeight: 700 }}># Invoices</TableCell>
                  <TableCell sx={{ color: "#6b7280", fontWeight: 700 }}>Total Value</TableCell>
                  <TableCell sx={{ color: "#6b7280", fontWeight: 700 }}>Exception Rate</TableCell>
                  <TableCell sx={{ color: "#6b7280", fontWeight: 700 }}>Avg DPO</TableCell>
                  <TableCell sx={{ color: "#6b7280", fontWeight: 700 }}>Payment Behavior</TableCell>
                  <TableCell sx={{ color: "#6b7280", fontWeight: 700 }}>Risk Score</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {vendors.map((v) => (
                  <TableRow
                    key={v.vendor_id}
                    hover
                    onClick={() => setSelectedVendorId(v.vendor_id)}
                    sx={{
                      cursor: "pointer",
                      background: selectedVendorId === v.vendor_id ? "#eff6ff" : "transparent",
                    }}
                  >
                    <TableCell sx={{ color: "#1f2937" }}>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <strong>{v.vendor_id}</strong>
                        {v.vendor_lifnr ? <Chip size="small" label={`LIFNR ${v.vendor_lifnr}`} /> : null}
                      </Stack>
                    </TableCell>
                    <TableCell sx={{ color: "#374151" }}>{v.total_cases}</TableCell>
                    <TableCell sx={{ color: "#374151" }}>{currency(v.total_value)}</TableCell>
                    <TableCell sx={{ color: "#374151" }}>{pct(v.exception_rate)}</TableCell>
                    <TableCell sx={{ color: "#374151" }}>{Number(v.avg_dpo || 0).toFixed(2)} days</TableCell>
                    <TableCell sx={{ color: "#374151" }}>
                      {v.payment_behavior ? "Vendor-specific" : "Global baseline"}
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={v.risk_score}
                        size="small"
                        color={
                          String(v.risk_score).toUpperCase() === "CRITICAL"
                            ? "error"
                            : String(v.risk_score).toUpperCase() === "HIGH"
                              ? "warning"
                              : "success"
                        }
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>

      <Grid container spacing={2}>
        <Grid item xs={12} md={6}>
          <Card sx={{ background: "#ffffff", border: "1px solid #d9efe2", minHeight: 260 }}>
            <CardContent>
              <Typography variant="h6" sx={{ color: "#2e7d32", mb: 1, fontWeight: 700 }}>
                2) Happy Paths
              </Typography>
              {pathsLoading ? (
                <CircularProgress size={20} />
              ) : (
                <Stack spacing={1}>
                  {(vendorPaths.happy_paths || []).length === 0 && (
                    <Typography variant="body2" sx={{ color: "#6b7280" }}>
                      No happy path variants returned for this vendor.
                    </Typography>
                  )}
                  {(vendorPaths.happy_paths || []).map((p, i) => (
                    <Card key={`happy-${i}`} sx={{ background: "#f7fcf9", border: "1px solid #ddf0e3" }}>
                      <CardContent sx={{ p: 1.2 }}>
                        <Typography variant="body2" sx={{ color: "#1f2937" }}>
                          {p.path || p.variant || "Variant"}
                        </Typography>
                        <Typography variant="caption" sx={{ color: "#2e7d32", display: "block" }}>
                          Freq: {p.frequency ?? p.count ?? 0} | {pct(p.percentage)} | Avg: {Number(p.avg_duration_days || p.avg_duration || 0).toFixed(2)} days
                        </Typography>
                      </CardContent>
                    </Card>
                  ))}
                </Stack>
              )}
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card sx={{ background: "#ffffff", border: "1px solid #f1d7d7", minHeight: 260 }}>
            <CardContent>
              <Typography variant="h6" sx={{ color: "#d32f2f", mb: 1, fontWeight: 700 }}>
                2) Exception Paths
              </Typography>
              {pathsLoading ? (
                <CircularProgress size={20} />
              ) : (
                <Stack spacing={1}>
                  {(vendorPaths.exception_paths || []).length === 0 && (
                    <Typography variant="body2" sx={{ color: "#6b7280" }}>
                      No exception path variants returned for this vendor.
                    </Typography>
                  )}
                  {(vendorPaths.exception_paths || []).map((p, i) => {
                    const type = String(p.exception_type || p.type || "").toLowerCase();
                    const emoji = type.includes("payment")
                      ? "🔴"
                      : type.includes("invoice")
                        ? "🟠"
                        : type.includes("short")
                          ? "🟡"
                          : "🔵";
                    return (
                      <Card key={`exc-${i}`} sx={{ background: "#fff8f8", border: "1px solid #f3d6d6" }}>
                        <CardContent sx={{ p: 1.2 }}>
                          <Typography variant="body2" sx={{ color: "#1f2937" }}>
                            {emoji} {p.path || p.variant || "Exception Variant"}
                          </Typography>
                          <Typography variant="caption" sx={{ color: "#d32f2f", display: "block" }}>
                            Type: {p.exception_type || "unknown"} | Freq: {p.frequency ?? p.count ?? 0} | {pct(p.percentage)}
                          </Typography>
                          {p.avg_dpo || p.dpo ? (
                            <Typography variant="caption" sx={{ color: "#ed6c02", display: "block" }}>
                              DPO: {Number(p.avg_dpo || p.dpo || 0).toFixed(2)} days
                            </Typography>
                          ) : null}
                        </CardContent>
                      </Card>
                    );
                  })}
                </Stack>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Card sx={{ background: "#ffffff", border: "1px solid #e5e7eb", mt: 2 }}>
        <CardContent>
          <Typography variant="h6" sx={{ color: "#1f2937", mb: 1, fontWeight: 700 }}>
            3) Exception Breakdown for {selectedVendorId}
          </Typography>
          <Grid container spacing={1.5}>
            {EXCEPTION_META.map((meta) => {
              const data = exceptionBreakdown?.[meta.key] || {};
              return (
                <Grid key={meta.key} item xs={12} md={6} lg={3}>
                  <Card sx={{ background: "#ffffff", border: `1px solid ${meta.color}55`, height: "100%" }}>
                    <CardContent sx={{ p: 1.2 }}>
                      <Typography variant="subtitle2" sx={{ color: meta.color, mb: 0.6 }}>
                        {meta.title}
                      </Typography>
                      <Typography variant="body2" sx={{ color: "#374151" }}>
                        Count: {data.count ?? 0}
                      </Typography>
                      <Typography variant="body2" sx={{ color: "#374151" }}>
                        Value: {currency(data.value || data.optimization_value || 0)}
                      </Typography>
                      <Typography variant="body2" sx={{ color: "#374151" }}>
                        % of Vendor: {pct(data.percentage)}
                      </Typography>
                      {data.avg_dpo !== undefined ? (
                        <Typography variant="body2" sx={{ color: "#374151" }}>
                          DPO: {Number(data.avg_dpo || 0).toFixed(2)} days
                        </Typography>
                      ) : null}
                      {data.time_stuck_days !== undefined ? (
                        <Typography variant="body2" sx={{ color: "#374151" }}>
                          Time Stuck: {Number(data.time_stuck_days || 0).toFixed(1)} days
                        </Typography>
                      ) : null}
                      {data.risk_level ? (
                        <Typography variant="body2" sx={{ color: "#374151" }}>
                          Risk: {data.risk_level}
                        </Typography>
                      ) : null}
                    </CardContent>
                  </Card>
                </Grid>
              );
            })}
          </Grid>
        </CardContent>
      </Card>

      <Card sx={{ background: "#ffffff", border: "1px solid #e5e7eb", mt: 2 }}>
        <CardContent>
          <Typography variant="h6" sx={{ color: "#1f2937", mb: 1, fontWeight: 700 }}>
            4) AI Analysis
          </Typography>
          <Button variant="contained" onClick={runAiAnalysis} disabled={aiLoading || !selectedVendor}>
            {aiLoading ? "Analyzing..." : "Run Vendor AI Analysis"}
          </Button>
          {aiLoading && <CircularProgress size={20} sx={{ ml: 1 }} />}
          <Divider sx={{ my: 1.2, borderColor: "#e5e7eb" }} />
          {aiResult ? (
            <Box>
              <Typography variant="subtitle2" sx={{ color: "#1976d2", fontWeight: 700 }}>Vendor Risk Assessment</Typography>
              <Typography variant="body2" sx={{ color: "#374151", mb: 0.8 }}>
                Risk Score: {aiResult?.vendor_analysis?.vendor_risk_score || "N/A"}
              </Typography>
              <Typography variant="subtitle2" sx={{ color: "#1976d2", fontWeight: 700 }}>AI Recommendations</Typography>
              <Stack sx={{ mb: 0.8 }}>
                {(aiResult.ai_recommendations || []).map((r, i) => (
                  <Typography key={i} variant="body2" sx={{ color: "#374151" }}>
                    • {r}
                  </Typography>
                ))}
              </Stack>
              {aiResult.celonis_evidence ? (
                <Chip label={`Celonis Evidence: ${aiResult.celonis_evidence}`} sx={{ background: "#dcfce7", color: "#166534" }} />
              ) : null}
              <details style={{ marginTop: 10 }}>
                <summary style={{ cursor: "pointer", color: "#1976d2" }}>Full AI JSON</summary>
                <pre className="json-display">{JSON.stringify(aiResult, null, 2)}</pre>
              </details>
            </Box>
          ) : (
            <Typography variant="body2" sx={{ color: "#6b7280" }}>
              Click the button to get AI root-cause, preventive/corrective/escalation recommendations, comparison to average, and financial impact.
            </Typography>
          )}
        </CardContent>
      </Card>

      <Card sx={{ background: "#ffffff", border: "1px solid #e5e7eb", mt: 2, mb: 2 }}>
        <CardContent>
          <Typography variant="h6" sx={{ color: "#1f2937", mb: 1, fontWeight: 700 }}>
            5) Payment Behavior for {selectedVendorId}
          </Typography>
          <PaymentBehaviorPie behavior={behavior} />
        </CardContent>
      </Card>
    </Box>
  );
}
