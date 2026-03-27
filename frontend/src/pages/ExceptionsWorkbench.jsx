import React, { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Alert, Box, Button, Card, CardContent, Chip, CircularProgress,
  Grid, Snackbar, Stack, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, Typography,
  TextField, MenuItem, Select, FormControl, InputLabel,
} from "@mui/material";
import {
  analyzeExceptionRecord, fetchExceptionCategories,
  fetchAllExceptionRecords, waitForCacheReady
} from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const money = (v) => {
  const n = Number(v || 0);
  if (!n) return "N/A";
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M $`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}K $`;
  return `${n.toFixed(0)} $`;
};
const pickData = (r) => { if (!r) return null; if (r.data !== undefined) return r.data; return r; };
const vendorDisplay = (rec) => rec?.vendor_name || rec?.vendor_id || rec?.recurring_vendor_hint || "—";

const RISK_STYLES = {
  CRITICAL: { bg: "#FAEAEA", color: "#B03030", border: "#E0A0A0", dot: "#C94040" },
  HIGH:     { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870", dot: "#C47020" },
  MEDIUM:   { bg: "#EBF2FC", color: "#1E4E8C", border: "#90B8E8", dot: "#2E6EBC" },
  LOW:      { bg: "#E0F0E8", color: "#1D5C3A", border: "#80C0A0", dot: "#2A7A50" },
};

function RiskBadge({ risk }) {
  const key = String(risk || "").toUpperCase();
  const s = RISK_STYLES[key] || RISK_STYLES.LOW;
  return (
    <Box sx={{
      display: "inline-flex", alignItems: "center", gap: 0.5,
      background: s.bg, color: s.color, border: `1px solid ${s.border}`,
      px: 1, py: 0.25, borderRadius: "99px",
    }}>
      <Box sx={{ width: 5, height: 5, borderRadius: "50%", background: s.dot, flexShrink: 0 }} />
      <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, fontFamily: G, letterSpacing: "0.05em" }}>
        {key}
      </Typography>
    </Box>
  );
}

export default function ExceptionsWorkbench() {
  const navigate = useNavigate();

  // ── Data state ──
  const [categories, setCategories] = useState([]);
  const [allRecords, setAllRecords] = useState([]);     // flat list across all categories
  const [loadingCategories, setLoadingCategories] = useState(true);
  const [loadingRecords, setLoadingRecords] = useState(false);
  const [error, setError] = useState("");
  const [toast, setToast] = useState({ open: false, message: "", severity: "success" });

  // ── Filter state ──
  const [filterRisk, setFilterRisk] = useState("ALL");
  const [filterCategory, setFilterCategory] = useState("ALL");
  const [filterVendor, setFilterVendor] = useState("");

  // ── Preview state (lightweight right-panel card) ──
  const [previewRecord, setPreviewRecord] = useState(null);
  const [previewAnalysis, setPreviewAnalysis] = useState(null);
  const [loadingPreview, setLoadingPreview] = useState(false);
  const previewRequestRef = useRef(0);

  const recordsRequestRef = useRef(0);

  // ── Load all categories then all records flat ──
  useEffect(() => {
    let active = true;
    const load = async (retry = true) => {
      try {
        const res = await fetchExceptionCategories();
        const cats = (Array.isArray(pickData(res)) ? pickData(res) : []).filter(
          (row) => Number(row.case_count || 0) > 0
        );
        if (retry && cats.length === 0) {
          await waitForCacheReady();
          return load(false);
        }
        if (!active) return;
        setCategories(cats);

        // load records once, then enrich them locally with category labels
        setLoadingRecords(true);
        const requestId = ++recordsRequestRef.current;
        const categoryMap = new Map(cats.map((cat) => [cat.category_id, cat]));
        const recordsRes = await fetchAllExceptionRecords();
        const rows = Array.isArray(pickData(recordsRes)) ? pickData(recordsRes) : [];
        if (!active || recordsRequestRef.current !== requestId) return;
        const flat = rows
          .map((row) => {
            const category = categoryMap.get(row.category_id) || null;
            return {
              ...row,
              category_label: row.category_label || category?.category_label || row.exception_type,
            };
          })
          .sort((a, b) => Number(b.invoice_amount || b.value_at_risk || 0) - Number(a.invoice_amount || a.value_at_risk || 0));
        setAllRecords(flat);
      } catch (e) {
        if (!active) return;
        setError(e?.response?.data?.detail || e.message || "Failed to load exceptions.");
      } finally {
        if (active) { setLoadingCategories(false); setLoadingRecords(false); }
      }
    };
    load();
    return () => { active = false; };
  }, []);

  // ── Derive risk from category ──
  const categoryRisk = (cat) => {
    if (cat.risk_level) return String(cat.risk_level).toUpperCase();
    const freq = Number(cat.frequency_percentage || 0);
    const count = Number(cat.case_count || 0);
    const score = freq > 0 ? freq : Math.min(100, count * 2);
    return score >= 60 ? "CRITICAL" : score >= 35 ? "HIGH" : score >= 15 ? "MEDIUM" : "LOW";
  };

  // ── Filtered queue ──
  const filteredRecords = useMemo(() => {
    return allRecords.filter((rec) => {
      if (filterRisk !== "ALL") {
        const risk = String(rec.risk_level || "MEDIUM").toUpperCase();
        if (risk !== filterRisk) return false;
      }
      if (filterCategory !== "ALL" && rec.category_id !== filterCategory) return false;
      if (filterVendor.trim()) {
        const v = vendorDisplay(rec).toLowerCase();
        if (!v.includes(filterVendor.trim().toLowerCase())) return false;
      }
      return true;
    });
  }, [allRecords, filterRisk, filterCategory, filterVendor]);

  // ── Category tile click = set category filter ──
  const handleCategoryTileClick = (cat) => {
    setFilterCategory((prev) => prev === cat.category_id ? "ALL" : cat.category_id);
    setPreviewRecord(null);
    setPreviewAnalysis(null);
  };

  // ── Row click = show lightweight preview card ──
  const handleRowClick = async (rec) => {
    setPreviewRecord(rec);
    setPreviewAnalysis(null);
    setLoadingPreview(true);
    const requestId = ++previewRequestRef.current;
    try {
      const payload = {
        exception_type: rec.category_label || rec.exception_type || "",
        exception_id: rec.exception_id,
        invoice_id: rec.invoice_id || rec.document_number || rec.case_id || "",
        vendor_id: rec.vendor_id || rec.recurring_vendor_hint || "",
        vendor_name: vendorDisplay(rec),
        invoice_amount: rec.invoice_amount || rec.value_at_risk || 0,
        currency: rec.currency || "USD",
        days_until_due: rec.days_until_due || 0,
        extra_context: rec,
      };
      const data = pickData(await analyzeExceptionRecord(payload));
      if (previewRequestRef.current !== requestId) return;
      setPreviewAnalysis(data);
    } catch {
      // preview best-effort — no error shown
    } finally {
      if (previewRequestRef.current === requestId) setLoadingPreview(false);
    }
  };

  // ── Navigate to Case Resolution with record pre-loaded ──
  const openInResolution = (rec) => {
    // Pass exception_id via query string so ExceptionIntelligence can pre-select it
    navigate(`/exception-intelligence?exception_id=${rec.exception_id}`);
  };

  const isLoading = loadingCategories || loadingRecords;

  return (
    <div className="page-container">
      {/* ── Page Header ── */}
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #ECEAE4", mb: 3 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.1rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.5 }}>
          Exception Triage
        </Typography>
        <Typography sx={{ fontSize: "0.85rem", color: "#9C9690", fontFamily: G }}>
          Identify, prioritize, and route exceptions for resolution. Click a row to preview, then open in Case Resolution.
        </Typography>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2, fontFamily: G }}>{error}</Alert>}

      {/* ── Persistent Filter Bar ── */}
      <Card sx={{ mb: 2.5, border: "1px solid #ECEAE4 !important" }}>
        <CardContent sx={{ pb: "14px !important" }}>
          <Grid container spacing={1.5} alignItems="center">
            <Grid item xs={12} sm={3}>
              <FormControl fullWidth size="small">
                <InputLabel sx={{ fontFamily: G, fontSize: "0.8rem" }}>Risk Level</InputLabel>
                <Select
                  value={filterRisk}
                  label="Risk Level"
                  onChange={(e) => setFilterRisk(e.target.value)}
                  sx={{ fontFamily: G, fontSize: "0.82rem" }}
                >
                  <MenuItem value="ALL">All Risks</MenuItem>
                  <MenuItem value="CRITICAL">Critical</MenuItem>
                  <MenuItem value="HIGH">High</MenuItem>
                  <MenuItem value="MEDIUM">Medium</MenuItem>
                  <MenuItem value="LOW">Low</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} sm={4}>
              <FormControl fullWidth size="small">
                <InputLabel sx={{ fontFamily: G, fontSize: "0.8rem" }}>Category</InputLabel>
                <Select
                  value={filterCategory}
                  label="Category"
                  onChange={(e) => setFilterCategory(e.target.value)}
                  sx={{ fontFamily: G, fontSize: "0.82rem" }}
                >
                  <MenuItem value="ALL">All Categories</MenuItem>
                  {categories.map((cat) => (
                    <MenuItem key={cat.category_id} value={cat.category_id}>
                      {cat.category_label || cat.category_id}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} sm={3}>
              <TextField
                fullWidth
                size="small"
                label="Vendor Search"
                value={filterVendor}
                onChange={(e) => setFilterVendor(e.target.value)}
                placeholder="Search vendor…"
                InputProps={{ sx: { fontFamily: G, fontSize: "0.82rem" } }}
                InputLabelProps={{ sx: { fontFamily: G, fontSize: "0.8rem" } }}
              />
            </Grid>
            <Grid item xs={12} sm={2}>
              <Button
                fullWidth
                variant="outlined"
                onClick={() => { setFilterRisk("ALL"); setFilterCategory("ALL"); setFilterVendor(""); }}
                sx={{ fontFamily: G, fontSize: "0.78rem", height: "40px" }}
              >
                Clear Filters
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* ── Category Tiles (filter shortcuts) ── */}
      {!loadingCategories && categories.length > 0 && (
        <Card sx={{ mb: 2.5, border: "1px solid #ECEAE4 !important" }}>
          <CardContent sx={{ pb: "12px !important" }}>
            <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: "#17140F", mb: 1.2 }}>
              Exception Categories
              <Typography component="span" sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G, ml: 1 }}>
                — click to filter queue
              </Typography>
            </Typography>
            <Grid container spacing={1}>
              {categories.map((cat) => {
                const label = cat.category_label || cat.category_id || "Category";
                const risk = categoryRisk(cat);
                const selected = filterCategory === cat.category_id;
                return (
                  <Grid item xs={12} sm={6} md={4} lg={3} key={cat.category_id}>
                    <Box
                      onClick={() => handleCategoryTileClick(cat)}
                      sx={{
                        p: 1.2, borderRadius: "10px", cursor: "pointer",
                        border: selected ? `1.5px solid #B5742A` : "1px solid #ECEAE4",
                        background: selected ? "#FBF5EA" : "#FDFCFA",
                        transition: "all 0.15s ease",
                        "&:hover": { background: selected ? "#FBF5EA" : "#F7F4EE", borderColor: selected ? "#B5742A" : "#C8C0B4" },
                      }}
                    >
                      <Typography sx={{ fontWeight: 600, fontSize: "0.78rem", color: "#17140F", mb: 0.4, fontFamily: G, lineHeight: 1.3 }}>
                        {label}
                      </Typography>
                      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                        <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G }}>
                          {cat.case_count || 0} cases · {money(cat.total_value)}
                        </Typography>
                        <RiskBadge risk={risk} />
                      </Box>
                    </Box>
                  </Grid>
                );
              })}
            </Grid>
          </CardContent>
        </Card>
      )}

      {/* ── Main two-column: Queue + Preview ── */}
      <Grid container spacing={2.5}>
        {/* ── LEFT: Exception Queue ── */}
        <Grid item xs={12} md={previewRecord ? 7 : 12}>
          <Card sx={{ border: "1px solid #ECEAE4 !important" }}>
            <CardContent sx={{ pb: "12px !important" }}>
              <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.5 }}>
                <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F" }}>
                  Exception Queue
                </Typography>
                <Box sx={{ display: "flex", gap: 1, alignItems: "center" }}>
                  {filteredRecords.length !== allRecords.length && (
                    <Chip
                      size="small"
                      label={`${filteredRecords.length} of ${allRecords.length}`}
                      sx={{ background: "#FEF3DC", color: "#A05A10", fontFamily: G, fontSize: "0.7rem", height: 20, border: "1px solid #F0C870" }}
                    />
                  )}
                  <Chip
                    size="small"
                    label={`${allRecords.length} total`}
                    sx={{ background: "#F0EDE6", color: "#6C6660", fontFamily: G, fontSize: "0.7rem", height: 20, border: "1px solid #E4E0D8" }}
                  />
                </Box>
              </Box>

              {isLoading ? (
                <Box sx={{ display: "flex", gap: 1.5, alignItems: "center", py: 3 }}>
                  <CircularProgress size={16} sx={{ color: "#B5742A" }} />
                  <Typography sx={{ fontSize: "0.8rem", color: "#9C9690", fontFamily: G }}>Loading exceptions…</Typography>
                </Box>
              ) : (
                <TableContainer sx={{ maxHeight: 560 }}>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        {["Invoice", "Category", "Vendor", "Amount", "DPO", "Risk"].map((h) => (
                          <TableCell
                            key={h}
                            sx={{ fontSize: "0.65rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: "#A09890", fontFamily: G, background: "#FDFCFA", borderBottom: "1px solid #ECEAE4", py: 0.8 }}
                          >
                            {h}
                          </TableCell>
                        ))}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {filteredRecords.map((rec) => {
                        const isSel = previewRecord?.exception_id === rec.exception_id;
                        const dpo = rec.avg_resolution_time_days ?? rec.days_in_exception ?? rec.avg_dpo ?? rec.dpo ?? 0;
                        return (
                          <TableRow
                            key={rec.exception_id}
                            hover
                            onClick={() => handleRowClick(rec)}
                            sx={{
                              cursor: "pointer",
                              background: isSel ? "#FBF5EA !important" : "transparent",
                              "&:hover": { background: isSel ? "#FBF5EA !important" : "#F7F4EE !important" },
                            }}
                          >
                            <TableCell sx={{ fontWeight: isSel ? 600 : 400, color: isSel ? "#B5742A" : "#17140F", fontFamily: G, fontSize: "0.77rem", borderBottom: "1px solid #F5F2EC" }}>
                              {rec.invoice_id || rec.exception_id}
                            </TableCell>
                            <TableCell sx={{ fontFamily: G, fontSize: "0.73rem", color: "#5C5650", borderBottom: "1px solid #F5F2EC", maxWidth: 140 }}>
                              <Typography sx={{ fontSize: "0.73rem", fontFamily: G, color: "#5C5650", lineHeight: 1.3, whiteSpace: "normal" }}>
                                {rec.category_label || rec.exception_type || "—"}
                              </Typography>
                            </TableCell>
                            <TableCell sx={{ fontFamily: G, fontSize: "0.75rem", color: "#5C5650", borderBottom: "1px solid #F5F2EC" }}>
                              {vendorDisplay(rec)}
                            </TableCell>
                            <TableCell sx={{ fontFamily: G, fontSize: "0.75rem", color: "#5C5650", borderBottom: "1px solid #F5F2EC" }}>
                              {money(rec.invoice_amount || rec.value_at_risk || 0)}
                            </TableCell>
                            <TableCell sx={{ fontFamily: G, fontSize: "0.75rem", color: "#5C5650", borderBottom: "1px solid #F5F2EC" }}>
                              {Number(dpo || 0).toFixed(1)}
                            </TableCell>
                            <TableCell sx={{ borderBottom: "1px solid #F5F2EC" }}>
                              <RiskBadge risk={rec.risk_level || "MEDIUM"} />
                            </TableCell>
                          </TableRow>
                        );
                      })}
                      {filteredRecords.length === 0 && (
                        <TableRow>
                          <TableCell colSpan={6} sx={{ borderBottom: "none", py: 3 }}>
                            <Typography sx={{ fontSize: "0.8rem", color: "#B0A898", fontFamily: G, textAlign: "center" }}>
                              {allRecords.length === 0 ? "No exception records available." : "No records match the current filters."}
                            </Typography>
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* ── RIGHT: Lightweight Preview Card ── */}
        {previewRecord && (
          <Grid item xs={12} md={5}>
            <Card sx={{ border: "1px solid #ECEAE4 !important", position: "sticky", top: "72px" }}>
              <CardContent>
                {/* Preview header */}
                <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", mb: 1.5 }}>
                  <Box sx={{ flex: 1, mr: 1 }}>
                    <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: "#17140F", mb: 0.3, lineHeight: 1.3 }}>
                      {previewRecord.category_label || previewRecord.exception_type || "Exception"}
                    </Typography>
                    <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G }}>
                      Invoice <strong style={{ color: "#5C5650" }}>{previewRecord.invoice_id || previewRecord.exception_id}</strong>
                      {" · "}
                      <strong style={{ color: "#5C5650" }}>{vendorDisplay(previewRecord)}</strong>
                    </Typography>
                  </Box>
                  <Box
                    onClick={() => { setPreviewRecord(null); setPreviewAnalysis(null); }}
                    sx={{ cursor: "pointer", color: "#C8C0B4", fontSize: "1.1rem", lineHeight: 1, mt: 0.2, userSelect: "none" }}
                  >
                    ✕
                  </Box>
                </Box>

                {/* Key chips */}
                <Stack direction="row" spacing={0.8} flexWrap="wrap" sx={{ mb: 1.5 }}>
                  <Chip
                    size="small"
                    label={money(previewRecord.invoice_amount || previewRecord.value_at_risk)}
                    sx={{ background: "#FEF3DC", color: "#A05A10", border: "1px solid #F0C870", fontFamily: G, fontSize: "0.7rem", height: 20 }}
                  />
                  <Chip
                    size="small"
                    label={`DPO ${Number(previewRecord.avg_resolution_time_days ?? previewRecord.dpo ?? 0).toFixed(1)}d`}
                    sx={{ background: "#EBF2FC", color: "#1E4E8C", border: "1px solid #90B8E8", fontFamily: G, fontSize: "0.7rem", height: 20 }}
                  />
                  <RiskBadge risk={previewRecord.risk_level || "MEDIUM"} />
                </Stack>

                {/* AI summary — one sentence */}
                {loadingPreview ? (
                  <Box sx={{ display: "flex", gap: 1.2, alignItems: "center", py: 1.5, background: "#F7F4EE", borderRadius: "8px", px: 1.5, mb: 1.5 }}>
                    <CircularProgress size={13} sx={{ color: "#B5742A" }} />
                    <Typography sx={{ fontSize: "0.77rem", color: "#9C9690", fontFamily: G }}>Fetching AI summary…</Typography>
                  </Box>
                ) : previewAnalysis?.summary ? (
                  <Box sx={{ background: "#F7F4EE", border: "1px solid #ECEAE4", borderRadius: "8px", p: 1.2, mb: 1.5 }}>
                    <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: "#A09890", fontFamily: G, mb: 0.4 }}>
                      AI Summary
                    </Typography>
                    <Typography sx={{ fontSize: "0.78rem", color: "#4C4840", fontFamily: G, lineHeight: 1.6 }}>
                      {previewAnalysis.summary}
                    </Typography>
                  </Box>
                ) : null}

                {/* Breach probability if available */}
                {previewAnalysis?.breach_probability != null && (
                  <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.5, px: 1.2, py: 0.8, background: Number(previewAnalysis.breach_probability) >= 70 ? "#FAEAEA" : "#FEF3DC", border: `1px solid ${Number(previewAnalysis.breach_probability) >= 70 ? "#E0A0A0" : "#F0C870"}`, borderRadius: "8px" }}>
                    <Typography sx={{ fontSize: "0.75rem", fontFamily: G, color: Number(previewAnalysis.breach_probability) >= 70 ? "#B03030" : "#A05A10" }}>
                      Breach Probability
                    </Typography>
                    <Typography sx={{ fontSize: "0.85rem", fontWeight: 700, fontFamily: G, color: Number(previewAnalysis.breach_probability) >= 70 ? "#B03030" : "#A05A10" }}>
                      {Number(previewAnalysis.breach_probability).toFixed(0)}%
                    </Typography>
                  </Box>
                )}

                {/* Root cause one-liner if available */}
                {previewAnalysis?.root_cause_analysis?.most_likely_cause && (
                  <Box sx={{ mb: 1.5, px: 1.2, py: 0.8, background: "#EBF2FC", border: "1px solid #90B8E8", borderLeft: "3px solid #1E4E8C", borderRadius: "0 8px 8px 0" }}>
                    <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#1E4E8C", fontFamily: G, mb: 0.3 }}>
                      Root Cause
                    </Typography>
                    <Typography sx={{ fontSize: "0.75rem", color: "#2A4A80", fontFamily: G, lineHeight: 1.55 }}>
                      {previewAnalysis.root_cause_analysis.most_likely_cause}
                    </Typography>
                  </Box>
                )}

                {/* Primary CTA */}
                <Button
                  fullWidth
                  variant="contained"
                  onClick={() => openInResolution(previewRecord)}
                  sx={{
                    background: "#1A6B5E !important",
                    fontFamily: G, fontSize: "0.82rem", fontWeight: 600,
                    borderRadius: "8px", textTransform: "none", py: 1,
                    "&:hover": { background: "#145A4E !important" },
                  }}
                >
                  Open in Case Resolution →
                </Button>
              </CardContent>
            </Card>
          </Grid>
        )}
      </Grid>

      <Snackbar
        open={toast.open}
        autoHideDuration={3500}
        onClose={() => setToast((t) => ({ ...t, open: false }))}
        anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
      >
        <Alert severity={toast.severity} onClose={() => setToast((t) => ({ ...t, open: false }))} sx={{ fontFamily: G }}>
          {toast.message}
        </Alert>
      </Snackbar>
    </div>
  );
}
