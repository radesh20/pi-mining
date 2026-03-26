import React, { useEffect, useState } from "react";
import { Alert, Box, Card, CardContent, Grid, Stack, Typography } from "@mui/material";
import LoadingSpinner from "../components/LoadingSpinner";
import { fetchAgentDeepDive } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";
const AGENT_NAME = "Invoice Exception Agent";
const asList = (v, fb = []) => Array.isArray(v) ? v : v ? [v] : fb;

const COLUMN_STYLES = [
  { label: "Workflow Prompts", color: "#1E4E8C", bg: "#EBF2FC", border: "#90B8E8", dotColor: "#1E4E8C" },
  { label: "Decision Logic", color: "#A05A10", bg: "#FEF3DC", border: "#F0C870", dotColor: "#A05A10" },
  { label: "Guardrails", color: "#B03030", bg: "#FAEAEA", border: "#E0A0A0", dotColor: "#B03030" },
];

const DETAIL_ROWS = [
  { key: "pi_evidence_used", label: "PI Evidence Used", color: "#1A6B5E" },
  { key: "timing_decision", label: "Timing Decision", color: "#1E4E8C" },
  { key: "action_impact", label: "Action Impact", color: "#5C5650" },
  { key: "why_bi_only_misses_this", label: "Why BI-only would miss this", color: "#B03030" },
];

function PromptBlock({ item, colStyle }) {
  const text = item.prompt || item.instruction || item.rule || item.action || "N/A";
  return (
    <Box sx={{ mb: 1.5, p: 1.5, background: colStyle.bg, border: `1px solid ${colStyle.border}`, borderRadius: "10px", borderLeft: `3px solid ${colStyle.color}` }}>
      <Typography sx={{ fontSize: "0.82rem", color: "#17140F", fontFamily: G, mb: 1, lineHeight: 1.5 }}>{text}</Typography>
      {DETAIL_ROWS.map(({ key, label, color }) => {
        const val = item[key] || item.source || "N/A";
        if (!item[key] && key !== "pi_evidence_used") return null;
        return (
          <Box key={key} sx={{ mb: 0.5 }}>
            <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color, opacity: 0.8, fontFamily: G }}>{label}</Typography>
            <Typography sx={{ fontSize: "0.72rem", color: "#5C5650", fontFamily: G }}>{val}</Typography>
          </Box>
        );
      })}
    </Box>
  );
}

export default function AgentDeepDive() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [promptData, setPromptData] = useState(null);

  useEffect(() => {
    fetchAgentDeepDive(AGENT_NAME)
      .then((res) => setPromptData(res.data || res))
      .catch((e) => setError(e.response?.data?.detail || e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <LoadingSpinner message="Loading deep dive prompts..." />;

  const workflow = asList(promptData?.workflow_prompts?.steps || []);
  const decision = asList(promptData?.decision_logic || []);
  const guardrails = asList(promptData?.guardrails || []);
  const cs = promptData?.critical_scenario;

  return (
    <div className="page-container">
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.5 }}>
          Agent Deep Dive
        </Typography>
        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
          {AGENT_NAME} — prompt design for core exception handling workflows.
        </Typography>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      {promptData?.pi_superiority_summary && (
        <Card sx={{ mb: 2.5, borderLeft: "3px solid #1A6B5E !important", background: "#F7FBF9 !important" }}>
          <CardContent>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1A6B5E", fontFamily: G, mb: 0.6 }}>PI Superiority Summary</Typography>
            <Typography sx={{ fontSize: "0.875rem", color: "#5C5650", fontFamily: G }}>{promptData.pi_superiority_summary}</Typography>
          </CardContent>
        </Card>
      )}

      {/* Critical Scenario */}
      {cs && (
        <Card sx={{ mb: 2.5, background: "#EBF2FC !important", border: "1px solid #90B8E8 !important", borderLeft: "3px solid #1E4E8C !important" }}>
          <CardContent>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1E4E8C", fontFamily: G, mb: 0.8 }}>Leadership Scenario</Typography>
            <Typography sx={{ fontSize: "0.875rem", color: "#17140F", fontFamily: G, mb: 0.8 }}>{cs.scenario}</Typography>
            <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap", mb: 0.8 }}>
              {[`Due in ${cs.days_until_due}d`, `Historical: ${cs.historical_processing_days}d`].map(t => (
                <Box key={t} sx={{ background: "#D0E4F8", px: 1.2, py: 0.2, borderRadius: "99px" }}>
                  <Typography sx={{ fontSize: "0.7rem", fontWeight: 600, color: "#1E4E8C", fontFamily: G }}>{t}</Typography>
                </Box>
              ))}
            </Box>
            <Typography sx={{ fontSize: "0.78rem", color: "#1E4E8C", fontFamily: G, mb: 0.4 }}><strong>Recommendation:</strong> {cs.recommended_action}</Typography>
            <Typography sx={{ fontSize: "0.75rem", color: "#B03030", fontFamily: G }}>{cs.why_pi_is_better}</Typography>
          </CardContent>
        </Card>
      )}

      {/* Three columns */}
      <Grid container spacing={2}>
        {[workflow, decision, guardrails].map((items, colIdx) => {
          const colStyle = COLUMN_STYLES[colIdx];
          return (
            <Grid item xs={12} md={4} key={colStyle.label}>
              {/* Column header */}
              <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1.5, pb: 1, borderBottom: `2px solid ${colStyle.border}` }}>
                <Box sx={{ width: "8px", height: "8px", borderRadius: "50%", background: colStyle.dotColor, flexShrink: 0 }} />
                <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: colStyle.color, fontFamily: G }}>{colStyle.label}</Typography>
                <Box sx={{ background: colStyle.bg, border: `1px solid ${colStyle.border}`, px: 0.8, py: 0.1, borderRadius: "99px", ml: "auto" }}>
                  <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: colStyle.color, fontFamily: G }}>{items.length}</Typography>
                </Box>
              </Box>
              {items.length === 0 ? (
                <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G, textAlign: "center", py: 3 }}>No {colStyle.label.toLowerCase()} defined.</Typography>
              ) : (
                items.map((item, i) => <PromptBlock key={i} item={item} colStyle={colStyle} />)
              )}
            </Grid>
          );
        })}
      </Grid>
    </div>
  );
}