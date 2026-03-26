import React, { useEffect, useState } from "react";
import { Box, Card, CardContent, Chip, Grid, Typography, Stack } from "@mui/material";
import AgentCard from "../components/AgentCard";
import ProcessMetrics from "../components/ProcessMetrics";
import LoadingSpinner from "../components/LoadingSpinner";
import { fetchProcessAgents } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

function SectionHeader({ num, label, meta }) {
  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 2, mt: 3.5 }}>
      {num && <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, color: "#9C9690", fontFamily: G, flexShrink: 0 }}>{num}</Typography>}
      <Typography sx={{ fontFamily: S, fontSize: "1.35rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.015em", whiteSpace: "nowrap" }}>
        {label}
      </Typography>
      {meta && <Box sx={{ background: "#F0EDE6", border: "1px solid #E8E3DA", borderRadius: "99px", px: 1.2, py: 0.2 }}>
        <Typography sx={{ fontSize: "0.69rem", fontWeight: 600, color: "#9C9690", fontFamily: G }}>{meta}</Typography>
      </Box>}
      <Box sx={{ flex: 1, height: "1px", background: "#E8E3DA" }} />
    </Box>
  );
}

export default function ProcessAgentsView() {
  const [agents, setAgents] = useState(null);
  const [context, setContext] = useState(null);
  const [lifecycleMap, setLifecycleMap] = useState([]);
  const [piVsBiMessage, setPiVsBiMessage] = useState("");
  const [criticalScenario, setCriticalScenario] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchProcessAgents()
      .then((res) => {
        setAgents(res.data?.recommended_agents || []);
        setLifecycleMap(res.data?.lifecycle_map || []);
        setPiVsBiMessage(res.data?.pi_vs_bi_message || "");
        setCriticalScenario(res.data?.critical_timing_scenario || null);
        setContext(res.process_context);
      })
      .catch((e) => setError(e.response?.data?.detail || e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <LoadingSpinner message="Analyzing Celonis data and recommending agents via Azure OpenAI..." />;

  if (error) return (
    <div className="page-container">
      <Box sx={{ pt: 4 }}>
        <div className="error-box">
          <Typography sx={{ fontFamily: S, fontSize: "1.1rem", mb: 0.5 }}>Error</Typography>
          <Typography sx={{ fontSize: "0.875rem" }}>{error}</Typography>
        </div>
      </Box>
    </div>
  );

  return (
    <div className="page-container">
      {/* Header */}
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.5 }}>
          Process Agents View
        </Typography>
        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
          Agents recommended from Celonis process mining data
        </Typography>
      </Box>

      {/* Why PI not BI */}
      {piVsBiMessage && (
        <Card sx={{ mb: 2.5, borderLeft: "3px solid #B5742A !important" }}>
          <CardContent>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.8 }}>Why PI, not BI-only</Typography>
            <Typography sx={{ fontSize: "0.875rem", color: "#5C5650", fontFamily: G }}>{piVsBiMessage}</Typography>
          </CardContent>
        </Card>
      )}

      {/* Critical Timing Scenario */}
      {criticalScenario && (
        <Card sx={{ mb: 2.5, background: "#EBF2FC !important", border: "1px solid #90B8E8 !important", borderLeft: "3px solid #1E4E8C !important" }}>
          <CardContent>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1E4E8C", fontFamily: G, mb: 0.8 }}>Critical Timing Scenario</Typography>
            <Typography sx={{ fontSize: "0.875rem", color: "#17140F", fontFamily: G, mb: 1 }}>{criticalScenario.scenario}</Typography>
            <Stack direction="row" spacing={1} flexWrap="wrap" gap={0.8} sx={{ mb: 0.8 }}>
              <Box sx={{ background: "#D0E4F8", border: "1px solid #90B8E8", px: 1.2, py: 0.3, borderRadius: "99px" }}>
                <Typography sx={{ fontSize: "0.7rem", fontWeight: 600, color: "#1E4E8C", fontFamily: G }}>Due in {criticalScenario.days_until_due}d</Typography>
              </Box>
              <Box sx={{ background: "#D0E4F8", border: "1px solid #90B8E8", px: 1.2, py: 0.3, borderRadius: "99px" }}>
                <Typography sx={{ fontSize: "0.7rem", fontWeight: 600, color: "#1E4E8C", fontFamily: G }}>Historical path {criticalScenario.historical_processing_days}d</Typography>
              </Box>
              {criticalScenario.pi_recommendation && (
                <Box sx={{ background: "#F5ECD9", border: "1px solid #DEC48A", px: 1.2, py: 0.3, borderRadius: "99px" }}>
                  <Typography sx={{ fontSize: "0.7rem", fontWeight: 600, color: "#B5742A", fontFamily: G }}>{criticalScenario.pi_recommendation}</Typography>
                </Box>
              )}
            </Stack>
            <Typography sx={{ fontSize: "0.75rem", color: "#2E5090", fontFamily: G }}>{criticalScenario.why_bi_only_misses_this}</Typography>
          </CardContent>
        </Card>
      )}

      {context && <ProcessMetrics context={context} />}

      {/* P2P Lifecycle Map */}
      {lifecycleMap.length > 0 && (
        <>
          <SectionHeader label="P2P Lifecycle Map" meta="Top Process Agents" />
          <Grid container spacing={2} sx={{ mb: 2 }}>
            {lifecycleMap.map((item) => (
              <Grid key={item.lifecycle_stage} item xs={12} md={6}>
                <Card sx={{ height: "100%" }}>
                  <CardContent>
                    <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", mb: 1.2 }}>
                      <Box sx={{ background: "#F0EDE6", border: "1px solid #E8E3DA", px: 1, py: 0.3, borderRadius: "6px" }}>
                        <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#9C9690", fontFamily: G }}>{item.lifecycle_stage}</Typography>
                      </Box>
                    </Box>
                    <Typography sx={{ fontWeight: 600, fontSize: "0.875rem", color: "#17140F", fontFamily: G, mb: 1 }}>{item.agent_name}</Typography>
                    {[
                      { label: "PI Evidence", value: item.pi_evidence, color: "#1A6B5E", bg: "#DCF0EB" },
                      { label: "Timing Decision", value: item.timing_decision, color: "#1E4E8C", bg: "#EBF2FC" },
                      { label: "Action Impact", value: item.action_impact, color: "#5C5650", bg: "#F0EDE6" },
                      { label: "Why BI misses this", value: item.why_bi_only_misses_this, color: "#B03030", bg: "#FAEAEA" },
                    ].map(({ label, value, color, bg }) => (
                      <Box key={label} sx={{ mb: 0.8, p: 0.8, background: bg, borderRadius: "6px" }}>
                        <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color, fontFamily: G, mb: 0.2, opacity: 0.8 }}>{label}</Typography>
                        <Typography sx={{ fontSize: "0.75rem", color: "#5C5650", fontFamily: G }}>{value}</Typography>
                      </Box>
                    ))}
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        </>
      )}

      <SectionHeader label="Recommended Process Agents" meta={agents ? `${agents.length} agents` : undefined} />
      <div className="card-grid">
        {agents && agents.map((agent, idx) => <AgentCard key={idx} agent={agent} />)}
      </div>
    </div>
  );
}