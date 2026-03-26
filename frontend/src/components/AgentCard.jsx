import React from "react";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import Box from "@mui/material/Box";

export default function AgentCard({ agent }) {
  const priorityColor =
    agent.priority === "HIGH"
      ? "error"
      : agent.priority === "MEDIUM"
      ? "warning"
      : "success";

  return (
    <Card
      sx={{
        borderRadius: 2,
        "&:hover": {
          borderColor: "#bfdbfe",
          boxShadow: "0 10px 24px rgba(15, 23, 42, 0.08)",
          transform: "translateY(-2px)",
        },
      }}
    >
      <CardContent>
        <Box sx={{ display: "flex", justifyContent: "space-between", mb: 1 }}>
          <Typography variant="h6" sx={{ fontWeight: 700, color: "#1f2937" }}>
            {agent.agent_name}
          </Typography>
          <Chip label={agent.priority} color={priorityColor} size="small" />
        </Box>
        <Typography variant="body2" sx={{ mb: 2, color: "#6b7280" }}>
          {agent.purpose}
        </Typography>
        {agent.lifecycle_stage && (
          <Typography variant="caption" sx={{ display: "block", mb: 0.8, color: "#1d4ed8", fontWeight: 700 }}>
            Lifecycle Stage: {agent.lifecycle_stage}
          </Typography>
        )}
        <Typography variant="caption" sx={{ fontWeight: 700, color: "#2e7d32" }}>
          Process Signals:
        </Typography>
        <Typography variant="body2" sx={{ mb: 2, color: "#374151", fontSize: "0.84rem" }}>
          {agent.process_mining_evidence}
        </Typography>
        {agent.timing_decision && (
          <Typography variant="body2" sx={{ mb: 1, color: "#1d4ed8", fontSize: "0.82rem" }}>
            <strong>Prediction:</strong> {agent.timing_decision}
          </Typography>
        )}
        {agent.action_impact && (
          <Typography variant="body2" sx={{ mb: 1, color: "#334155", fontSize: "0.82rem" }}>
            <strong>Decision:</strong> {agent.action_impact}
          </Typography>
        )}
        {agent.why_bi_only_misses_this && (
          <Typography variant="body2" sx={{ mb: 1.2, color: "#7c2d12", fontSize: "0.82rem" }}>
            <strong>Why This Action:</strong> {agent.why_bi_only_misses_this}
          </Typography>
        )}
        <Box sx={{ mb: 1 }}>
          <Typography variant="caption" sx={{ fontWeight: 700, color: "#374151" }}>
            Activities:
          </Typography>
          <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mt: 0.5 }}>
            {(agent.activities_covered || []).map((act, i) => (
              <span key={i} className="evidence-tag">{act}</span>
            ))}
          </Box>
        </Box>
        {agent.interacts_with && agent.interacts_with.length > 0 && (
          <Box>
            <Typography variant="caption" sx={{ fontWeight: 700, color: "#374151" }}>
              Interacts with:
            </Typography>
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mt: 0.5 }}>
              {agent.interacts_with.map((a, i) => (
                <Chip key={i} label={a} size="small" variant="outlined" />
              ))}
            </Box>
          </Box>
        )}
      </CardContent>
    </Card>
  );
}
