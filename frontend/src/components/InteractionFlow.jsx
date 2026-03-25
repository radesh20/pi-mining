import React from "react";
import {
  Box,
  Card,
  CardContent,
  Chip,
  Stack,
  Typography,
} from "@mui/material";

const AGENT_STYLES = [
  { match: "vendor intelligence", color: "#9C27B0", icon: "🧠" },
  { match: "prompt writer", color: "#2196F3", icon: "✍️" },
  { match: "automation policy", color: "#FF9800", icon: "🛡️" },
  { match: "invoice processing", color: "#6C63FF", icon: "📄" },
  { match: "exception", color: "#ff5252", icon: "⚠️" },
  { match: "human", color: "#00D4AA", icon: "👤" },
];

const getAgentStyle = (agentName) => {
  const name = String(agentName || "").toLowerCase();
  const found = AGENT_STYLES.find((s) => name.includes(s.match));
  return found || { color: "#9aa6ff", icon: "🔹" };
};

const getStepOutput = (step) => {
  if (step?.full_output) return step.full_output;
  if (step?.output) return step.output;
  return {};
};

export default function InteractionFlow({ executionTrace }) {
  if (!executionTrace) return null;
  const steps = executionTrace.steps || [];
  const handoffs = executionTrace.handoff_messages || [];

  return (
    <Box>
      {steps.map((step, index) => {
        const style = getAgentStyle(step.agent);
        const stepOutput = getStepOutput(step);
        const handoff = handoffs[index];

        return (
          <React.Fragment key={`${step.step_number || index}-${step.agent}`}>
            <Card
              sx={{
                background: "#ffffff",
                border: `1px solid ${style.color}33`,
                borderLeft: `5px solid ${style.color}`,
                mb: 1.25,
                boxShadow: "0 2px 8px rgba(15, 23, 42, 0.06)",
              }}
            >
              <CardContent>
                <Stack direction="row" justifyContent="space-between" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                  <Typography variant="subtitle1" sx={{ color: style.color, fontWeight: 700 }}>
                    {style.icon} Step {step.step_number}: {step.agent}
                  </Typography>
                  {step.action && <Chip label={step.action} size="small" variant="outlined" />}
                </Stack>

                {step.celonis_evidence_used && (
                  <Chip
                    label={`Celonis: ${step.celonis_evidence_used}`}
                    size="small"
                    sx={{ mb: 1, background: "#dcfce7", color: "#166534" }}
                  />
                )}

                {step.financial_impact && (
                  <Typography variant="caption" sx={{ display: "block", color: "#ed6c02", mb: 0.6 }}>
                    Financial Impact: {String(step.financial_impact)}
                  </Typography>
                )}

                {step.duration_estimate && (
                  <Typography variant="caption" sx={{ display: "block", color: "#6b7280", mb: 0.6 }}>
                    Duration Estimate: {step.duration_estimate}
                  </Typography>
                )}

                {typeof step.expected_turnaround_days !== "undefined" && (
                  <Typography variant="caption" sx={{ display: "block", color: "#1d4ed8", mb: 0.5 }}>
                    Expected Turnaround (PI): {step.expected_turnaround_days} days
                  </Typography>
                )}

                {typeof step.days_until_due !== "undefined" && (
                  <Typography variant="caption" sx={{ display: "block", color: "#0f766e", mb: 0.5 }}>
                    Days Until Due: {step.days_until_due}
                  </Typography>
                )}

                {step.urgency_decision && (
                  <Typography variant="caption" sx={{ display: "block", color: "#b45309", mb: 0.5 }}>
                    Urgency Decision: {step.urgency_decision}
                  </Typography>
                )}

                {step.payload_field_justification_from_pi && (
                  <Typography variant="caption" sx={{ display: "block", color: "#7c2d12", mb: 0.6 }}>
                    PI Payload Justification: {step.payload_field_justification_from_pi}
                  </Typography>
                )}

                {step.error && (
                  <Typography variant="caption" sx={{ display: "block", color: "#d32f2f", mb: 0.8 }}>
                    Error: {step.error}
                  </Typography>
                )}

                {step.input_summary && (
                  <Typography variant="body2" sx={{ color: "#374151", mb: 0.4 }}>
                    Input: {step.input_summary}
                  </Typography>
                )}
                {step.output_summary && (
                  <Typography variant="body2" sx={{ color: "#374151", mb: 0.8 }}>
                    Output: {step.output_summary}
                  </Typography>
                )}

                <details>
                  <summary style={{ cursor: "pointer", color: "#1976d2" }}>Input JSON</summary>
                  <pre className="json-display">{JSON.stringify(step.input || {}, null, 2)}</pre>
                </details>
                <details style={{ marginTop: 6 }}>
                  <summary style={{ cursor: "pointer", color: "#1976d2" }}>Output JSON</summary>
                  <pre className="json-display">{JSON.stringify(stepOutput, null, 2)}</pre>
                </details>
              </CardContent>
            </Card>

            {handoff && (
              <Box sx={{ textAlign: "center", mb: 1.25 }}>
                <Typography sx={{ color: "#94a3b8", fontSize: 16, lineHeight: 1 }}>↓</Typography>
                <Chip
                  size="small"
                  label={`${handoff.from_agent} → ${handoff.to_agent} | ${handoff.message_type}`}
                  sx={{ background: "#eff6ff", color: "#1d4ed8", mb: 0.5, border: "1px solid #bfdbfe" }}
                />
                {handoff.payload_summary && (
                  <Typography variant="caption" sx={{ display: "block", color: "#64748b" }}>
                    {handoff.payload_summary}
                  </Typography>
                )}
                {handoff.detected_process_step && (
                  <Typography variant="caption" sx={{ display: "block", color: "#0f172a" }}>
                    Process Step: {handoff.detected_process_step}
                  </Typography>
                )}
                {typeof handoff.expected_turnaround_days !== "undefined" && (
                  <Typography variant="caption" sx={{ display: "block", color: "#1d4ed8" }}>
                    Historical Turnaround: {handoff.expected_turnaround_days} days
                  </Typography>
                )}
                {typeof handoff.days_until_due !== "undefined" && (
                  <Typography variant="caption" sx={{ display: "block", color: "#0f766e" }}>
                    Due-Date Buffer: {handoff.days_until_due} days
                  </Typography>
                )}
                {handoff.urgency_decision && (
                  <Typography variant="caption" sx={{ display: "block", color: "#b45309" }}>
                    Urgency: {handoff.urgency_decision}
                  </Typography>
                )}
                {handoff.pi_payload_justification && (
                  <Typography variant="caption" sx={{ display: "block", color: "#7c2d12" }}>
                    PI Rationale: {handoff.pi_payload_justification}
                  </Typography>
                )}
              </Box>
            )}
          </React.Fragment>
        );
      })}
    </Box>
  );
}
