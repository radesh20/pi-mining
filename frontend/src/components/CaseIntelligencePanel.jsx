import React from "react";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Grid from "@mui/material/Grid";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

function breachColor(prob) {
  const v = Number(prob || 0);
  if (v > 70) return { bg: "#FAEAEA", border: "#E0A0A0", text: "#B03030" };
  if (v >= 40) return { bg: "#FEF3DC", border: "#F0C870", text: "#A05A10" };
  return { bg: "#DCF0EB", border: "#8FCFC5", text: "#1A6B5E" };
}

function KVRow({ label, value }) {
  return (
    <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", py: 0.6, borderBottom: "1px solid #F0EDE6" }}>
      <Typography sx={{ fontSize: "0.72rem", color: "#9C9690", fontFamily: G, flexShrink: 0, mr: 2 }}>{label}</Typography>
      <Typography sx={{ fontSize: "0.75rem", color: "#5C5650", fontFamily: G, fontWeight: 600, textAlign: "right" }}>{value || "N/A"}</Typography>
    </Box>
  );
}

// Accepts either real data props or falls back to realistic mock data
export default function CaseIntelligencePanel({ caseId, currentStage, timeInStage, historicalAvg, percentile75, remainingSla, predictedCompletion, breachProbability, rootCause }) {
  // Use provided props or fall back to realistic mock values
  const data = {
    caseId: caseId || "INV-20240291",
    currentStage: currentStage || "3-Way Match Review",
    timeInStage: timeInStage != null ? timeInStage : 5.2,
    historicalAvg: historicalAvg != null ? historicalAvg : 3.1,
    percentile75: percentile75 != null ? percentile75 : 4.8,
    remainingSla: remainingSla != null ? remainingSla : "2 days",
    predictedCompletion: predictedCompletion || "7.4 days",
    breachProbability: breachProbability != null ? breachProbability : 78,
    rootCause: rootCause || "Prolonged dwell at 3-Way Match exceeds 75th percentile; historically 82% of late invoices breach SLA from this stage.",
  };

  const bColors = breachColor(data.breachProbability);
  const abovePercentile = Number(data.timeInStage) > Number(data.percentile75);

  return (
    <Card sx={{ border: "1px solid #B8DFD0 !important", background: "#F2FAF6 !important" }}>
      <CardContent>
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.5 }}>
          <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#1A6B5E" }}>Case Intelligence</Typography>
          <Box sx={{ background: "#DCF0EB", border: "1px solid #8FCFC5", px: 1.2, py: 0.3, borderRadius: "99px" }}>
            <Typography sx={{ fontSize: "0.7rem", fontWeight: 700, color: "#1A6B5E", fontFamily: G }}>{data.caseId}</Typography>
          </Box>
        </Box>

        <Grid container spacing={2}>
          {/* Stage Timing */}
          <Grid item xs={12} sm={6}>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1A6B5E", fontFamily: G, mb: 0.8 }}>Stage Timing</Typography>
            <Stack spacing={0}>
              <KVRow label="Current Stage" value={data.currentStage} />
              <KVRow label="Time in Stage" value={`${data.timeInStage} days`} />
              <KVRow label="Historical Avg" value={`${data.historicalAvg} days`} />
              <KVRow label="75th Percentile" value={`${data.percentile75} days`} />
            </Stack>
            {abovePercentile && (
              <Box sx={{ mt: 0.8, display: "inline-flex", alignItems: "center", gap: 0.5, background: "#FEF3DC", border: "1px solid #F0C870", px: 0.8, py: 0.2, borderRadius: "99px" }}>
                <Box sx={{ width: 6, height: 6, borderRadius: "50%", background: "#A05A10" }} />
                <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: "#A05A10", fontFamily: G }}>Above 75th percentile</Typography>
              </Box>
            )}
          </Grid>

          {/* Predictions */}
          <Grid item xs={12} sm={6}>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1E4E8C", fontFamily: G, mb: 0.8 }}>Prediction</Typography>
            <Stack spacing={0}>
              <KVRow label="Remaining SLA" value={data.remainingSla} />
              <KVRow label="Predicted Completion" value={data.predictedCompletion} />
            </Stack>
            <Box sx={{ mt: 0.8, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <Typography sx={{ fontSize: "0.72rem", color: "#1E4E8C", fontFamily: G, opacity: 0.85 }}>Breach Probability</Typography>
              <Box sx={{ background: bColors.bg, border: `1px solid ${bColors.border}`, px: 0.8, py: 0.2, borderRadius: "99px" }}>
                <Typography sx={{ fontSize: "0.7rem", fontWeight: 700, color: bColors.text, fontFamily: G }}>{data.breachProbability}%</Typography>
              </Box>
            </Box>
            {Number(data.breachProbability) > 70 && (
              <Box sx={{ mt: 0.6, display: "inline-flex", alignItems: "center", gap: 0.5, background: "#FAEAEA", border: "1px solid #E0A0A0", px: 0.8, py: 0.2, borderRadius: "99px" }}>
                <Box sx={{ width: 6, height: 6, borderRadius: "50%", background: "#B03030" }} />
                <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: "#B03030", fontFamily: G }}>On Failure Trajectory</Typography>
              </Box>
            )}
          </Grid>

          {/* Root Cause */}
          <Grid item xs={12}>
            <Box sx={{ background: "#EBF2FC", border: "1px solid #90B8E8", borderLeft: "3px solid #1E4E8C", borderRadius: "0 8px 8px 0", p: 1.2 }}>
              <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#1E4E8C", fontFamily: G, mb: 0.4 }}>Root Cause Explanation</Typography>
              <Typography sx={{ fontSize: "0.75rem", color: "#2E5090", fontFamily: G }}>{data.rootCause}</Typography>
            </Box>
          </Grid>
        </Grid>
      </CardContent>
    </Card>
  );
}
