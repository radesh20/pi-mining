import React from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

const G = "'Geist', system-ui, sans-serif";

export default function ProcessSignalsPanel({ currentStage, observedDuration, percentile75, unit = "days" }) {
  const abovePercentile = observedDuration != null && percentile75 != null && Number(observedDuration) > Number(percentile75);

  return (
    <Box sx={{ mb: 0.8, p: 1, background: "#DCF0EB", borderRadius: "6px" }}>
      <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#1A6B5E", fontFamily: G, mb: 0.6, opacity: 0.9 }}>
        Process Signals
      </Typography>
      {currentStage && (
        <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.4 }}>
          <Typography sx={{ fontSize: "0.72rem", color: "#1A6B5E", fontFamily: G, opacity: 0.85 }}>Current Stage</Typography>
          <Typography sx={{ fontSize: "0.72rem", color: "#17140F", fontWeight: 600, fontFamily: G }}>{currentStage}</Typography>
        </Box>
      )}
      {observedDuration != null && (
        <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.4 }}>
          <Typography sx={{ fontSize: "0.72rem", color: "#1A6B5E", fontFamily: G, opacity: 0.85 }}>Observed Process Duration</Typography>
          <Typography sx={{ fontSize: "0.72rem", color: "#17140F", fontWeight: 600, fontFamily: G }}>
            {observedDuration} {unit}
          </Typography>
        </Box>
      )}
      {percentile75 != null && (
        <Box sx={{ display: "flex", justifyContent: "space-between", mb: abovePercentile ? 0.6 : 0 }}>
          <Typography sx={{ fontSize: "0.72rem", color: "#1A6B5E", fontFamily: G, opacity: 0.85 }}>75th Percentile</Typography>
          <Typography sx={{ fontSize: "0.72rem", color: "#17140F", fontWeight: 600, fontFamily: G }}>
            {percentile75} {unit}
          </Typography>
        </Box>
      )}
      {abovePercentile && (
        <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, background: "#FEF3DC", border: "1px solid #F0C870", px: 0.8, py: 0.2, borderRadius: "99px" }}>
          <Box sx={{ width: 6, height: 6, borderRadius: "50%", background: "#A05A10" }} />
          <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: "#A05A10", fontFamily: G }}>Above 75th percentile</Typography>
        </Box>
      )}
    </Box>
  );
}
