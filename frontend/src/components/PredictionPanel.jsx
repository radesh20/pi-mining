import React from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

const G = "'Geist', system-ui, sans-serif";

function breachColor(prob) {
  const v = Number(prob || 0);
  if (v > 70) return { bg: "#FAEAEA", border: "#E0A0A0", text: "#B03030" };
  if (v >= 40) return { bg: "#FEF3DC", border: "#F0C870", text: "#A05A10" };
  return { bg: "#DCF0EB", border: "#8FCFC5", text: "#1A6B5E" };
}

export default function PredictionPanel({ expectedCompletion, remainingSla, breachProbability, onFailureTrajectory }) {
  const bColors = breachColor(breachProbability);

  return (
    <Box sx={{ mb: 0.8, p: 1, background: "#EBF2FC", borderRadius: "6px" }}>
      <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#1E4E8C", fontFamily: G, mb: 0.6, opacity: 0.9 }}>
        Prediction
      </Typography>
      {expectedCompletion != null && (
        <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.4 }}>
          <Typography sx={{ fontSize: "0.72rem", color: "#1E4E8C", fontFamily: G, opacity: 0.85 }}>Expected Completion</Typography>
          <Typography sx={{ fontSize: "0.72rem", color: "#17140F", fontWeight: 600, fontFamily: G }}>{expectedCompletion}</Typography>
        </Box>
      )}
      {remainingSla != null && (
        <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.4 }}>
          <Typography sx={{ fontSize: "0.72rem", color: "#1E4E8C", fontFamily: G, opacity: 0.85 }}>Remaining SLA</Typography>
          <Typography sx={{ fontSize: "0.72rem", color: "#17140F", fontWeight: 600, fontFamily: G }}>{remainingSla}</Typography>
        </Box>
      )}
      {breachProbability != null && (
        <Box sx={{ display: "flex", justifyContent: "space-between", mb: onFailureTrajectory ? 0.6 : 0 }}>
          <Typography sx={{ fontSize: "0.72rem", color: "#1E4E8C", fontFamily: G, opacity: 0.85 }}>Breach Probability</Typography>
          <Box sx={{ background: bColors.bg, border: `1px solid ${bColors.border}`, px: 0.8, py: 0.1, borderRadius: "99px" }}>
            <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, color: bColors.text, fontFamily: G }}>{breachProbability}%</Typography>
          </Box>
        </Box>
      )}
      {onFailureTrajectory && (
        <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, background: "#FAEAEA", border: "1px solid #E0A0A0", px: 0.8, py: 0.2, borderRadius: "99px" }}>
          <Box sx={{ width: 6, height: 6, borderRadius: "50%", background: "#B03030" }} />
          <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: "#B03030", fontFamily: G }}>On Failure Trajectory</Typography>
        </Box>
      )}
    </Box>
  );
}
