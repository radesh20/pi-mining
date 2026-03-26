import React from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

const G = "'Geist', system-ui, sans-serif";

export default function DecisionPanel({ action }) {
  if (!action) return null;
  return (
    <Box sx={{ mb: 0.8, p: 1, background: "#F0EDE6", borderRadius: "6px" }}>
      <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#5C5650", fontFamily: G, mb: 0.4, opacity: 0.9 }}>
        Decision
      </Typography>
      <Typography sx={{ fontSize: "0.75rem", color: "#5C5650", fontFamily: G }}>{action}</Typography>
    </Box>
  );
}
