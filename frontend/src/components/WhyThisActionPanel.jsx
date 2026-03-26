import React from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

const G = "'Geist', system-ui, sans-serif";

export default function WhyThisActionPanel({ reason }) {
  return (
    <Box sx={{ mb: 0.8, p: 1, background: "#FAEAEA", borderRadius: "6px" }}>
      <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#B03030", fontFamily: G, mb: 0.4, opacity: 0.9 }}>
        Why This Action
      </Typography>
      <Typography sx={{ fontSize: "0.75rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>
        {reason || "Action based on observed process behavior"}
      </Typography>
      <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G, fontStyle: "italic" }}>
        Stage-level timing, transition patterns, historical outcomes — not static thresholds.
      </Typography>
    </Box>
  );
}
