import React from "react";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import Box from "@mui/material/Box";

export default function GuardrailCard({ guardrail }) {
  const enforcementColor =
    guardrail.enforcement === "BLOCK" ? "error" : guardrail.enforcement === "ESCALATE" ? "warning" : "info";

  return (
    <Card
      sx={{
        borderRadius: 2,
        mb: 1,
        border: "1px solid #fecaca",
        background: "#fff7f7",
        "&:hover": { boxShadow: "0 8px 18px rgba(127, 29, 29, 0.12)" },
      }}
    >
      <CardContent sx={{ py: 1.5, "&:last-child": { pb: 1.5 } }}>
        <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.5 }}>
          <Typography variant="body2" sx={{ fontWeight: 700, color: "#b91c1c" }}>
            {guardrail.rule}
          </Typography>
          <Chip label={guardrail.enforcement} color={enforcementColor} size="small" />
        </Box>
        {guardrail.source && (
          <Typography variant="caption" sx={{ color: "#2e7d32" }}>
            📊 {guardrail.source}
          </Typography>
        )}
      </CardContent>
    </Card>
  );
}
