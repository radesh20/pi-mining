import React from "react";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Grid from "@mui/material/Grid";
import Box from "@mui/material/Box";

function MetricCard({ label, value, unit, color = "#1976d2" }) {
  return (
    <Card
      sx={{
        borderRadius: 2,
        "&:hover": { boxShadow: "0 10px 24px rgba(15, 23, 42, 0.08)", transform: "translateY(-1px)" },
      }}
    >
      <CardContent sx={{ textAlign: "center" }}>
        <Typography variant="caption" sx={{ color: "#6b7280" }}>{label}</Typography>
        <Typography variant="h4" sx={{ fontWeight: 700, color, my: 0.5 }}>{value}</Typography>
        {unit && <Typography variant="caption" sx={{ color: "#6b7280" }}>{unit}</Typography>}
      </CardContent>
    </Card>
  );
}

export default function ProcessMetrics({ context }) {
  if (!context) return null;
  return (
    <Box sx={{ mb: 4 }}>
      <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
        <Typography variant="h4" sx={{ fontWeight: 700, color: "#1f2937" }}>
          Process Mining Insights
        </Typography>
        <span className="celonis-badge">Live from Celonis</span>
      </Box>
      <Grid container spacing={2}>
        <Grid item xs={6} md={3}>
          <MetricCard label="Total Cases" value={context.total_cases} />
        </Grid>
        <Grid item xs={6} md={3}>
          <MetricCard label="Total Events" value={context.total_events} />
        </Grid>
        <Grid item xs={6} md={3}>
          <MetricCard label="Avg E2E Duration" value={context.avg_end_to_end_days} unit="days" color="#2e7d32" />
        </Grid>
        <Grid item xs={6} md={3}>
          <MetricCard label="Exception Rate" value={`${context.exception_rate}%`} color="#d32f2f" />
        </Grid>
      </Grid>
      {context.bottleneck && (
        <Card sx={{ background: "#fff7ed", border: "1px solid #fed7aa", mt: 2, borderRadius: 2 }}>
          <CardContent>
            <Typography variant="subtitle2" sx={{ color: "#ed6c02", fontWeight: 700 }}>⚠️ Bottleneck Detected</Typography>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              {context.bottleneck.activity} — <strong>{context.bottleneck.duration_days} days</strong> average
            </Typography>
          </CardContent>
        </Card>
      )}
    </Box>
  );
}
