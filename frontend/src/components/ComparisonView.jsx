import React from "react";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Grid from "@mui/material/Grid";
import Box from "@mui/material/Box";

export default function ComparisonView({ data }) {
  if (!data) return null;
  const { without_process_mining, with_process_mining, key_differences } = data;

  return (
    <Box sx={{ mt: 3 }}>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 2, color: "#fff" }}>
        ⚖️ With vs Without Celonis Process Mining
      </Typography>
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Card sx={{ background: "#1a1a1a", border: "1px solid #ff525255", borderRadius: 2, height: "100%" }}>
            <CardContent>
              <Typography variant="h6" sx={{ color: "#ff5252", fontWeight: 600, mb: 2 }}>
                ❌ Without Process Mining (BI/RDBMS)
              </Typography>
              <Typography variant="body2" sx={{ color: "#ccc", whiteSpace: "pre-wrap", mb: 2 }}>
                {without_process_mining?.prompt}
              </Typography>
              <Typography variant="subtitle2" sx={{ color: "#ff5252", mt: 2 }}>Limitations:</Typography>
              {(without_process_mining?.limitations || []).map((l, i) => (
                <Typography key={i} variant="body2" sx={{ color: "#aaa", ml: 1 }}>• {l}</Typography>
              ))}
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={6}>
          <Card sx={{ background: "#0a1a1a", border: "1px solid #00D4AA55", borderRadius: 2, height: "100%" }}>
            <CardContent>
              <Typography variant="h6" sx={{ color: "#00D4AA", fontWeight: 600, mb: 2 }}>
                ✅ With Celonis Process Mining
              </Typography>
              <Typography variant="body2" sx={{ color: "#ccc", whiteSpace: "pre-wrap", mb: 2 }}>
                {with_process_mining?.prompt}
              </Typography>
              <Typography variant="subtitle2" sx={{ color: "#00D4AA", mt: 2 }}>Advantages:</Typography>
              {(with_process_mining?.advantages || []).map((a, i) => (
                <Typography key={i} variant="body2" sx={{ color: "#aaa", ml: 1 }}>✅ {a}</Typography>
              ))}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
      {key_differences && key_differences.length > 0 && (
        <Card sx={{ background: "#12122a", mt: 3, borderRadius: 2 }}>
          <CardContent>
            <Typography variant="h6" sx={{ fontWeight: 600, color: "#6C63FF", mb: 1 }}>Key Differences</Typography>
            {key_differences.map((d, i) => (
              <Typography key={i} variant="body2" sx={{ color: "#ccc", mb: 0.5 }}>🔹 {d}</Typography>
            ))}
          </CardContent>
        </Card>
      )}
    </Box>
  );
}