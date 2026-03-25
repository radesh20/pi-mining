import React from "react";
import CircularProgress from "@mui/material/CircularProgress";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

export default function LoadingSpinner({ message = "Loading from Celonis..." }) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        minHeight: 220,
        gap: 2,
        border: "1px solid #e5e7eb",
        borderRadius: 2,
        background: "#ffffff",
        boxShadow: "0 1px 2px rgba(16,24,40,0.06)",
        p: 3,
      }}
    >
      <CircularProgress color="primary" />
      <Typography variant="body2" color="text.secondary" sx={{ textAlign: "center" }}>
        {message}
      </Typography>
    </Box>
  );
}
