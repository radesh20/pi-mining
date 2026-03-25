import React from "react";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";

export default function PromptSection({ title, data, color = "#1976d2" }) {
  if (!data) return null;

  return (
    <Card
      sx={{
        border: `1px solid ${color}33`,
        borderRadius: 2,
        mb: 2,
        "&:hover": { boxShadow: "0 10px 24px rgba(15, 23, 42, 0.08)" },
      }}
    >
      <CardContent>
        <Typography variant="h6" sx={{ fontWeight: 600, color, mb: 2 }}>
          {title}
        </Typography>
        {Array.isArray(data) ? (
          data.map((item, idx) => (
            <Box key={idx} className="step-card" sx={{ mb: 1 }}>
              {typeof item === "object" ? (
                <>
                  {item.step && (
                    <Typography variant="subtitle2" sx={{ color: "#1f2937", fontWeight: 700 }}>
                      Step {item.step}
                    </Typography>
                  )}
                  {item.instruction && (
                    <Typography variant="body2" sx={{ color: "#374151" }}>
                      {item.instruction}
                    </Typography>
                  )}
                  {item.condition && (
                    <Typography variant="body2" sx={{ color: "#374151" }}>
                      <strong>IF:</strong> {item.condition} → <strong>{item.action}</strong>
                    </Typography>
                  )}
                  {item.rule && (
                    <Typography variant="body2" sx={{ color: "#374151" }}>
                      {item.rule}
                    </Typography>
                  )}
                  {(item.source || item.source_evidence || item.process_context) && (
                    <Typography variant="caption" sx={{ color: "#2e7d32", mt: 0.5, display: "block" }}>
                      📊 {item.source || item.source_evidence || item.process_context}
                    </Typography>
                  )}
                  {item.enforcement && <span className="guardrail-tag">{item.enforcement}</span>}
                  {item.type && <span className="guardrail-tag">{item.type}</span>}
                </>
              ) : (
                <Typography variant="body2" sx={{ color: "#374151" }}>{item}</Typography>
              )}
            </Box>
          ))
        ) : typeof data === "object" ? (
          <pre className="json-display">{JSON.stringify(data, null, 2)}</pre>
        ) : (
          <Typography variant="body2" sx={{ color: "#374151" }}>{data}</Typography>
        )}
      </CardContent>
    </Card>
  );
}
