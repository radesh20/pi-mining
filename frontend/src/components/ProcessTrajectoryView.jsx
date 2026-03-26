import React from "react";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

// Mock delay data per stage when none provided — used for demo visualization
const DEFAULT_DELAY_NOTES = {
  "Invoice Receipt": null,
  "3-Way Match": "82% of delays occur in this stage",
  "Approval": "High-delay stage — avg 2.1d above baseline",
  "Payment Run": null,
};

export default function ProcessTrajectoryView({ goldenPath, goldenPathPercentage, activityDurations }) {
  const steps = goldenPath ? goldenPath.split("→").map(s => s.trim()) : [];
  const percentLabel = goldenPathPercentage != null ? `${goldenPathPercentage}%` : null;

  return (
    <>
      <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 2, mt: 3.5 }}>
        <Typography sx={{ fontFamily: S, fontSize: "1.35rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.015em", whiteSpace: "nowrap" }}>
          Process Trajectory
        </Typography>
        {percentLabel && (
          <Box sx={{ background: "#F0EDE6", border: "1px solid #E8E3DA", borderRadius: "99px", px: 1.2, py: 0.2 }}>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 600, color: "#9C9690", fontFamily: G }}>{percentLabel} follow golden path</Typography>
          </Box>
        )}
        <Box sx={{ flex: 1, height: "1px", background: "#E8E3DA" }} />
      </Box>
      <Card>
        <CardContent>
          {steps.length > 0 && (
            <Box sx={{ display: "flex", alignItems: "flex-start", gap: 1, flexWrap: "wrap", mb: activityDurations ? 2 : 0 }}>
              {steps.map((step, i, arr) => {
                const durationVal = activityDurations?.[step];
                const note = DEFAULT_DELAY_NOTES[step];
                const isHighDelay = note != null;
                return (
                  <React.Fragment key={i}>
                    <Box sx={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 0.4 }}>
                      <Box sx={{
                        background: isHighDelay ? "#FEF3DC" : "#F5ECD9",
                        border: isHighDelay ? "1px solid #F0C870" : "1px solid #DEC48A",
                        color: isHighDelay ? "#A05A10" : "#B5742A",
                        px: 1.5, py: 0.5, borderRadius: "8px",
                        fontSize: "0.78rem", fontWeight: 500, fontFamily: G,
                        position: "relative",
                      }}>
                        {step}
                        {isHighDelay && (
                          <Box sx={{ position: "absolute", top: -6, right: -6, width: 10, height: 10, borderRadius: "50%", background: "#A05A10", border: "1.5px solid #FFF" }} />
                        )}
                      </Box>
                      {durationVal != null && (
                        <Typography sx={{ fontSize: "0.65rem", color: "#9C9690", fontFamily: G }}>{durationVal}d avg</Typography>
                      )}
                      {isHighDelay && (
                        <Typography sx={{ fontSize: "0.62rem", color: "#A05A10", fontFamily: G, textAlign: "center", maxWidth: "110px" }}>{note}</Typography>
                      )}
                    </Box>
                    {i < arr.length - 1 && (
                      <Box sx={{ display: "flex", alignItems: "center", pt: 0.5 }}>
                        {activityDurations?.[step] != null && activityDurations?.[steps[i + 1]] != null ? (
                          <Box sx={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
                            <Typography sx={{ color: "#C4BDB0", fontSize: "0.9rem" }}>→</Typography>
                          </Box>
                        ) : (
                          <Typography sx={{ color: "#C4BDB0", fontSize: "0.9rem" }}>→</Typography>
                        )}
                      </Box>
                    )}
                  </React.Fragment>
                );
              })}
            </Box>
          )}

          {activityDurations && Object.keys(activityDurations).length > 0 && (
            <Box sx={{ pt: steps.length > 0 ? 1.5 : 0, borderTop: steps.length > 0 ? "1px solid #F0EDE6" : "none" }}>
              <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 1 }}>
                Observed Transition Durations
              </Typography>
              <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1 }}>
                {Object.entries(activityDurations).map(([key, val]) => (
                  <Box key={key} sx={{ background: "#F5F2EC", border: "1px solid #E8E3DA", borderRadius: "8px", px: 1.2, py: 0.5 }}>
                    <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G, mb: 0.2 }}>{key}</Typography>
                    <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#1A6B5E", lineHeight: 1 }}>
                      {val}<span style={{ fontSize: "0.7rem", color: "#9C9690", marginLeft: "3px" }}>days</span>
                    </Typography>
                  </Box>
                ))}
              </Box>
            </Box>
          )}

          {steps.length === 0 && !activityDurations && (
            <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>No trajectory data available.</Typography>
          )}
        </CardContent>
      </Card>
    </>
  );
}
