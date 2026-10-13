import React from "react";
import { C, F } from "./theme";

export const Eyebrow: React.FC<{ children: React.ReactNode; style?: React.CSSProperties }> = ({ children, style }) => (
  <div
    style={{
      fontFamily: F.mono,
      fontSize: 26,
      letterSpacing: "0.22em",
      textTransform: "uppercase",
      color: C.ash,
      fontWeight: 600,
      ...style,
    }}
  >
    {children}
  </div>
);

export const Tag: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <span
    style={{
      fontFamily: F.display,
      fontSize: 26,
      color: C.ground,
      background: C.gold,
      padding: "8px 16px",
      letterSpacing: "0.06em",
      marginRight: 22,
      display: "inline-block",
    }}
  >
    {children}
  </span>
);

export const Frame: React.FC<{ children: React.ReactNode; style?: React.CSSProperties }> = ({ children, style }) => (
  <div
    style={{
      position: "absolute",
      inset: 0,
      padding: "100px 120px",
      display: "flex",
      flexDirection: "column",
      justifyContent: "center",
      ...style,
    }}
  >
    {children}
  </div>
);
