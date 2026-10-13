import React from "react";
import { AbsoluteFill, random, useCurrentFrame, useVideoConfig } from "remotion";
import { C } from "./theme";

const N = 90;

/** Blocks streaming rightwards into a lava line: the compactor's mouth. */
export const BlockField: React.FC<{ opacity?: number }> = ({ opacity = 0.55 }) => {
  const frame = useCurrentFrame();
  const { width, height } = useVideoConfig();
  const mouth = width * 0.86;
  return (
    <AbsoluteFill style={{ opacity, overflow: "hidden" }}>
      {Array.from({ length: N }).map((_, i) => {
        const x0 = random(`x${i}`) * (mouth + 200);
        const y = random(`y${i}`) * height;
        const size = 8 + random(`s${i}`) * 18;
        const v = 2.5 + random(`v${i}`) * 5;
        const gold = random(`g${i}`) < 0.16;
        const span = mouth + 200;
        const x = ((x0 + frame * v) % span) - 100;
        const d = mouth - x;
        const k = d < 260 ? Math.max(0.15, d / 260) : 1;
        const s = size * k;
        return (
          <div
            key={i}
            style={{
              position: "absolute",
              left: x,
              top: y - s / 2,
              width: s,
              height: s,
              background: gold ? C.gold : C.kaiju,
              opacity: 0.12 + random(`a${i}`) * 0.3 + (d < 260 ? (1 - k) * 0.5 : 0),
            }}
          />
        );
      })}
      <div
        style={{
          position: "absolute",
          left: mouth - 60,
          top: 0,
          width: 62,
          height,
          background: `linear-gradient(90deg, rgba(232,71,42,0), rgba(232,71,42,0.45))`,
        }}
      />
      <div style={{ position: "absolute", left: mouth, top: 0, width: 3, height, background: C.gold, opacity: 0.7 }} />
    </AbsoluteFill>
  );
};
