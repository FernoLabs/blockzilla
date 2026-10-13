import React from "react";
import { AbsoluteFill, useCurrentFrame, useVideoConfig } from "remotion";
import { C, F, countTo, fmt, riseIn, stompIn } from "../theme";
import { Eyebrow, Frame } from "../ui";

export const Bytes: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const tx = countTo(frame, 8, 70, 565020467324);
  const t = stompIn(frame, fps, 2);
  return (
    <AbsoluteFill>
      <Frame>
        <div style={{ fontFamily: F.display, fontSize: 150, color: C.bone, lineHeight: 1, opacity: t.opacity, scale: `${t.scale}`, transformOrigin: "left center" }}>EVERY BYTE.</div>
        <div style={{ fontFamily: F.display, fontSize: 170, color: C.sol, lineHeight: 1, marginTop: 40, fontVariantNumeric: "tabular-nums", ...riseIn(frame, fps, 6) }}>
          {fmt(tx)}
        </div>
        <Eyebrow style={{ marginTop: 14, ...riseIn(frame, fps, 10) }}>transactions, genesis to epoch 1018</Eyebrow>
        <div style={{ display: "flex", gap: 80, marginTop: 70, ...riseIn(frame, fps, 60) }}>
          {[
            ["418,258,202", "blocks"],
            ["577,682,776,307", "signatures"],
            ["21,388,292,235", "account entries"],
          ].map(([n, l]) => (
            <div key={l} style={{ borderTop: `4px solid ${C.kaiju}`, paddingTop: 16 }}>
              <div style={{ fontFamily: F.display, fontSize: 60, color: C.bone }}>{n}</div>
              <Eyebrow style={{ marginTop: 6 }}>{l}</Eyebrow>
            </div>
          ))}
        </div>
      </Frame>
    </AbsoluteFill>
  );
};
