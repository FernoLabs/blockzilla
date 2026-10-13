import React from "react";
import { AbsoluteFill, useCurrentFrame, useVideoConfig } from "remotion";
import { C, F, riseIn, shake, stamp, stompIn } from "../theme";
import { Eyebrow, Frame } from "../ui";

export const Title: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const a = stompIn(frame, fps, 4);
  const b = stompIn(frame, fps, 24);
  const c = stompIn(frame, fps, 44);
  const sub = riseIn(frame, fps, 66);
  return (
    <AbsoluteFill style={{ translate: `${shake(frame, 8, 22, 10, "t1")}` }}>
      <AbsoluteFill style={{ translate: `${shake(frame, 48, 22, 10, "t3")}` }}>
        <Frame>
          <Eyebrow style={{ ...riseIn(frame, fps, 0), marginBottom: 40 }}>
            Solana archive baseline · measured 2026-08-24 · 1,019 / 1,019 epochs
          </Eyebrow>
          <div style={{ fontFamily: F.display, fontSize: 236, lineHeight: 0.92, letterSpacing: "0.01em" }}>
            <div style={{ color: C.lava, textShadow: stamp(C.lavaDeep), opacity: a.opacity, scale: `${a.scale}`, transformOrigin: "left center" }}>
              BLOCKZILLA
            </div>
            <div style={{ color: C.kaiju, textShadow: stamp(C.kaijuDeep), opacity: b.opacity, scale: `${b.scale}`, transformOrigin: "left center" }}>
              EATS
            </div>
            <div style={{ color: C.lava, textShadow: stamp(C.lavaDeep), opacity: c.opacity, scale: `${c.scale}`, transformOrigin: "left center" }}>
              SOLANA.
            </div>
          </div>
          <div style={{ fontFamily: F.body, fontWeight: 500, fontSize: 52, color: C.bone, marginTop: 44, ...sub }}>
            Every block since genesis. Swallowed whole.
          </div>
        </Frame>
      </AbsoluteFill>
    </AbsoluteFill>
  );
};
