import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame, useVideoConfig } from "remotion";
import { C, F, riseIn, shake, stamp, stompIn } from "../theme";
import { Eyebrow, Frame } from "../ui";

export const Outro: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps, durationInFrames } = useVideoConfig();
  const a = stompIn(frame, fps, 3);
  const b = stompIn(frame, fps, 16);
  const fade = interpolate(frame, [durationInFrames - 14, durationInFrames - 1], [1, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
  return (
    <AbsoluteFill style={{ opacity: fade, translate: `${shake(frame, 18, 18, 10, "out")}` }}>
      <Frame style={{ alignItems: "center", textAlign: "center" }}>
        <div style={{ fontFamily: F.display, fontSize: 190, lineHeight: 0.95 }}>
          <div style={{ color: C.kaiju, textShadow: stamp(C.kaijuDeep), opacity: a.opacity, scale: `${a.scale}` }}>THE WHOLE CHAIN.</div>
          <div style={{ color: C.lava, textShadow: stamp(C.lavaDeep), opacity: b.opacity, scale: `${b.scale}` }}>ONE BOX.</div>
        </div>
        <div style={{ fontFamily: F.mono, fontSize: 44, color: C.gold, marginTop: 60, letterSpacing: "0.08em", ...riseIn(frame, fps, 34) }}>blockzilla.dev/baseline</div>
        <Eyebrow style={{ marginTop: 22, ...riseIn(frame, fps, 42) }}>Blockzilla eats Solana · MIT licensed · Rust</Eyebrow>
      </Frame>
    </AbsoluteFill>
  );
};
