import React from "react";
import { AbsoluteFill, interpolate, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { C, F, countTo, fmt, riseIn, shake, stamp, stompIn } from "../theme";
import { Eyebrow, Frame, Tag } from "../ui";

const Bar: React.FC<{ label: string; sub: string; w: number; color: string; value: string; start: number; textColor: string; stripes?: boolean; inside?: boolean }> = ({
  label, sub, w, color, value, start, textColor, stripes, inside,
}) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const s = spring({ frame: frame - start, fps, config: { damping: 18, stiffness: 120 }, durationInFrames: 40 });
  const width = interpolate(s, [0, 1], [0, w]);
  return (
    <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: 32, alignItems: "center", opacity: frame < start ? 0 : 1 }}>
      <div>
        <div style={{ fontFamily: F.mono, fontSize: 24, letterSpacing: "0.16em", color: C.ash, textTransform: "uppercase" }}>{label}</div>
        <div style={{ fontFamily: F.body, fontWeight: 700, fontSize: 40, color: C.bone }}>{sub}</div>
      </div>
      <div style={{ position: "relative", height: 120, background: C.ground2, border: `2px solid ${C.line}` }}>
        <div
          style={{
            position: "absolute", top: 0, bottom: 0, left: 0, width: `${width}%`,
            background: stripes ? "repeating-linear-gradient(135deg,#2a2f33 0 18px,#1e2327 18px 36px)" : `linear-gradient(90deg, ${C.kaijuDeep}, ${color})`,
          }}
        />
        <div style={{ position: "absolute", top: "50%", translate: "0 -50%", ...(inside ? { right: 28 } : { left: `calc(${width}% + 28px)` }), fontFamily: F.display, fontSize: 60, color: textColor, whiteSpace: "nowrap" }}>
          {value}
        </div>
      </div>
    </div>
  );
};

export const Size: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const src = countTo(frame, 6, 40, 522.64);
  const bz = countTo(frame, 50, 84, 87.82);
  const blast = stompIn(frame, fps, 80);
  return (
    <AbsoluteFill style={{ translate: `${shake(frame, 82, 26, 12, "sz")}` }}>
      <Frame>
        <div style={{ ...riseIn(frame, fps, 0), marginBottom: 56, display: "flex", alignItems: "center" }}>
          <Tag>ROUND 1</Tag>
          <Eyebrow>Same chain. Same host. Like for like.</Eyebrow>
        </div>
        <div style={{ display: "grid", gap: 44 }}>
          <Bar label="Source" sub="Old Faithful CAR" w={100} color="#2a2f33" stripes inside value={`${fmt(src, 2)} TiB`} start={6} textColor={C.bone} />
          <Bar label="Blockzilla" sub="Compact V2" w={16.8} color={C.kaiju} value={`${fmt(bz, 2)} TiB`} start={50} textColor={C.kaiju} />
        </div>
        <div style={{ display: "flex", alignItems: "baseline", gap: 40, marginTop: 70 }}>
          <div style={{ fontFamily: F.display, fontSize: 250, lineHeight: 0.9, color: C.gold, textShadow: stamp(C.lavaDeep), opacity: blast.opacity, scale: `${blast.scale}`, transformOrigin: "left bottom" }}>
            5.95×
          </div>
          <div style={{ fontFamily: F.display, fontSize: 96, color: C.bone, ...riseIn(frame, fps, 92) }}>SMALLER</div>
        </div>
        <div style={{ fontFamily: F.body, fontSize: 40, color: C.ash, marginTop: 28, ...riseIn(frame, fps, 104) }}>
          Every block, every signature, every account entry. Indexes included. zstd alone only gets you 2.5×.
        </div>
      </Frame>
    </AbsoluteFill>
  );
};
