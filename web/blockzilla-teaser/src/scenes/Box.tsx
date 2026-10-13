import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame, useVideoConfig } from "remotion";
import { C, F, riseIn, shake, stamp, stompIn } from "../theme";
import { Eyebrow, Frame } from "../ui";

export const Box: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const a = stompIn(frame, fps, 4);
  const b = stompIn(frame, fps, 22);
  return (
    <AbsoluteFill style={{ translate: `${shake(frame, 26, 18, 10, "bx")}` }}>
      <Frame style={{ flexDirection: "row", alignItems: "center", gap: 90 }}>
        <div style={{ flex: 1.2 }}>
          <div style={{ fontFamily: F.display, fontSize: 150, lineHeight: 0.95 }}>
            <div style={{ color: C.bone, opacity: a.opacity, scale: `${a.scale}`, transformOrigin: "left center" }}>ALL 1,019</div>
            <div style={{ color: C.bone, opacity: a.opacity, scale: `${a.scale}`, transformOrigin: "left center" }}>EPOCHS.</div>
            <div style={{ color: C.lava, textShadow: stamp(C.lavaDeep), opacity: b.opacity, scale: `${b.scale}`, transformOrigin: "left center", marginTop: 12 }}>ONE BOX.</div>
          </div>
          <div style={{ fontFamily: F.mono, fontSize: 30, color: C.ash, marginTop: 44, lineHeight: 1.6, ...riseIn(frame, fps, 40) }}>
            UGREEN DXP8800 Plus · Intel i5-1235U · 10 cores<br />
            <span style={{ color: C.gold }}>7.5 GiB RAM</span> · 8 × 28 TB · no rack, no cluster
          </div>
        </div>
        <div style={{ flex: 0.8, ...riseIn(frame, fps, 30, 60) }}>
          {/* the NAS: eight bays lighting up one after another */}
          <div style={{ border: `3px solid ${C.line}`, background: C.ground2, padding: 28, display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 18 }}>
            {Array.from({ length: 8 }).map((_, i) => {
              const on = interpolate(frame, [42 + i * 5, 50 + i * 5], [0, 1], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
              return (
                <div key={i} style={{ height: 150, border: `2px solid ${C.line}`, background: `rgba(77,179,94,${0.08 + on * 0.75})`, display: "flex", alignItems: "flex-end", justifyContent: "center", paddingBottom: 12 }}>
                  <div style={{ width: 14, height: 14, borderRadius: 7, background: on > 0.5 ? C.sol : C.line }} />
                </div>
              );
            })}
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginTop: 26, ...riseIn(frame, fps, 84) }}>
            <Eyebrow>Full read + decode</Eyebrow>
            <div style={{ fontFamily: F.display, fontSize: 84, color: C.kaiju }}>1d 12h</div>
          </div>
        </div>
      </Frame>
    </AbsoluteFill>
  );
};
