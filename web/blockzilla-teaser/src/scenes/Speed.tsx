import React from "react";
import { AbsoluteFill, Sequence, interpolate, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { C, F, countTo, fmt, riseIn, shake, stamp, stompIn } from "../theme";
import { Eyebrow, Frame, Tag } from "../ui";

const Lane: React.FC<{ name: string; meta: string; w: number; color: string; value: string; start: number; inside?: boolean }> = ({ name, meta, w, color, value, start, inside }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const s = spring({ frame: frame - start, fps, config: { damping: 22, stiffness: 90 }, durationInFrames: 48 });
  const width = interpolate(s, [0, 1], [0, w]);
  return (
    <div style={{ display: "grid", gridTemplateColumns: "440px 1fr", gap: 32, alignItems: "center", opacity: frame < start ? 0 : 1 }}>
      <div>
        <div style={{ fontFamily: F.body, fontWeight: 700, fontSize: 46, color: C.bone }}>{name}</div>
        <div style={{ fontFamily: F.mono, fontSize: 24, color: C.ash, lineHeight: 1.4 }}>{meta}</div>
      </div>
      <div style={{ position: "relative", height: 130, border: `2px solid ${C.line}`, background: `repeating-linear-gradient(90deg, transparent 0 118px, ${C.line} 118px 120px), ${C.ground2}` }}>
        <div style={{ position: "absolute", top: 0, bottom: 0, left: 0, width: `${width}%`, background: `linear-gradient(90deg, ${color}22, ${color})` }} />
        <div style={{ position: "absolute", top: 0, bottom: 0, left: `calc(${width}% - 8px)`, width: 8, background: C.bone }} />
        <div style={{ position: "absolute", top: "50%", translate: "0 -50%", ...(inside ? { right: `calc(${100 - width}% + 36px)`, color: C.ground } : { left: `calc(${width}% + 28px)`, color }), fontFamily: F.display, fontSize: 60, whiteSpace: "nowrap" }}>
          {value}
        </div>
      </div>
    </div>
  );
};

const Race: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const bz = countTo(frame, 10, 52, 4.34);
  const js = countTo(frame, 22, 64, 2.7);
  return (
    <Frame>
      <div style={{ ...riseIn(frame, fps, 0), marginBottom: 56, display: "flex", alignItems: "center" }}>
        <Tag>ROUND 2</Tag>
        <Eyebrow>Sustained decode rate, transactions per second</Eyebrow>
      </div>
      <div style={{ display: "grid", gap: 40 }}>
        <Lane name="Blockzilla" meta="10-core laptop chip · 7.5 GiB RAM · local disk" w={92} color={C.kaiju} value={`${fmt(bz, 2)}M tx/s`} start={10} inside />
        <Lane name="Jetstreamer" meta="64 cores · 30 Gbps+ link · record run" w={57.2} color={C.lava} value={`${fmt(js, 1)}M tx/s`} start={22} />
      </div>
      <div style={{ fontFamily: F.body, fontSize: 40, color: C.ash, marginTop: 56, ...riseIn(frame, fps, 62) }}>
        Both count transactions handed to a consumer. Blockzilla's is the average over the entire archive, not a burst.
      </div>
    </Frame>
  );
};

const HeadToHead: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const bzT = Math.min(79.6, countTo(frame, 14, 58, 144.7));
  const jsT = countTo(frame, 14, 58, 144.7);
  const bzDone = bzT >= 79.6;
  const win = stompIn(frame, fps, 60);
  return (
    <AbsoluteFill style={{ translate: `${shake(frame, 62, 20, 10, "h2h")}` }}>
      <Frame>
        <div style={{ ...riseIn(frame, fps, 0), marginBottom: 40 }}>
          <Eyebrow>Head to head · same 8,192 blocks of epoch 900 · byte-identical output</Eyebrow>
        </div>
        <div style={{ fontFamily: F.display, fontSize: 120, color: C.bone, lineHeight: 1, ...riseIn(frame, fps, 2) }}>SAME BYTES OUT.</div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 40, marginTop: 56 }}>
          <div style={{ border: `3px solid ${bzDone ? C.kaiju : C.line}`, background: bzDone ? "rgba(77,179,94,0.12)" : C.ground2, padding: "34px 40px", ...riseIn(frame, fps, 8) }}>
            <div style={{ fontFamily: F.body, fontWeight: 700, fontSize: 44, color: C.bone }}>Blockzilla</div>
            <div style={{ fontFamily: F.mono, fontSize: 24, color: C.ash }}>0.91 CPU cores average</div>
            <div style={{ fontFamily: F.display, fontSize: 150, color: bzDone ? C.kaiju : C.bone, lineHeight: 1, marginTop: 14 }}>{fmt(bzT, 1)}<span style={{ fontSize: 60 }}> s</span></div>
          </div>
          <div style={{ border: `3px solid ${C.line}`, background: C.ground2, padding: "34px 40px", ...riseIn(frame, fps, 12) }}>
            <div style={{ fontFamily: F.body, fontWeight: 700, fontSize: 44, color: C.bone }}>Jetstreamer 0.7.0</div>
            <div style={{ fontFamily: F.mono, fontSize: 24, color: C.ash }}>4.13 CPU cores average</div>
            <div style={{ fontFamily: F.display, fontSize: 150, color: C.lava, lineHeight: 1, marginTop: 14 }}>{fmt(jsT, 1)}<span style={{ fontSize: 60 }}> s</span></div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "baseline", gap: 30, marginTop: 40 }}>
          <div style={{ fontFamily: F.display, fontSize: 96, color: C.gold, textShadow: stamp(C.lavaDeep), opacity: win.opacity, scale: `${win.scale}`, transformOrigin: "left center", whiteSpace: "nowrap" }}>1.82× FASTER</div>
          <div style={{ fontFamily: F.body, fontSize: 36, color: C.ash, lineHeight: 1.3, ...riseIn(frame, fps, 70) }}>on a quarter of the CPU.<br />Full-epoch scans: 10× faster than zstd CAR.</div>
        </div>
      </Frame>
    </AbsoluteFill>
  );
};

export const Speed: React.FC = () => {
  return (
    <AbsoluteFill>
      <Sequence name="Race" durationInFrames={90} premountFor={10}>
        <Race />
      </Sequence>
      <Sequence name="Head to head" from={90} durationInFrames={90} premountFor={10}>
        <HeadToHead />
      </Sequence>
    </AbsoluteFill>
  );
};
