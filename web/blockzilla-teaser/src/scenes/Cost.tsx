import React from "react";
import { AbsoluteFill, useCurrentFrame, useVideoConfig } from "remotion";
import { C, F, countTo, fmt, riseIn, shake, stamp, stompIn } from "../theme";
import { Eyebrow, Frame, Tag } from "../ui";

const Card: React.FC<{ who: string; price: React.ReactNode; body: string; foot: string; start: number; win?: boolean }> = ({ who, price, body, foot, start, win }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const st = win ? stompIn(frame, fps, start) : { opacity: 1, scale: 1 };
  const r = win ? {} : riseIn(frame, fps, start, 70);
  return (
    <div style={{ border: `3px solid ${win ? C.kaiju : C.line}`, background: win ? "linear-gradient(180deg, rgba(77,179,94,0.18), #0d1114)" : C.ground2, padding: "36px 38px", display: "flex", flexDirection: "column", gap: 14, minHeight: 470, opacity: st.opacity, scale: `${st.scale}`, ...r }}>
      <div style={{ fontFamily: F.mono, fontSize: 24, letterSpacing: "0.16em", textTransform: "uppercase", color: C.ash }}>{who}</div>
      <div style={{ fontFamily: F.display, fontSize: 84, lineHeight: 1, color: win ? C.kaiju : C.bone }}>{price}</div>
      <div style={{ fontFamily: F.body, fontSize: 32, color: win ? C.bone : C.ash, lineHeight: 1.35 }}>{body}</div>
      <div style={{ marginTop: "auto", fontFamily: F.mono, fontSize: 21, color: C.ash }}>{foot}</div>
    </div>
  );
};

export const Cost: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const save = countTo(frame, 96, 130, 72376);
  const s = stompIn(frame, fps, 96);
  return (
    <AbsoluteFill style={{ translate: `${shake(frame, 60, 16, 10, "cost")}` }}>
      <Frame>
        <div style={{ ...riseIn(frame, fps, 0), marginBottom: 44, display: "flex", alignItems: "center" }}>
          <Tag>ROUND 3</Tag>
          <Eyebrow>Published 2026 list prices. No egress, no power.</Eyebrow>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 28 }}>
          <Card who="Rent it · RPC node" price={<>$2,900<span style={{ fontSize: 36, color: C.ash }}>/mo</span></>} body="Dedicated node, entry tier. History stays metered, throttled and serial." foot="helius.dev/pricing" start={6} />
          <Card who="Run it · archive node" price={<>~$40k<span style={{ fontSize: 36, color: C.ash }}> +$3k/mo</span></>} body="400 TB of ledger, growing 80 TB a year. Upgrade again in 18 months." foot="GetBlock guide, 2025" start={18} />
          <Card who="Own it · Blockzilla" price="ONE NAS" body="Whole chain on 87.82 TiB. About €8k in parts. No vendor, no metering, no account in the path." foot="blockzilla.dev/baseline" start={58} win />
        </div>
        <div style={{ display: "flex", alignItems: "baseline", gap: 30, marginTop: 50 }}>
          <div style={{ fontFamily: F.display, fontSize: 120, color: C.gold, textShadow: stamp(C.lavaDeep), lineHeight: 1, opacity: s.opacity, scale: `${s.scale}`, transformOrigin: "left center" }}>
            ${fmt(save)}
          </div>
          <div style={{ fontFamily: F.body, fontSize: 44, color: C.bone, ...riseIn(frame, fps, 104) }}>a year of rented stack you don't pay for.</div>
        </div>
      </Frame>
    </AbsoluteFill>
  );
};
