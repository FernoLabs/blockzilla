import React from "react";
import { AbsoluteFill, Audio, Sequence, Series } from "remotion";
import { boneCrack, ding, vineBoom, whoosh } from "@remotion/sfx";
import { BlockField } from "./BlockField";
import { C } from "./theme";
import { Title } from "./scenes/Title";
import { Size } from "./scenes/Size";
import { Box } from "./scenes/Box";
import { Speed } from "./scenes/Speed";
import { Cost } from "./scenes/Cost";
import { Bytes } from "./scenes/Bytes";
import { Outro } from "./scenes/Outro";

// 30 s at 30 fps = 900 frames. Hard cuts; each scene brings its own entrance.
export const SCENES = { title: 105, size: 150, box: 120, speed: 180, cost: 150, bytes: 105, outro: 90 };
export const TOTAL = Object.values(SCENES).reduce((a, b) => a + b, 0);

const cut = (n: number) => Object.values(SCENES).slice(0, n).reduce((a, b) => a + b, 0);

export const Teaser: React.FC = () => {
  return (
    <AbsoluteFill style={{ background: C.ground, color: C.bone }}>
      <BlockField />
      <Series>
        <Series.Sequence name="Title" durationInFrames={SCENES.title} premountFor={15}><Title /></Series.Sequence>
        <Series.Sequence name="Size" durationInFrames={SCENES.size} premountFor={15}><Size /></Series.Sequence>
        <Series.Sequence name="Box" durationInFrames={SCENES.box} premountFor={15}><Box /></Series.Sequence>
        <Series.Sequence name="Speed" durationInFrames={SCENES.speed} premountFor={15}><Speed /></Series.Sequence>
        <Series.Sequence name="Cost" durationInFrames={SCENES.cost} premountFor={15}><Cost /></Series.Sequence>
        <Series.Sequence name="Bytes" durationInFrames={SCENES.bytes} premountFor={15}><Bytes /></Series.Sequence>
        <Series.Sequence name="Outro" durationInFrames={SCENES.outro} premountFor={15}><Outro /></Series.Sequence>
      </Series>

      {/* Sound cues */}
      <Sequence name="Boom: BLOCKZILLA" from={4} layout="none"><Audio src={vineBoom} volume={0.9} /></Sequence>
      <Sequence name="Boom: SOLANA" from={44} layout="none"><Audio src={vineBoom} volume={0.9} /></Sequence>
      {[1, 2, 3, 4, 5, 6].map((n) => (
        <Sequence key={n} name={`Whoosh cut ${n}`} from={cut(n) - 4} layout="none"><Audio src={whoosh} volume={0.6} /></Sequence>
      ))}
      <Sequence name="Crunch: compaction" from={cut(1) + 48} layout="none"><Audio src={boneCrack} volume={0.8} /></Sequence>
      <Sequence name="Boom: 5.95x" from={cut(1) + 80} layout="none"><Audio src={vineBoom} volume={0.7} /></Sequence>
      <Sequence name="Boom: ONE BOX" from={cut(2) + 22} layout="none"><Audio src={vineBoom} volume={0.7} /></Sequence>
      <Sequence name="Ding: Blockzilla finishes" from={cut(3) + 90 + 60} layout="none"><Audio src={ding} volume={0.7} /></Sequence>
      <Sequence name="Boom: savings" from={cut(4) + 96} layout="none"><Audio src={vineBoom} volume={0.7} /></Sequence>
      <Sequence name="Boom: outro" from={cut(6) + 16} layout="none"><Audio src={vineBoom} volume={0.9} /></Sequence>
    </AbsoluteFill>
  );
};
