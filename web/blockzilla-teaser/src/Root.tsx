import "./index.css";
import { Composition, Folder } from "remotion";
import { SCENES, TOTAL, Teaser } from "./Teaser";
import { Title } from "./scenes/Title";
import { Size } from "./scenes/Size";
import { Box } from "./scenes/Box";
import { Speed } from "./scenes/Speed";
import { Cost } from "./scenes/Cost";
import { Bytes } from "./scenes/Bytes";
import { Outro } from "./scenes/Outro";

const base = { width: 1920, height: 1080, fps: 30 };

export const RemotionRoot: React.FC = () => {
  return (
    <>
      <Composition id="Teaser" component={Teaser} durationInFrames={TOTAL} {...base} />
      <Folder name="Scenes">
        <Composition id="Title" component={Title} durationInFrames={SCENES.title} {...base} />
        <Composition id="Size" component={Size} durationInFrames={SCENES.size} {...base} />
        <Composition id="Box" component={Box} durationInFrames={SCENES.box} {...base} />
        <Composition id="Speed" component={Speed} durationInFrames={SCENES.speed} {...base} />
        <Composition id="Cost" component={Cost} durationInFrames={SCENES.cost} {...base} />
        <Composition id="Bytes" component={Bytes} durationInFrames={SCENES.bytes} {...base} />
        <Composition id="Outro" component={Outro} durationInFrames={SCENES.outro} {...base} />
      </Folder>
    </>
  );
};
