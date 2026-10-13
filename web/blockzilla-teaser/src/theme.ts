import { loadFont as loadBungee } from "@remotion/google-fonts/Bungee";
import { loadFont as loadBarlow } from "@remotion/google-fonts/Barlow";
import { loadFont as loadPlex } from "@remotion/google-fonts/IBMPlexMono";
import { interpolate, random, spring } from "remotion";

const bungee = loadBungee("normal", { weights: ["400"], subsets: ["latin"] });
const barlow = loadBarlow("normal", { weights: ["500", "700"], subsets: ["latin"] });
const plex = loadPlex("normal", { weights: ["500", "600"], subsets: ["latin"] });

export const F = {
  display: `${bungee.fontFamily}, Impact, "Arial Black", sans-serif`,
  body: `${barlow.fontFamily}, "Helvetica Neue", Arial, sans-serif`,
  mono: `${plex.fontFamily}, Menlo, Consolas, monospace`,
};

export const C = {
  ground: "#07090b",
  ground2: "#0d1114",
  line: "#1f2a2c",
  bone: "#e9ede6",
  ash: "#8a948c",
  kaiju: "#4db35e",
  kaijuDeep: "#2b7a3a",
  lava: "#e8472a",
  lavaDeep: "#9c3d2c",
  gold: "#f5c542",
  sol: "#14f195",
};

/** Poster text shadow: gold offset + deep colour offset. */
export const stamp = (deep: string) =>
  `4px 4px 0 ${C.gold}, 8px 8px 0 ${deep}`;

/** A kaiju stomp: scales down from 1.5 to 1 with a hard landing. */
export const stompIn = (frame: number, fps: number, start: number) => {
  const s = spring({
    frame: frame - start,
    fps,
    config: { damping: 14, stiffness: 320, mass: 0.9 },
    durationInFrames: 22,
  });
  return {
    opacity: frame < start ? 0 : 1,
    scale: interpolate(s, [0, 1], [1.6, 1]),
  };
};

/** A push-in from below with no bounce. */
export const riseIn = (frame: number, fps: number, start: number, dist = 40) => {
  const s = spring({
    frame: frame - start,
    fps,
    config: { damping: 200 },
    durationInFrames: 20,
  });
  return {
    opacity: frame < start ? 0 : s,
    translate: `0px ${interpolate(s, [0, 1], [dist, 0])}px`,
  };
};

/** Camera shake that decays over `len` frames after `start`. */
export const shake = (frame: number, start: number, amp = 18, len = 12, seed = "s") => {
  const t = frame - start;
  if (t < 0 || t > len) return "0px 0px";
  const k = amp * (1 - t / len);
  const x = (random(`${seed}x${t}`) - 0.5) * 2 * k;
  const y = (random(`${seed}y${t}`) - 0.5) * 2 * k;
  return `${x.toFixed(1)}px ${y.toFixed(1)}px`;
};

/** Ease-out count-up from 0 to target between start and end frames. */
export const countTo = (frame: number, start: number, end: number, target: number) => {
  const p = interpolate(frame, [start, end], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const e = 1 - Math.pow(1 - p, 3);
  return target * e;
};

export const fmt = (n: number, dec = 0) =>
  n.toLocaleString("en-US", { minimumFractionDigits: dec, maximumFractionDigits: dec });
