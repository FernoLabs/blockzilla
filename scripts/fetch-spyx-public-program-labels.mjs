#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

const [inventoryPath, outputPath] = process.argv.slice(2);
if (!inventoryPath || !outputPath || process.argv.length !== 4) {
  console.error(
    "usage: node scripts/fetch-spyx-public-program-labels.mjs <program-inventory.json> <output.json>",
  );
  process.exit(2);
}

const concurrency = positiveInteger(
  process.env.SPYX_LABEL_CONCURRENCY ?? "6",
  "SPYX_LABEL_CONCURRENCY",
  12,
);
const sourceBase = (
  process.env.SPYX_LABEL_SOURCE ?? "https://wassup.trenchscreener.ai/"
).replace(/\/?$/, "/");

const inventoryBytes = await readFile(inventoryPath);
const inventory = JSON.parse(inventoryBytes.toString("utf8"));
if (!Array.isArray(inventory.programs) || inventory.programs.length === 0) {
  throw new Error("inventory does not contain a non-empty programs array");
}

const programIds = inventory.programs.map((program) => program.program_id);
if (
  programIds.some((programId) => typeof programId !== "string") ||
  new Set(programIds).size !== programIds.length
) {
  throw new Error("inventory contains a missing or duplicate program_id");
}

const results = new Map();
try {
  const previous = JSON.parse(await readFile(outputPath, "utf8"));
  if (previous.source?.inventory_sha256 === sha256(inventoryBytes)) {
    for (const item of previous.programs ?? []) {
      if (programIds.includes(item.program_id) && !item.error) {
        results.set(item.program_id, item);
      }
    }
  }
} catch (error) {
  if (error?.code !== "ENOENT") throw error;
}

let nextIndex = 0;
let completedThisRun = 0;
let writes = Promise.resolve();

async function worker() {
  while (true) {
    const index = nextIndex++;
    if (index >= programIds.length) return;
    const programId = programIds[index];
    if (!results.has(programId)) {
      results.set(programId, await fetchLabel(programId));
      completedThisRun += 1;
    }
    const completed = results.size;
    if (completed % 25 === 0 || completed === programIds.length) {
      console.error(
        `public labels: ${completed}/${programIds.length}, named=${namedCount()}, fetched_this_run=${completedThisRun}`,
      );
      writes = writes.then(() => writeReport(completed === programIds.length));
      await writes;
    }
  }
}

await Promise.all(Array.from({ length: concurrency }, () => worker()));
await writes;
await writeReport(true);

async function fetchLabel(programId) {
  const url = new URL(sourceBase);
  url.searchParams.set("search", programId);
  url.searchParams.set("chain", "solana");
  let lastError;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      const response = await fetch(url, {
        headers: {
          accept: "text/html",
          "user-agent": "blockzilla-spyx-program-identification/1",
        },
        signal: AbortSignal.timeout(30_000),
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const html = await response.text();
      const row = findExactRow(html, programId);
      return {
        program_id: programId,
        query_url: url.toString(),
        response_sha256: sha256(Buffer.from(html)),
        tracked: row !== null,
        name: row?.name ?? null,
        github_url: row?.githubUrl ?? null,
        has_idl: row?.hasIdl ?? false,
        checked_at: new Date().toISOString(),
      };
    } catch (error) {
      lastError = error;
      if (attempt !== 4) {
        await new Promise((resolve) =>
          setTimeout(resolve, Math.min(8_000, 400 * 2 ** attempt)),
        );
      }
    }
  }
  return {
    program_id: programId,
    query_url: url.toString(),
    error: errorMessage(lastError),
    checked_at: new Date().toISOString(),
  };
}

function findExactRow(html, programId) {
  for (const match of html.matchAll(/<tr>([\s\S]*?)<\/tr>/g)) {
    const cells = Array.from(match[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g));
    if (cells.length < 11) continue;
    const address = textContent(cells[1][1]);
    if (address !== programId) continue;
    const rawName = textContent(cells[2][1]);
    const githubMatch = cells[8][1].match(/<a\s+href="([^"]+)"/);
    return {
      name: rawName === "-" || rawName.length === 0 ? null : rawName,
      githubUrl: githubMatch ? decodeEntities(githubMatch[1]) : null,
      hasIdl: textContent(cells[10][1]) === "✓",
    };
  }
  return null;
}

function textContent(html) {
  return decodeEntities(html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim());
}

function decodeEntities(value) {
  return value
    .replaceAll("&amp;", "&")
    .replaceAll("&quot;", '"')
    .replaceAll("&#39;", "'")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">");
}

function namedCount() {
  let count = 0;
  for (const item of results.values()) if (item.name) count += 1;
  return count;
}

async function writeReport(complete) {
  const programs = programIds
    .filter((programId) => results.has(programId))
    .map((programId) => results.get(programId));
  const report = {
    schema_version: 1,
    artifact_kind: "spyx_public_program_labels",
    complete:
      complete &&
      programs.length === programIds.length &&
      programs.every((item) => !item.error),
    generated_at: new Date().toISOString(),
    source: {
      inventory_path: path.resolve(inventoryPath),
      inventory_sha256: sha256(inventoryBytes),
      inventory_programs: programIds.length,
      public_tracker: sourceBase,
      lookup: "exact program ID search",
    },
    counters: {
      programs_checked: programs.length,
      programs_tracked: programs.filter((item) => item.tracked).length,
      programs_named: programs.filter((item) => item.name).length,
      programs_with_github: programs.filter((item) => item.github_url).length,
      programs_with_idl: programs.filter((item) => item.has_idl).length,
      request_errors: programs.filter((item) => item.error).length,
    },
    programs,
  };
  await atomicWriteJson(outputPath, report);
}

async function atomicWriteJson(filePath, value) {
  const directory = path.dirname(filePath);
  await mkdir(directory, { recursive: true });
  const temporary = `${filePath}.tmp-${process.pid}`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, {
    flag: "w",
  });
  await rename(temporary, filePath);
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function positiveInteger(raw, name, maximum) {
  if (!/^\d+$/.test(raw)) throw new Error(`${name} must be a positive integer`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new Error(`${name} must be in 1..${maximum}`);
  }
  return value;
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}
