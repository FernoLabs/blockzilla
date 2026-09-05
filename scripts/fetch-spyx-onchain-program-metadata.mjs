#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

const [inventoryPath, outputDirectory] = process.argv.slice(2);
if (!inventoryPath || !outputDirectory || process.argv.length !== 4) {
  console.error(
    "usage: node scripts/fetch-spyx-onchain-program-metadata.mjs <program-inventory.json> <output-directory>",
  );
  process.exit(2);
}

const concurrency = parsePositiveInteger(
  process.env.SPYX_METADATA_CONCURRENCY ?? "6",
  "SPYX_METADATA_CONCURRENCY",
  16,
);
const apiBase = (process.env.SPYX_IDL_API_BASE ?? "https://idl.solana.com").replace(
  /\/$/,
  "",
);

const inventoryBytes = await readFile(inventoryPath);
const inventory = JSON.parse(inventoryBytes.toString("utf8"));
if (!Array.isArray(inventory.programs) || inventory.programs.length === 0) {
  throw new Error("inventory does not contain a non-empty programs array");
}

const programIds = new Set();
for (const program of inventory.programs) {
  if (
    typeof program.program_id !== "string" ||
    program.program_id.length === 0 ||
    programIds.has(program.program_id)
  ) {
    throw new Error("inventory contains a missing or duplicate program_id");
  }
  programIds.add(program.program_id);
}

const idlDirectory = path.join(outputDirectory, "onchain-idls");
const idlMissDirectory = path.join(outputDirectory, "onchain-idl-misses");
const securityDirectory = path.join(outputDirectory, "onchain-security");
const securityMissDirectory = path.join(
  outputDirectory,
  "onchain-security-misses",
);
await Promise.all([
  mkdir(idlDirectory, { recursive: true }),
  mkdir(idlMissDirectory, { recursive: true }),
  mkdir(securityDirectory, { recursive: true }),
  mkdir(securityMissDirectory, { recursive: true }),
]);

let completed = 0;
let idlsFound = 0;
let securityFilesFound = 0;
let namesFound = 0;
let requestErrors = 0;
const results = new Array(inventory.programs.length);
let nextIndex = 0;

async function worker() {
  while (true) {
    const index = nextIndex++;
    if (index >= inventory.programs.length) return;
    const program = inventory.programs[index];
    try {
      results[index] = await fetchProgram(program, index + 1);
      if (results[index].idl.found) idlsFound += 1;
      if (results[index].security.found) securityFilesFound += 1;
      if (results[index].identified) namesFound += 1;
    } catch (error) {
      requestErrors += 1;
      results[index] = {
        rank: index + 1,
        ...inventoryFields(program),
        identified: false,
        decoder_schema_found: false,
        identity_name: null,
        identity_source: null,
        idl: { found: false, error: errorMessage(error) },
        security: { found: false, error: "not requested after IDL error" },
      };
    }
    completed += 1;
    if (completed % 25 === 0 || completed === inventory.programs.length) {
      console.error(
        `on-chain metadata: ${completed}/${inventory.programs.length}, IDLs=${idlsFound}, security=${securityFilesFound}, named=${namesFound}, errors=${requestErrors}`,
      );
    }
  }
}

await Promise.all(Array.from({ length: concurrency }, () => worker()));

const summary = {
  schema_version: 1,
  artifact_kind: "spyx_program_onchain_metadata",
  complete: requestErrors === 0,
  generated_at: new Date().toISOString(),
  source: {
    inventory_path: path.resolve(inventoryPath),
    inventory_sha256: sha256(inventoryBytes),
    inventory_programs: inventory.programs.length,
    inventory_transactions: inventory.counters?.transactions ?? null,
    inventory_instruction_occurrences:
      (inventory.counters?.outer_occurrences ?? 0) +
      (inventory.counters?.inner_occurrences ?? 0),
    api_base: apiBase,
    resolution_order: [
      "canonical Program Metadata Program IDL",
      "Solana Foundation fallback Program Metadata Program IDL",
      "legacy Anchor IDL",
      "Program Metadata Program security.txt",
      "ELF security.txt",
    ],
  },
  counters: {
    programs: inventory.programs.length,
    idls_found: idlsFound,
    security_files_found: securityFilesFound,
    programs_with_names: namesFound,
    request_errors: requestErrors,
  },
  programs: results,
};

const summaryPath = path.join(
  outputDirectory,
  "onchain-program-metadata-summary.json",
);
await atomicWriteJson(summaryPath, summary);
console.error(`wrote ${summaryPath}`);
if (!summary.complete) process.exitCode = 1;

async function fetchProgram(program, rank) {
  const programId = program.program_id;
  const idl = await cachedRequest(
    "idl",
    programId,
    idlDirectory,
    idlMissDirectory,
  );
  const idlName = idl.found ? extractIdlName(idl.payload, programId) : null;
  const security =
    idlName === null
      ? await cachedRequest(
          "security-txt",
          programId,
          securityDirectory,
          securityMissDirectory,
        )
      : { found: false, skipped: "IDL supplied a program name" };
  const securityName = security.found
    ? extractSecurityName(security.payload)
    : null;
  const identityName = idlName ?? securityName;

  return {
    rank,
    ...inventoryFields(program),
    identified: identityName !== null,
    decoder_schema_found: idl.found && idl.payload?.valid !== false,
    identity_name: identityName,
    identity_source: idlName
      ? `onchain_idl_${idl.payload?.type ?? "unknown"}`
      : securityName
        ? `onchain_security_${security.payload?.type ?? "unknown"}`
        : null,
    idl: summarizeHit(idl, outputDirectory, extractIdlName(idl.payload, programId)),
    security: summarizeHit(
      security,
      outputDirectory,
      security.found ? extractSecurityName(security.payload) : null,
    ),
  };
}

function inventoryFields(program) {
  return {
    registry_id: program.registry_id,
    program_id: program.program_id,
    total_occurrences: program.total_occurrences,
    outer_occurrences: program.outer_occurrences,
    inner_occurrences: program.inner_occurrences,
    transactions: program.transactions,
  };
}

async function cachedRequest(kind, programId, hitDirectory, missDirectory) {
  const hitPath = path.join(hitDirectory, `${programId}.json`);
  const missPath = path.join(missDirectory, `${programId}.json`);
  const cachedHit = await readJsonIfPresent(hitPath);
  if (cachedHit !== null) {
    return { found: true, payload: cachedHit, path: hitPath, cached: true };
  }
  const cachedMiss = await readJsonIfPresent(missPath);
  if (cachedMiss !== null) {
    return {
      found: false,
      status: cachedMiss.http_status,
      path: missPath,
      cached: true,
    };
  }

  const response = await requestWithRetry(kind, programId);
  if (response.found) {
    await atomicWriteJson(hitPath, response.payload);
    return { ...response, path: hitPath, cached: false };
  }
  await atomicWriteJson(missPath, {
    program_id: programId,
    endpoint: kind,
    http_status: response.status,
    checked_at: new Date().toISOString(),
  });
  return { ...response, path: missPath, cached: false };
}

async function requestWithRetry(kind, programId) {
  const url = new URL(`/api/${kind}`, apiBase);
  url.searchParams.set("programId", programId);
  url.searchParams.set("cluster", "mainnet-beta");

  let lastError;
  for (let attempt = 0; attempt < 6; attempt += 1) {
    try {
      const response = await fetch(url, {
        headers: {
          accept: "application/json",
          "user-agent": "blockzilla-spyx-program-inventory/1",
        },
        signal: AbortSignal.timeout(45_000),
      });
      if (response.status === 404 || response.status === 422) {
        return { found: false, status: response.status };
      }
      if (response.ok) {
        const payload = await response.json();
        if (
          payload === null ||
          typeof payload !== "object" ||
          (payload.programId !== undefined && payload.programId !== programId)
        ) {
          throw new Error(`${kind} returned an invalid program binding`);
        }
        return { found: true, status: response.status, payload };
      }
      if (response.status !== 429 && response.status < 500) {
        throw new Error(`${kind} returned HTTP ${response.status}`);
      }
      const retryAfter = Number(response.headers.get("retry-after"));
      const delay = Number.isFinite(retryAfter)
        ? Math.max(1_000, retryAfter * 1_000)
        : Math.min(20_000, 750 * 2 ** attempt);
      await wait(delay);
    } catch (error) {
      lastError = error;
      if (attempt < 5) await wait(Math.min(20_000, 750 * 2 ** attempt));
    }
  }
  throw new Error(
    `${kind} request failed after retries: ${errorMessage(lastError)}`,
  );
}

function extractIdlName(payload, programId) {
  if (!payload || payload.valid === false) return null;
  const idl = payload.idl;
  const candidates = [
    idl?.metadata?.name,
    idl?.name,
    idl?.program?.name,
    idl?.program?.metadata?.name,
  ];
  if (Array.isArray(idl?.program?.programs)) {
    const exact = idl.program.programs.find(
      (program) => program?.publicKey === programId || program?.address === programId,
    );
    candidates.push(exact?.name);
    if (idl.program.programs.length === 1) {
      candidates.push(idl.program.programs[0]?.name);
    }
  }
  if (Array.isArray(idl?.programs)) {
    const exact = idl.programs.find(
      (program) => program?.publicKey === programId || program?.address === programId,
    );
    candidates.push(exact?.name);
    if (idl.programs.length === 1) candidates.push(idl.programs[0]?.name);
  }
  return firstText(candidates);
}

function extractSecurityName(payload) {
  return firstText([
    payload?.fields?.name,
    payload?.metadata?.name,
    payload?.name,
  ]);
}

function firstText(values) {
  for (const value of values) {
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return null;
}

function summarizeHit(hit, root, name) {
  if (hit.skipped) return { found: false, skipped: hit.skipped };
  if (!hit.found) {
    return {
      found: false,
      http_status: hit.status ?? null,
      cache_file: hit.path ? path.relative(root, hit.path) : null,
    };
  }
  const bytes = Buffer.from(`${JSON.stringify(hit.payload)}\n`);
  return {
    found: true,
    type: hit.payload?.type ?? null,
    valid: hit.payload?.valid ?? null,
    address: hit.payload?.address ?? null,
    authority: hit.payload?.authority ?? null,
    name,
    sha256: sha256(bytes),
    cache_file: path.relative(root, hit.path),
  };
}

async function readJsonIfPresent(filePath) {
  try {
    return JSON.parse(await readFile(filePath, "utf8"));
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

async function atomicWriteJson(filePath, value) {
  const bytes = `${JSON.stringify(value, null, 2)}\n`;
  const temporaryPath = `${filePath}.${process.pid}.partial`;
  await writeFile(temporaryPath, bytes, { flag: "wx" });
  await rename(temporaryPath, filePath);
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function parsePositiveInteger(value, name, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new Error(`${name} must be an integer from 1 through ${maximum}`);
  }
  return parsed;
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function wait(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
