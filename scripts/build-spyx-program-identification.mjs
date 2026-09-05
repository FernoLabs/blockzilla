#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";

const [inventoryPath, resultDirectory, carbonDirectory] = process.argv.slice(2);
if (
  !inventoryPath ||
  !resultDirectory ||
  !carbonDirectory ||
  process.argv.length !== 5
) {
  console.error(
    "usage: node scripts/build-spyx-program-identification.mjs <program-inventory.json> <result-directory> <carbon-repository>",
  );
  process.exit(2);
}

const sourceDirectory = path.join(resultDirectory, "sources");
const sourcePaths = {
  onchain: path.join(resultDirectory, "onchain-program-metadata-summary.json"),
  usableIdls: path.join(
    resultDirectory,
    "onchain-probe/onchain-identified-programs.txt",
  ),
  strictIdls: path.join(
    resultDirectory,
    "onchain-probe/onchain-identified-programs-strict.txt",
  ),
  jupiter: path.join(sourceDirectory, "jupiter-program-id-to-label.json"),
  wassup: path.join(sourceDirectory, "wassup-program-labels.json"),
  vybe: path.join(sourceDirectory, "vybe-available-dexs-amms.md"),
  prism: path.join(sourceDirectory, "prism-client-sdk-76cf51b-README.md"),
  explorer: path.join(sourceDirectory, "solana-explorer-tx-17f644e.ts"),
  solanaFm: path.join(
    sourceDirectory,
    "solanafm-local-idl-repository-3d20d79.ts",
  ),
  osec: path.join(sourceDirectory, "osec-verified-programs-status.json"),
  manual: path.join(sourceDirectory, "manual-public-identities.json"),
};

const inputBytes = {};
for (const [name, filePath] of Object.entries({
  inventory: inventoryPath,
  ...sourcePaths,
})) {
  inputBytes[name] = await readFile(filePath);
}

const inventory = parseJson(inputBytes.inventory, "inventory");
const onchain = parseJson(inputBytes.onchain, "on-chain metadata");
const jupiter = parseJson(inputBytes.jupiter, "Jupiter program labels");
const wassup = parseJson(inputBytes.wassup, "public tracker labels");
const osec = parseJson(inputBytes.osec, "verified-build registry");
const manual = parseJson(inputBytes.manual, "manual identities");
const usableIdls = parseIdSet(inputBytes.usableIdls, "usable IDL set");
const strictIdls = parseIdSet(inputBytes.strictIdls, "strict IDL set");

if (!inventory.complete || !Array.isArray(inventory.programs)) {
  throw new Error("program inventory is not complete");
}
if (!onchain.complete || !Array.isArray(onchain.programs)) {
  throw new Error("on-chain metadata report is not complete");
}
if (!wassup.complete || !Array.isArray(wassup.programs)) {
  throw new Error("public tracker report is not complete");
}

const inventoryById = new Map();
for (const [index, program] of inventory.programs.entries()) {
  if (
    typeof program.program_id !== "string" ||
    inventoryById.has(program.program_id)
  ) {
    throw new Error("inventory contains a missing or duplicate program ID");
  }
  inventoryById.set(program.program_id, { ...program, rank: index + 1 });
}

const evidenceById = new Map(
  inventory.programs.map((program) => [program.program_id, []]),
);
const decoderSourceIds = new Set();
const ignoredEvidence = [];
const sourceMatches = new Map();
const excludedClassOnly = new Set(manual.excluded_class_only ?? []);

function addEvidence(programId, evidence) {
  if (!inventoryById.has(programId)) return;
  if (evidence.decoder_source === true) decoderSourceIds.add(programId);
  const name = cleanName(evidence.name);
  if (!name || isClassOnlyName(name)) {
    ignoredEvidence.push({ program_id: programId, ...evidence, name });
    return;
  }
  if (excludedClassOnly.has(programId) && isClassOnlyName(name)) return;
  const normalized = {
    source: evidence.source,
    name,
    confidence: evidence.confidence ?? "high",
    evidence_type: evidence.evidence_type,
    source_url: evidence.source_url,
    source_commit: evidence.source_commit ?? null,
    decoder_source: evidence.decoder_source ?? false,
    details: evidence.details ?? null,
  };
  const values = evidenceById.get(programId);
  if (
    !values.some(
      (value) =>
        value.source === normalized.source &&
        value.name === normalized.name &&
        value.source_url === normalized.source_url,
    )
  ) {
    values.push(normalized);
    sourceMatches.set(
      normalized.source,
      (sourceMatches.get(normalized.source) ?? 0) + 1,
    );
  }
}

for (const program of onchain.programs) {
  if (!inventoryById.has(program.program_id)) {
    throw new Error(`on-chain report has unknown program ${program.program_id}`);
  }
  if (program.identified) {
    const isIdl = program.decoder_schema_found === true;
    addEvidence(program.program_id, {
      source: isIdl ? "onchain_idl" : "onchain_security_txt",
      name: program.identity_name,
      confidence: "high",
      evidence_type: isIdl
        ? `onchain_${program.idl?.type ?? "unknown"}_idl`
        : `onchain_${program.security?.type ?? "unknown"}_security_txt`,
      source_url: isIdl
        ? idlApiUrl(program.program_id)
        : securityApiUrl(program.program_id),
      decoder_source: isIdl,
      details: isIdl
        ? {
            usable_idl: usableIdls.has(program.program_id),
            address_clean_idl: strictIdls.has(program.program_id),
            cache_file: program.idl?.cache_file ?? null,
            canonical_payload_sha256: program.idl?.sha256 ?? null,
          }
        : {
            cache_file: program.security?.cache_file ?? null,
            canonical_payload_sha256: program.security?.sha256 ?? null,
          },
    });
  }
}

for (const [programId, name] of Object.entries(jupiter)) {
  addEvidence(programId, {
    source: "jupiter_program_labels",
    name,
    evidence_type: "public_router_program_label",
    source_url: "https://lite-api.jup.ag/swap/v1/program-id-to-label",
  });
}

for (const item of wassup.programs) {
  if (!item.name) continue;
  addEvidence(item.program_id, {
    source: "public_program_tracker",
    name: item.name,
    confidence: item.github_url ? "high" : "medium",
    evidence_type: item.github_url
      ? "exact_program_label_with_source_link"
      : "exact_program_label",
    source_url: item.github_url ?? item.query_url,
    decoder_source: item.has_idl === true,
    details: {
      tracker_query_url: item.query_url,
      response_sha256: item.response_sha256,
    },
  });
}

for (const item of parseMarkdownProgramTable(inputBytes.vybe.toString("utf8"))) {
  addEvidence(item.programId, {
    source: "vybe_dex_registry",
    name: item.name,
    evidence_type: "indexed_dex_program_registry",
    source_url: "https://docs.vybenetwork.com/docs/available-dexs-amms",
  });
}

for (const item of parsePrismPrograms(inputBytes.prism.toString("utf8"))) {
  addEvidence(item.programId, {
    source: "prism_protocol_sdk",
    name: item.name,
    evidence_type:
      item.programId === "Prism8hsRo6Ww5jiN5Zeh3YDPLZHqHduCPSAV7JF7qv"
        ? "protocol_sdk_live_program"
        : "protocol_sdk_supported_program",
    source_url:
      "https://github.com/Hweippy/prism-client-sdk/blob/76cf51b0aeefb7bcf0f06a53723024e76b7293da/README.md",
    source_commit: "76cf51b0aeefb7bcf0f06a53723024e76b7293da",
  });
}

const carbonPrograms = await parseCarbonPrograms(carbonDirectory);
for (const item of carbonPrograms) {
  addEvidence(item.programId, {
    source: "carbon_decoder_registry",
    name: item.name,
    evidence_type: "published_generated_instruction_decoder",
    source_url: `https://github.com/sevenlabs-hq/carbon/tree/af70b199b39e60a1a33306e5411f8040374f8d9a/decoders/${item.packageName}`,
    source_commit: "af70b199b39e60a1a33306e5411f8040374f8d9a",
    decoder_source: true,
  });
}

for (const item of parseExplorerPrograms(inputBytes.explorer.toString("utf8"))) {
  addEvidence(item.programId, {
    source: "solana_foundation_explorer",
    name: item.name,
    evidence_type: "official_explorer_program_registry",
    source_url:
      "https://github.com/solana-foundation/explorer/blob/17f644efa1f6f174e1dd7bc46bc397ca55f0a004/app/utils/tx.ts",
    source_commit: "17f644efa1f6f174e1dd7bc46bc397ca55f0a004",
  });
}

for (const item of parseSolanaFmPrograms(inputBytes.solanaFm.toString("utf8"))) {
  addEvidence(item.programId, {
    source: "solanafm_local_idl_registry",
    name: item.name,
    evidence_type: "published_local_instruction_schema",
    source_url:
      "https://github.com/solana-fm/explorer-kit/blob/3d20d79948237e0fb9de87420ade33c49c0041d6/packages/explorerkit-idls/src/idls/LocalIdlRepository.ts",
    source_commit: "3d20d79948237e0fb9de87420ade33c49c0041d6",
    decoder_source: true,
  });
}

for (const item of osec.data ?? []) {
  if (!item.is_verified || typeof item.repo_url !== "string") continue;
  addEvidence(item.program_id, {
    source: "ottersec_verified_build",
    name: repositoryIdentity(item.repo_url),
    confidence: "high",
    evidence_type: "onchain_binary_matches_public_source_commit",
    source_url: item.repo_url,
    source_commit: item.commit ?? null,
    decoder_source: false,
    details: { on_chain_hash: item.on_chain_hash ?? null },
  });
}

for (const item of manual.accepted ?? []) {
  addEvidence(item.program_id, {
    source: item.source ?? "targeted_public_search",
    name: item.name,
    confidence: item.confidence,
    evidence_type: item.evidence_type,
    source_url: item.source_url,
    source_commit: item.repo_commit ?? null,
  });
}

const sourcePriority = new Map([
  ["targeted_public_search", 100],
  ["program_execution_log", 98],
  ["jupiter_program_labels", 95],
  ["prism_protocol_sdk", 92],
  ["vybe_dex_registry", 90],
  ["onchain_security_txt", 88],
  ["onchain_idl", 86],
  ["carbon_decoder_registry", 84],
  ["solana_foundation_explorer", 82],
  ["solanafm_local_idl_registry", 80],
  ["ottersec_verified_build", 78],
  ["public_program_tracker", 70],
]);

const programs = [];
for (const inventoryProgram of inventory.programs) {
  const programId = inventoryProgram.program_id;
  const evidence = evidenceById.get(programId);
  evidence.sort(compareEvidence);
  const selected = evidence[0] ?? null;
  programs.push({
    rank: programs.length + 1,
    registry_id: inventoryProgram.registry_id,
    program_id: programId,
    identity_status: selected ? "identified" : "unidentified",
    selected_name: selected?.name ?? null,
    selected_source: selected?.source ?? null,
    selected_confidence: selected?.confidence ?? null,
    usable_onchain_idl: usableIdls.has(programId),
    address_clean_onchain_idl: strictIdls.has(programId),
    decoder_source_found: decoderSourceIds.has(programId),
    total_occurrences: inventoryProgram.total_occurrences,
    outer_occurrences: inventoryProgram.outer_occurrences,
    inner_occurrences: inventoryProgram.inner_occurrences,
    transactions: inventoryProgram.transactions,
    evidence,
  });
}

const identified = programs.filter(
  (program) => program.identity_status === "identified",
);
const unidentified = programs.filter(
  (program) => program.identity_status === "unidentified",
);
const decoderPrograms = programs.filter((program) => program.decoder_source_found);
const totalOccurrences =
  inventory.counters.outer_occurrences + inventory.counters.inner_occurrences;
const identifiedOccurrences = sum(identified, "total_occurrences");
const decoderOccurrences = sum(decoderPrograms, "total_occurrences");

const report = {
  schema_version: 1,
  artifact_kind: "spyx_program_identification",
  complete: true,
  generated_at: new Date().toISOString(),
  definitions: {
    identified:
      "An exact program ID has a usable on-chain name or an exact public-source identity. Generic class labels such as Arbitrage Bot do not qualify.",
    decoder_source_found:
      "A usable on-chain IDL or a published decoder/schema was found. This does not prove historical decoder correctness.",
    instruction_coverage:
      "The share of all outer and inner instruction occurrences assigned to an identified program. This is not unique transaction coverage.",
  },
  source: {
    inventory_path: path.resolve(inventoryPath),
    inventory_sha256: sha256(inputBytes.inventory),
    dump_manifest_sha256: inventory.source.manifest_sha256,
    dump_transaction_stream_sha256:
      inventory.source.transaction_stream_sha256,
    dump_pubkey_registry_sha256: inventory.source.pubkey_registry_sha256,
    first_epoch: inventory.source.first_epoch,
    last_epoch: inventory.source.last_epoch,
    sources: sourceManifest(),
  },
  counters: {
    transactions: inventory.counters.transactions,
    programs_total: programs.length,
    programs_identified: identified.length,
    programs_unidentified: unidentified.length,
    identified_program_ratio: ratio(identified.length, programs.length),
    programs_named_onchain: onchain.counters.programs_with_names,
    programs_added_by_public_sources:
      identified.filter(
        (program) =>
          !program.evidence.some((item) => item.source.startsWith("onchain_")),
      ).length,
    usable_onchain_idls: usableIdls.size,
    address_clean_onchain_idls: strictIdls.size,
    programs_with_any_decoder_source: decoderPrograms.length,
    decoder_source_program_ratio: ratio(decoderPrograms.length, programs.length),
    instruction_occurrences_total: totalOccurrences,
    identified_instruction_occurrences: identifiedOccurrences,
    unidentified_instruction_occurrences: totalOccurrences - identifiedOccurrences,
    identified_instruction_occurrence_ratio: ratio(
      identifiedOccurrences,
      totalOccurrences,
    ),
    decoder_source_instruction_occurrences: decoderOccurrences,
    decoder_source_instruction_occurrence_ratio: ratio(
      decoderOccurrences,
      totalOccurrences,
    ),
    identified_outer_occurrences: sum(identified, "outer_occurrences"),
    identified_inner_occurrences: sum(identified, "inner_occurrences"),
    ignored_generic_or_empty_evidence: ignoredEvidence.length,
    programs_explicitly_excluded_as_class_only: excludedClassOnly.size,
  },
  source_match_counts: Object.fromEntries(
    [...sourceMatches.entries()].sort((left, right) => left[0].localeCompare(right[0])),
  ),
  programs,
};

await mkdir(resultDirectory, { recursive: true });
await Promise.all([
  atomicWriteJson(
    path.join(resultDirectory, "program-identification-report.json"),
    report,
  ),
  atomicWrite(
    path.join(resultDirectory, "identified-program-ids.txt"),
    `${identified.map((program) => program.program_id).join("\n")}\n`,
  ),
  atomicWrite(
    path.join(resultDirectory, "decoder-source-program-ids.txt"),
    `${decoderPrograms.map((program) => program.program_id).join("\n")}\n`,
  ),
  atomicWrite(
    path.join(resultDirectory, "unidentified-programs.tsv"),
    [
      "rank\tprogram_id\ttransactions\ttotal_occurrences\touter_occurrences\tinner_occurrences",
      ...unidentified.map((program) =>
        [
          program.rank,
          program.program_id,
          program.transactions,
          program.total_occurrences,
          program.outer_occurrences,
          program.inner_occurrences,
        ].join("\t"),
      ),
      "",
    ].join("\n"),
  ),
]);

console.error(
  `identified ${identified.length}/${programs.length} programs; ` +
    `${identifiedOccurrences}/${totalOccurrences} instruction occurrences; ` +
    `decoder sources ${decoderPrograms.length}/${programs.length}`,
);

function compareEvidence(left, right) {
  const confidence = { high: 3, medium: 2, low: 1 };
  return (
    (confidence[right.confidence] ?? 0) - (confidence[left.confidence] ?? 0) ||
    (sourcePriority.get(right.source) ?? 0) -
      (sourcePriority.get(left.source) ?? 0) ||
    left.name.localeCompare(right.name)
  );
}

function parseMarkdownProgramTable(text) {
  const output = [];
  for (const line of text.split(/\r?\n/)) {
    const match = line.match(
      /^\|\s*(.*?)\s*\|\s*([1-9A-HJ-NP-Za-km-z]{32,44})\s*\|/,
    );
    if (match) output.push({ name: match[1].trim(), programId: match[2] });
  }
  return output;
}

function parsePrismPrograms(text) {
  const output = [];
  const live = text.match(
    /The live program is \[`([^`]+)`\]\([^)]*\)/,
  );
  if (live) output.push({ name: "Prism", programId: live[1] });
  for (const line of text.split(/\r?\n/)) {
    const match = line.match(
      /^\|\s*\[([^\]]+)\][^|]*\|\s*\[`([1-9A-HJ-NP-Za-km-z]{32,44})`\]/,
    );
    if (match) output.push({ name: match[1].trim(), programId: match[2] });
  }
  return output;
}

async function parseCarbonPrograms(repository) {
  const decoderDirectory = path.join(repository, "decoders");
  const entries = await readdir(decoderDirectory, { withFileTypes: true });
  const output = [];
  for (const entry of entries) {
    if (!entry.isDirectory() || !entry.name.endsWith("-decoder")) continue;
    const libPath = path.join(decoderDirectory, entry.name, "src/lib.rs");
    let text;
    try {
      text = await readFile(libPath, "utf8");
    } catch (error) {
      if (error?.code === "ENOENT") continue;
      throw error;
    }
    const match = text.match(
      /PROGRAM_ID:[\s\S]{0,160}?from_str_const\("([1-9A-HJ-NP-Za-km-z]{32,44})"\)/,
    );
    if (!match) continue;
    const rawName = entry.name.replace(/-decoder$/, "");
    output.push({
      packageName: entry.name,
      name: titleName(rawName),
      programId: match[1],
    });
  }
  return output;
}

function parseExplorerPrograms(text) {
  const enumBlock = text.match(/export enum PROGRAM_NAMES \{([\s\S]*?)\n\}/)?.[1] ?? "";
  const names = new Map();
  for (const match of enumBlock.matchAll(/^\s*([A-Z0-9_]+)\s*=\s*'([^']+)'/gm)) {
    names.set(match[1], match[2]);
  }
  const objectBlock =
    text.match(/export const PROGRAM_INFO_BY_ID[^=]*= \{([\s\S]*?)\n\};/)?.[1] ??
    "";
  const output = [];
  const pattern =
    /^\s*(?:'([1-9A-HJ-NP-Za-km-z]{32,44})'|([1-9A-HJ-NP-Za-km-z]{32,44})):\s*\{([\s\S]*?)^\s*\},/gm;
  for (const match of objectBlock.matchAll(pattern)) {
    const nameKey = match[3].match(/name:\s*PROGRAM_NAMES\.([A-Z0-9_]+)/)?.[1];
    const name = names.get(nameKey);
    if (name) output.push({ programId: match[1] ?? match[2], name });
  }
  return output;
}

function parseSolanaFmPrograms(text) {
  const output = [];
  const pattern =
    /\["([1-9A-HJ-NP-Za-km-z]{32,44})",\s*new Map\(\[\[0,\s*([A-Za-z0-9_]+)\]\]\)\]/g;
  for (const match of text.matchAll(pattern)) {
    output.push({
      programId: match[1],
      name: titleName(
        match[2]
          .replace(/KinobiTree$/, "")
          .replace(/IDL$/, "")
          .replace(/([a-z0-9])([A-Z])/g, "$1-$2"),
      ),
    });
  }
  return output;
}

function sourceManifest() {
  const details = {
    onchain: {
      kind: "official_onchain_idl_and_security_resolution",
      url: "https://idl.solana.com/docs",
    },
    usableIdls: { kind: "direct_rpc_usable_idl_set" },
    strictIdls: { kind: "direct_rpc_address_clean_idl_set" },
    jupiter: {
      url: "https://lite-api.jup.ag/swap/v1/program-id-to-label",
    },
    wassup: { url: "https://wassup.trenchscreener.ai/" },
    vybe: { url: "https://docs.vybenetwork.com/docs/available-dexs-amms" },
    prism: {
      url: "https://github.com/Hweippy/prism-client-sdk",
      commit: "76cf51b0aeefb7bcf0f06a53723024e76b7293da",
    },
    explorer: {
      url: "https://github.com/solana-foundation/explorer",
      commit: "17f644efa1f6f174e1dd7bc46bc397ca55f0a004",
    },
    solanaFm: {
      url: "https://github.com/solana-fm/explorer-kit",
      commit: "3d20d79948237e0fb9de87420ade33c49c0041d6",
    },
    osec: { url: "https://verify.osec.io/verified-programs-status" },
    manual: { kind: "targeted_exact_id_public_search" },
    carbon: {
      url: "https://github.com/sevenlabs-hq/carbon",
      commit: "af70b199b39e60a1a33306e5411f8040374f8d9a",
      parsed_decoder_packages: carbonPrograms.length,
    },
  };
  for (const [name, bytes] of Object.entries(inputBytes)) {
    if (name === "inventory") continue;
    details[name] = {
      ...details[name],
      local_path: path.resolve(sourcePaths[name]),
      sha256: sha256(bytes),
    };
  }
  return details;
}

function repositoryIdentity(repoUrl) {
  try {
    const segments = new URL(repoUrl).pathname.split("/").filter(Boolean);
    if (segments.length >= 2) return `${segments[0]}/${segments[1]}`;
  } catch {}
  return repoUrl;
}

function idlApiUrl(programId) {
  return `https://idl.solana.com/api/idl?programId=${programId}&cluster=mainnet-beta`;
}

function securityApiUrl(programId) {
  return `https://idl.solana.com/api/security-txt?programId=${programId}&cluster=mainnet-beta`;
}

function isClassOnlyName(name) {
  const normalized = name.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim();
  return (
    /^arbitrage bot(?:\s|\(|$)/i.test(normalized) ||
    /^unknown program/i.test(normalized)
  );
}

function cleanName(value) {
  if (typeof value !== "string") return null;
  const name = value.replace(/\s+/g, " ").trim();
  return name && name !== "-" ? name : null;
}

function titleName(value) {
  const acronyms = new Map([
    ["amm", "AMM"],
    ["clmm", "CLMM"],
    ["cpmm", "CPMM"],
    ["damm", "DAMM"],
    ["dca", "DCA"],
    ["dex", "DEX"],
    ["dlmm", "DLMM"],
    ["idl", "IDL"],
    ["mpl", "MPL"],
    ["nft", "NFT"],
    ["v1", "v1"],
    ["v2", "v2"],
    ["v3", "v3"],
    ["v4", "v4"],
  ]);
  return value
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((word) => acronyms.get(word.toLowerCase()) ?? `${word[0].toUpperCase()}${word.slice(1)}`)
    .join(" ");
}

function parseIdSet(bytes, label) {
  const values = bytes
    .toString("utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));
  if (new Set(values).size !== values.length) {
    throw new Error(`${label} contains duplicate IDs`);
  }
  return new Set(values);
}

function parseJson(bytes, label) {
  try {
    return JSON.parse(bytes.toString("utf8"));
  } catch (error) {
    throw new Error(`${label} is not valid JSON`, { cause: error });
  }
}

function sum(programsToSum, field) {
  return programsToSum.reduce((total, program) => total + program[field], 0);
}

function ratio(numerator, denominator) {
  return Number((numerator / denominator).toFixed(12));
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

async function atomicWriteJson(filePath, value) {
  await atomicWrite(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

async function atomicWrite(filePath, value) {
  const temporary = `${filePath}.tmp-${process.pid}`;
  await writeFile(temporary, value, { flag: "w" });
  await rename(temporary, filePath);
}
