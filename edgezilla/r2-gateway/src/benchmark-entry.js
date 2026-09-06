var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/benchmark.ts
var MAX_RANGE_BYTES = 64 * 1024 * 1024;
var MAX_FULL_JSON_CONTROL_BYTES = 4 * 1024 * 1024;
var MAX_FULL_CONTROL_BYTES = 8 * 1024 * 1024;
var MAX_ALLOWLIST_BYTES = 4096;
var MAX_ALLOWLIST_ENTRIES = 32;
var MAX_RELEASE_MAP_BYTES = 8192;
var MAX_RELEASE_MAP_ENTRIES = 64;
var MAX_RELEASE_PREFIX_BYTES = 512;
var MAX_IP_TEXT_BYTES = 64;
var MAX_U64 = (1n << 64n) - 1n;
var NO_STORE = "no-store";
var encoder = new TextEncoder();
function binaryRangeOnly(name) {
  return {
    name,
    contentType: "application/octet-stream",
    fullGetLimit: null
  };
}
__name(binaryRangeOnly, "binaryRangeOnly");
function binarySmallControl(name) {
  return {
    name,
    contentType: "application/octet-stream",
    fullGetLimit: MAX_FULL_CONTROL_BYTES
  };
}
__name(binarySmallControl, "binarySmallControl");
function jsonSmallControl(name) {
  return {
    name,
    contentType: "application/json; charset=utf-8",
    fullGetLimit: MAX_FULL_JSON_CONTROL_BYTES
  };
}
__name(jsonSmallControl, "jsonSmallControl");
function fileMap(files) {
  return new Map(files.map((file) => [file.name, file]));
}
__name(fileMap, "fileMap");
var COMPACT_FILES = fileMap([
  binaryRangeOnly("archive-v2-blocks.zstd"),
  binarySmallControl("archive-v2-blocks.index"),
  binaryRangeOnly("archive-v2-meta.wincode"),
  binarySmallControl("registry.bin"),
  binarySmallControl("registry.mphf"),
  binarySmallControl("registry_counts.bin"),
  binaryRangeOnly("blockhash_registry.bin"),
  binarySmallControl("prev_blockhash_tail.bin"),
  binaryRangeOnly("vote_hash_registry.bin"),
  binaryRangeOnly("poh.wincode"),
  binaryRangeOnly("shredding.wincode"),
  binaryRangeOnly("signatures.bin")
]);
var INDEXER_V3_FILES = fileMap([
  jsonSmallControl("archive-v2-retained-sidecars.candidate.json"),
  binarySmallControl("archive-v2-standalone-blocks.index"),
  binarySmallControl("archive-v2-standalone-transaction-directory.wincode"),
  binaryRangeOnly("archive-v2-standalone-messages.wincode"),
  binaryRangeOnly("archive-v2-standalone-loaded-addresses.wincode"),
  binaryRangeOnly("archive-v2-standalone-inner-instructions.wincode"),
  binaryRangeOnly("archive-v2-standalone-logs.wincode"),
  binaryRangeOnly("archive-v2-standalone-token-balances.wincode"),
  binaryRangeOnly("archive-v2-standalone-balances.wincode"),
  binaryRangeOnly("archive-v2-standalone-outcomes.wincode"),
  binaryRangeOnly("archive-v2-standalone-transaction-rewards.wincode"),
  binaryRangeOnly("archive-v2-standalone-raw-metadata-fallbacks.wincode"),
  binaryRangeOnly("archive-v2-standalone-block-rewards.wincode"),
  binaryRangeOnly("archive-v2-meta.wincode"),
  binaryRangeOnly("blockhash_registry.bin"),
  binarySmallControl("prev_blockhash_tail.bin"),
  binaryRangeOnly("vote_hash_registry.bin"),
  binaryRangeOnly("poh.wincode"),
  binaryRangeOnly("shredding.wincode"),
  binaryRangeOnly("signatures.bin"),
  binaryRangeOnly("archive-v2-standalone-account-postings-adaptive-v3.pages"),
  binarySmallControl(
    "archive-v2-standalone-account-postings-adaptive-v3.control"
  ),
  binaryRangeOnly(
    "archive-v2-standalone-account-postings-adaptive-v3.coverage"
  ),
  binarySmallControl("registry.bin"),
  binarySmallControl("registry.mphf")
]);
var BenchmarkHttpError = class extends Error {
  static {
    __name(this, "BenchmarkHttpError");
  }
  status;
  code;
  headers;
  constructor(status, code, headers = {}) {
    super(code);
    this.name = "BenchmarkHttpError";
    this.status = status;
    this.code = code;
    this.headers = headers;
  }
};
function isBenchmarkPath(pathname) {
  return pathname === "/car" || pathname.startsWith("/car/") || pathname === "/compact-v2" || pathname.startsWith("/compact-v2/") || pathname === "/indexer-v3" || pathname.startsWith("/indexer-v3/");
}
__name(isBenchmarkPath, "isBenchmarkPath");
function hasAsciiControl(value) {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 31 || code === 127) {
      return true;
    }
  }
  return false;
}
__name(hasAsciiControl, "hasAsciiControl");
function decodePathSegment(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new BenchmarkHttpError(400, "invalid_path_encoding");
  }
}
__name(decodePathSegment, "decodePathSegment");
function isSafeObjectName(value) {
  return value.length > 0 && value !== "." && value !== ".." && !value.includes("/") && !value.includes("\\") && !hasAsciiControl(value) && encoder.encode(value).byteLength <= 512;
}
__name(isSafeObjectName, "isSafeObjectName");
function parseEpoch(value) {
  if (value.length > 20 || !/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new BenchmarkHttpError(400, "invalid_epoch");
  }
  if (BigInt(value) > MAX_U64) {
    throw new BenchmarkHttpError(400, "invalid_epoch");
  }
  return value;
}
__name(parseEpoch, "parseEpoch");
function objectKey(format, epoch, name) {
  return `${format}/epoch-${epoch}/${name}`;
}
__name(objectKey, "objectKey");
function releaseMapKey(format, epoch) {
  return `${format}:${epoch}`;
}
__name(releaseMapKey, "releaseMapKey");
function isSafeReleasePrefix(format, value) {
  if (encoder.encode(value).byteLength > MAX_RELEASE_PREFIX_BYTES || hasAsciiControl(value) || value.includes("\\") || value.startsWith("/") || value.endsWith("/") || !value.startsWith(`${format}/`)) {
    return false;
  }
  const segments = value.split("/");
  return segments.length >= 2 && segments.every((segment) => /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(segment) && segment !== "." && segment !== "..");
}
__name(isSafeReleasePrefix, "isSafeReleasePrefix");
function configuredReleaseMap(env) {
  const configured = Reflect.get(env, "BENCHMARK_RELEASE_MAP");
  if (typeof configured !== "string" || configured.length === 0 || configured !== configured.trim() || encoder.encode(configured).byteLength > MAX_RELEASE_MAP_BYTES) {
    throw new BenchmarkHttpError(500, "invalid_release_map_configuration");
  }
  const entries = configured.split(",");
  if (entries.length === 0 || entries.length > MAX_RELEASE_MAP_ENTRIES) {
    throw new BenchmarkHttpError(500, "invalid_release_map_configuration");
  }
  const routes = /* @__PURE__ */ new Map();
  const prefixes = /* @__PURE__ */ new Set();
  for (const entry of entries) {
    const match = /^(compact-v2|indexer-v3):(0|[1-9][0-9]*)=(.+)$/.exec(entry);
    if (match === null || match[1] === void 0 || match[2] === void 0 || match[3] === void 0 || match[2].length > 20 || BigInt(match[2]) > MAX_U64 || !isSafeReleasePrefix(match[1], match[3])) {
      throw new BenchmarkHttpError(500, "invalid_release_map_configuration");
    }
    const key = releaseMapKey(match[1], match[2]);
    if (routes.has(key) || prefixes.has(match[3])) {
      throw new BenchmarkHttpError(500, "invalid_release_map_configuration");
    }
    routes.set(key, match[3]);
    prefixes.add(match[3]);
  }
  return routes;
}
__name(configuredReleaseMap, "configuredReleaseMap");
function carRoute(segments) {
  if (segments.length !== 4) {
    throw new BenchmarkHttpError(404, "not_found");
  }
  const epoch = parseEpoch(decodePathSegment(segments[2] ?? ""));
  const name = decodePathSegment(segments[3] ?? "");
  const carName = `epoch-${epoch}.car`;
  const slotIndexName = `epoch-${epoch}-slot-ranges.raw`;
  let file;
  if (name === carName) {
    file = {
      name,
      contentType: "application/vnd.ipld.car",
      fullGetLimit: null
    };
  } else if (name === slotIndexName) {
    file = binarySmallControl(name);
  } else {
    throw new BenchmarkHttpError(404, "file_not_published");
  }
  return {
    ...file,
    format: "car",
    epoch,
    key: objectKey("car", epoch, name)
  };
}
__name(carRoute, "carRoute");
function indexedRoute(format, segments, releaseMap) {
  if (segments[2] !== "v1" || segments[3] !== "epochs" || segments.length !== 7 || segments[5] !== "files") {
    throw new BenchmarkHttpError(404, "not_found");
  }
  const epoch = parseEpoch(decodePathSegment(segments[4] ?? ""));
  const name = decodePathSegment(segments[6] ?? "");
  if (!isSafeObjectName(name)) {
    throw new BenchmarkHttpError(400, "invalid_file_name");
  }
  const file = format === "compact-v2" ? COMPACT_FILES.get(name) : INDEXER_V3_FILES.get(name);
  if (file === void 0) {
    throw new BenchmarkHttpError(404, "file_not_published");
  }
  const prefix = releaseMap.get(releaseMapKey(format, epoch));
  if (prefix === void 0) {
    throw new BenchmarkHttpError(404, "benchmark_release_not_published");
  }
  return {
    ...file,
    format,
    epoch,
    prefix,
    key: `${prefix}/${name}`
  };
}
__name(indexedRoute, "indexedRoute");
function parseRoute(pathname, env) {
  const segments = pathname.split("/");
  if (segments[0] !== "") {
    throw new BenchmarkHttpError(404, "not_found");
  }
  switch (segments[1]) {
    case "car":
      return carRoute(segments);
    case "compact-v2":
      return indexedRoute("compact-v2", segments, configuredReleaseMap(env));
    case "indexer-v3":
      return indexedRoute("indexer-v3", segments, configuredReleaseMap(env));
    default:
      throw new BenchmarkHttpError(404, "not_found");
  }
}
__name(parseRoute, "parseRoute");
function rangeError(size) {
  return new BenchmarkHttpError(416, "range_not_satisfiable", {
    "Accept-Ranges": "bytes",
    "Content-Range": `bytes */${size}`
  });
}
__name(rangeError, "rangeError");
function parseSingleRange(value) {
  const match = /^bytes=([0-9]+)-([0-9]+)$/.exec(value);
  if (match === null || match[1] === void 0 || match[2] === void 0 || match[1].length > 20 || match[2].length > 20) {
    return void 0;
  }
  const start = BigInt(match[1]);
  const end = BigInt(match[2]);
  const length = end - start + 1n;
  if (start > end || end > BigInt(Number.MAX_SAFE_INTEGER) || length > BigInt(MAX_RANGE_BYTES)) {
    return void 0;
  }
  return {
    start: Number(start),
    end: Number(end),
    length: Number(length)
  };
}
__name(parseSingleRange, "parseSingleRange");
function canonicalIp(value) {
  if (value.length === 0 || encoder.encode(value).byteLength > MAX_IP_TEXT_BYTES || hasAsciiControl(value) || value.includes(",") || /\s/.test(value)) {
    return void 0;
  }
  try {
    if (value.includes(":")) {
      const hostname2 = new URL(`http://[${value}]/`).hostname;
      if (!hostname2.startsWith("[") || !hostname2.endsWith("]")) {
        return void 0;
      }
      return hostname2.slice(1, -1).toLowerCase();
    }
    if (!/^[0-9]+(?:\.[0-9]+){3}$/.test(value)) {
      return void 0;
    }
    const hostname = new URL(`http://${value}/`).hostname;
    return /^[0-9]+(?:\.[0-9]+){3}$/.test(hostname) ? hostname : void 0;
  } catch {
    return void 0;
  }
}
__name(canonicalIp, "canonicalIp");
function publicReadEnabled(env) {
  const configured = env.BENCHMARK_PUBLIC_READ;
  if (configured === true) {
    return true;
  }
  if (configured === false || configured === void 0) {
    return false;
  }
  throw new BenchmarkHttpError(500, "invalid_public_read_configuration");
}
__name(publicReadEnabled, "publicReadEnabled");
function configuredSourceIps(env) {
  const configured = Reflect.get(
    env,
    "BENCHMARK_SOURCE_IP_ALLOWLIST"
  );
  if (configured === void 0) {
    return void 0;
  }
  if (typeof configured !== "string" || configured.length === 0 || configured !== configured.trim() || encoder.encode(configured).byteLength > MAX_ALLOWLIST_BYTES) {
    throw new BenchmarkHttpError(500, "invalid_ip_allowlist_configuration");
  }
  const entries = configured.split(",");
  if (entries.length === 0 || entries.length > MAX_ALLOWLIST_ENTRIES) {
    throw new BenchmarkHttpError(500, "invalid_ip_allowlist_configuration");
  }
  const result = [];
  const seen = /* @__PURE__ */ new Set();
  for (const entry of entries) {
    const canonical = canonicalIp(entry.trim());
    if (canonical === void 0 || seen.has(canonical)) {
      throw new BenchmarkHttpError(500, "invalid_ip_allowlist_configuration");
    }
    seen.add(canonical);
    result.push(canonical);
  }
  return result;
}
__name(configuredSourceIps, "configuredSourceIps");
async function digestText(value) {
  return crypto.subtle.digest("SHA-256", encoder.encode(value));
}
__name(digestText, "digestText");
async function authorizeBenchmarkRead(request, env) {
  if (publicReadEnabled(env)) {
    return;
  }
  const allowed = configuredSourceIps(env);
  if (allowed === void 0) {
    throw new BenchmarkHttpError(503, "benchmark_access_not_configured");
  }
  const candidate = canonicalIp(request.headers.get("CF-Connecting-IP") ?? "");
  const [candidateDigest, ...allowedDigests] = await Promise.all([
    digestText(candidate ?? ""),
    ...allowed.map((value) => digestText(value))
  ]);
  let accepted = false;
  for (const allowedDigest of allowedDigests) {
    accepted = crypto.subtle.timingSafeEqual(candidateDigest, allowedDigest) || accepted;
  }
  if (!accepted || candidate === void 0) {
    throw new BenchmarkHttpError(403, "source_ip_not_allowed");
  }
}
__name(authorizeBenchmarkRead, "authorizeBenchmarkRead");
function addCommonHeaders(headers) {
  headers.set("Cache-Control", NO_STORE);
  headers.set("Vary", "CF-Connecting-IP");
  headers.set("X-Content-Type-Options", "nosniff");
}
__name(addCommonHeaders, "addCommonHeaders");
function objectHeaders(object, route, contentLength, range) {
  const headers = new Headers({
    "Accept-Ranges": "bytes",
    "Content-Length": contentLength.toString(),
    "Content-Type": route.contentType,
    ETag: object.httpEtag
  });
  if (range !== void 0) {
    headers.set(
      "Content-Range",
      `bytes ${range.start}-${range.end}/${object.size}`
    );
  }
  addCommonHeaders(headers);
  return headers;
}
__name(objectHeaders, "objectHeaders");
function errorResponse(error) {
  const headers = new Headers(error.headers);
  headers.set("Content-Type", "application/json; charset=utf-8");
  addCommonHeaders(headers);
  return new Response(JSON.stringify({ error: error.code }), {
    status: error.status,
    headers
  });
}
__name(errorResponse, "errorResponse");
async function requireObject(env, route) {
  const object = await env.BENCHMARK_BUCKET.head(route.key);
  if (object === null) {
    throw new BenchmarkHttpError(404, "benchmark_object_not_found");
  }
  return object;
}
__name(requireObject, "requireObject");
async function serveObject(request, env, route) {
  if (request.method === "HEAD") {
    if (request.headers.has("Range")) {
      throw new BenchmarkHttpError(400, "head_range_not_supported");
    }
    const head = await requireObject(env, route);
    return new Response(null, {
      status: 200,
      headers: objectHeaders(head, route, head.size)
    });
  }
  const rangeValue = request.headers.get("Range");
  if (rangeValue !== null) {
    const range = parseSingleRange(rangeValue);
    if (range === void 0) {
      const head = await requireObject(env, route);
      throw rangeError(head.size);
    }
    let object2;
    try {
      object2 = await env.BENCHMARK_BUCKET.get(route.key, {
        range: { offset: range.start, length: range.length }
      });
    } catch (error) {
      const head = await requireObject(env, route);
      if (range.start >= head.size || range.end >= head.size) {
        throw rangeError(head.size);
      }
      throw error;
    }
    if (object2 === null) {
      throw new BenchmarkHttpError(404, "benchmark_object_not_found");
    }
    const returnedRange = object2.range;
    if (range.start >= object2.size || range.end >= object2.size || returnedRange === void 0 || !("offset" in returnedRange) || returnedRange.offset !== range.start || returnedRange.length !== range.length) {
      await object2.body.cancel().catch(() => void 0);
      if (range.start >= object2.size || range.end >= object2.size) {
        throw rangeError(object2.size);
      }
      throw new BenchmarkHttpError(503, "benchmark_range_mismatch");
    }
    return new Response(object2.body, {
      status: 206,
      headers: objectHeaders(object2, route, range.length, range)
    });
  }
  if (route.fullGetLimit === null) {
    throw new BenchmarkHttpError(400, "full_file_get_not_allowed", {
      "Accept-Ranges": "bytes"
    });
  }
  const object = await env.BENCHMARK_BUCKET.get(route.key);
  if (object === null) {
    throw new BenchmarkHttpError(404, "benchmark_object_not_found");
  }
  if (object.size > route.fullGetLimit) {
    await object.body.cancel().catch(() => void 0);
    throw new BenchmarkHttpError(400, "full_file_get_not_allowed", {
      "Accept-Ranges": "bytes"
    });
  }
  return new Response(object.body, {
    status: 200,
    headers: objectHeaders(object, route, object.size)
  });
}
__name(serveObject, "serveObject");
async function handleBenchmarkRequest(request, env, url) {
  const startedAt = performance.now();
  await authorizeBenchmarkRead(request, env);
  if (url.search !== "") {
    throw new BenchmarkHttpError(400, "query_not_supported");
  }
  if (request.method !== "GET" && request.method !== "HEAD") {
    throw new BenchmarkHttpError(405, "method_not_allowed", {
      Allow: "GET, HEAD"
    });
  }
  const route = parseRoute(url.pathname, env);
  const response = await serveObject(request, env, route);
  console.log(
    JSON.stringify({
      event: "benchmark_gateway_success",
      format: route.format,
      epoch: route.epoch,
      file: route.name,
      release_prefix: route.prefix,
      method: request.method,
      status: response.status,
      returned_bytes: Number(response.headers.get("Content-Length") ?? "0"),
      range_request: request.headers.has("Range"),
      gateway_response_ready_ms: performance.now() - startedAt
    })
  );
  return response;
}
__name(handleBenchmarkRequest, "handleBenchmarkRequest");
async function maybeServeBenchmark(request, env) {
  const url = new URL(request.url);
  if (!isBenchmarkPath(url.pathname)) {
    return null;
  }
  try {
    return await handleBenchmarkRequest(request, env, url);
  } catch (error) {
    if (error instanceof BenchmarkHttpError) {
      if (error.status >= 500) {
        console.error(
          JSON.stringify({
            event: "benchmark_gateway_error",
            method: request.method,
            pathname: url.pathname,
            code: error.code
          })
        );
      }
      return errorResponse(error);
    }
    console.error(
      JSON.stringify({
        event: "benchmark_gateway_error",
        method: request.method,
        pathname: url.pathname,
        code: "internal_error",
        error: error instanceof Error ? error.name : "unknown"
      })
    );
    return errorResponse(new BenchmarkHttpError(500, "internal_error"));
  }
}
__name(maybeServeBenchmark, "maybeServeBenchmark");

// src/benchmark-entry.ts
function notFound() {
  return Response.json(
    { error: "not_found" },
    {
      status: 404,
      headers: {
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff"
      }
    }
  );
}
__name(notFound, "notFound");
var benchmark_entry_default = {
  async fetch(request, env) {
    const response = await maybeServeBenchmark(request, env);
    return response ?? notFound();
  }
};
export {
  benchmark_entry_default as default
};
//# sourceMappingURL=benchmark-entry.js.map

