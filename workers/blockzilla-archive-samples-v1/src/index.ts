/// <reference path="../worker-configuration.d.ts" />

const PUBLISHED_EPOCHS: ReadonlySet<string> = new Set([
  "0",
  "100",
  "200",
  "300",
  "400",
  "500",
  "600",
  "700",
  "800",
  "900",
  "1000",
]);

const COMPACT_V2_FILES: ReadonlySet<string> = new Set([
  "archive-v2-blocks.index",
  "archive-v2-blocks.zstd",
  "archive-v2-meta.wincode",
  "blockhash_registry.bin",
  "poh.wincode",
  "prev_blockhash_tail.bin",
  "registry.bin",
  "registry.mphf",
  "shredding.wincode",
  "signatures.bin",
  "vote_hash_registry.bin",
]);

const INDEXER_V3_FILES: ReadonlySet<string> = new Set([
  "archive-v2-meta.wincode",
  "archive-v2-standalone-account-postings-adaptive-v3.control",
  "archive-v2-standalone-account-postings-adaptive-v3.coverage",
  "archive-v2-standalone-account-postings-adaptive-v3.pages",
  "archive-v2-standalone-balances.wincode",
  "archive-v2-standalone-block-rewards.wincode",
  "archive-v2-standalone-blocks.index",
  "archive-v2-standalone-inner-instructions.wincode",
  "archive-v2-standalone-loaded-addresses.wincode",
  "archive-v2-standalone-logs.wincode",
  "archive-v2-standalone-messages.wincode",
  "archive-v2-standalone-outcomes.wincode",
  "archive-v2-standalone-raw-metadata-fallbacks.wincode",
  "archive-v2-standalone-token-balances.wincode",
  "archive-v2-standalone-transaction-directory.wincode",
  "archive-v2-standalone-transaction-rewards.wincode",
  "blockhash_registry.bin",
  "poh.wincode",
  "prev_blockhash_tail.bin",
  "registry.bin",
  "registry.mphf",
  "shredding.wincode",
  "signatures.bin",
  "vote_hash_registry.bin",
]);

type ArchiveFormat = "car" | "compact-v2" | "indexer-v3";

type FileRoute = {
  contentType: string;
  epoch: string;
  format: ArchiveFormat;
  key: string;
  name: string;
};

type ByteRange = {
  end: number;
  length: number;
  start: number;
};

class HttpError extends Error {
  readonly code: string;
  readonly headers: HeadersInit;
  readonly status: number;

  constructor(
    status: number,
    code: string,
    headers: HeadersInit = {},
  ) {
    super(code);
    this.name = "HttpError";
    this.code = code;
    this.headers = headers;
    this.status = status;
  }
}

function decodeSegment(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new HttpError(400, "invalid_path_encoding");
  }
}

function isSafeFileName(value: string): boolean {
  return (
    value.length > 0 &&
    value !== "." &&
    value !== ".." &&
    !value.includes("/") &&
    !value.includes("\\") &&
    !/[\u0000-\u001f\u007f]/.test(value)
  );
}

function isPublishedFile(format: ArchiveFormat, epoch: string, name: string): boolean {
  if (epoch === "0" && name === "prev_blockhash_tail.bin") {
    return false;
  }
  if (format === "compact-v2") {
    return COMPACT_V2_FILES.has(name);
  }
  if (format === "indexer-v3") {
    return INDEXER_V3_FILES.has(name);
  }
  return name === `epoch-${epoch}.car` || name === `epoch-${epoch}-slot-ranges.raw`;
}

function parseRoute(pathname: string): FileRoute {
  const segments = pathname.split("/");
  if (segments.length !== 4 || segments[0] !== "") {
    throw new HttpError(404, "not_found");
  }

  const format = decodeSegment(segments[1] ?? "");
  if (format !== "car" && format !== "compact-v2" && format !== "indexer-v3") {
    throw new HttpError(404, "not_found");
  }

  const epoch = decodeSegment(segments[2] ?? "");
  if (!/^(0|[1-9][0-9]*)$/.test(epoch)) {
    throw new HttpError(400, "invalid_epoch");
  }
  if (!PUBLISHED_EPOCHS.has(epoch)) {
    throw new HttpError(404, "epoch_not_published");
  }

  const name = decodeSegment(segments[3] ?? "");
  if (!isSafeFileName(name)) {
    throw new HttpError(400, "invalid_file_name");
  }
  if (!isPublishedFile(format, epoch, name)) {
    throw new HttpError(404, "file_not_published");
  }

  const key = `${format}/${epoch}/${name}`;
  return {
    contentType:
      format === "car" && name.endsWith(".car")
        ? "application/vnd.ipld.car"
        : "application/octet-stream",
    epoch,
    format,
    key,
    name,
  };
}

function rangeError(size: number): HttpError {
  return new HttpError(416, "range_not_satisfiable", {
    "Accept-Ranges": "bytes",
    "Content-Range": `bytes */${size}`,
  });
}

function parseDecimal(value: string): bigint | undefined {
  if (value.length === 0 || value.length > 20 || !/^[0-9]+$/.test(value)) {
    return undefined;
  }
  return BigInt(value);
}

function parseRange(value: string, size: number): ByteRange {
  const match = /^bytes=([0-9]*)-([0-9]*)$/.exec(value);
  if (match === null || size <= 0 || !Number.isSafeInteger(size)) {
    throw rangeError(size);
  }

  const first = match[1] ?? "";
  const last = match[2] ?? "";
  if (first === "" && last === "") {
    throw rangeError(size);
  }

  const sizeBig = BigInt(size);
  let start: bigint;
  let end: bigint;
  if (first === "") {
    const suffix = parseDecimal(last);
    if (suffix === undefined || suffix === 0n) {
      throw rangeError(size);
    }
    const length = suffix < sizeBig ? suffix : sizeBig;
    start = sizeBig - length;
    end = sizeBig - 1n;
  } else {
    const parsedStart = parseDecimal(first);
    const parsedEnd = last === "" ? sizeBig - 1n : parseDecimal(last);
    if (
      parsedStart === undefined ||
      parsedEnd === undefined ||
      parsedStart >= sizeBig ||
      parsedStart > parsedEnd
    ) {
      throw rangeError(size);
    }
    start = parsedStart;
    end = parsedEnd < sizeBig ? parsedEnd : sizeBig - 1n;
  }

  return {
    start: Number(start),
    end: Number(end),
    length: Number(end - start + 1n),
  };
}

function isStrongEtag(value: string): boolean {
  return /^"[^"\u0000-\u001f\u007f]*"$/.test(value);
}

function etagMatchesIfNoneMatch(value: string | null, etag: string): boolean {
  if (value === null) {
    return false;
  }
  const target = etag.startsWith("W/") ? etag.slice(2) : etag;
  return value.split(",").some((candidate) => {
    const trimmed = candidate.trim();
    if (trimmed === "*") {
      return true;
    }
    return (trimmed.startsWith("W/") ? trimmed.slice(2) : trimmed) === target;
  });
}

function commonHeaders(): Headers {
  return new Headers({
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Expose-Headers":
      "Accept-Ranges, Content-Length, Content-Range, ETag, Last-Modified",
    "Cache-Control": "public, max-age=60, must-revalidate",
    "X-Content-Type-Options": "nosniff",
  });
}

function objectHeaders(
  object: R2Object,
  route: FileRoute,
  contentLength: number,
  range?: ByteRange,
): Headers {
  if (!isStrongEtag(object.httpEtag)) {
    throw new HttpError(503, "invalid_object_etag");
  }
  const headers = commonHeaders();
  headers.set("Accept-Ranges", "bytes");
  headers.set("Content-Length", contentLength.toString());
  headers.set("Content-Type", route.contentType);
  headers.set("ETag", object.httpEtag);
  headers.set("Last-Modified", object.uploaded.toUTCString());
  if (range !== undefined) {
    headers.set("Content-Range", `bytes ${range.start}-${range.end}/${object.size}`);
  }
  return headers;
}

function notModified(object: R2Object, route: FileRoute): Response {
  const headers = objectHeaders(object, route, 0);
  headers.delete("Content-Length");
  return new Response(null, { status: 304, headers });
}

async function cancelBody(object: R2ObjectBody): Promise<void> {
  try {
    await object.body.cancel();
  } catch {
    // The response already fails closed. A cancel error must not hide that result.
  }
}

async function requireHead(env: Env, route: FileRoute): Promise<R2Object> {
  const object = await env.ARCHIVE_BUCKET.head(route.key);
  if (object === null) {
    throw new HttpError(404, "object_not_found");
  }
  return object;
}

async function serveHead(request: Request, env: Env, route: FileRoute): Promise<Response> {
  const object = await requireHead(env, route);
  if (etagMatchesIfNoneMatch(request.headers.get("If-None-Match"), object.httpEtag)) {
    return notModified(object, route);
  }
  return new Response(null, {
    status: 200,
    headers: objectHeaders(object, route, object.size),
  });
}

async function serveFull(request: Request, env: Env, route: FileRoute): Promise<Response> {
  const object = await env.ARCHIVE_BUCKET.get(route.key);
  if (object === null) {
    throw new HttpError(404, "object_not_found");
  }
  if (!("body" in object)) {
    throw new HttpError(503, "object_body_missing");
  }
  if (etagMatchesIfNoneMatch(request.headers.get("If-None-Match"), object.httpEtag)) {
    await cancelBody(object);
    return notModified(object, route);
  }
  return new Response(object.body, {
    status: 200,
    headers: objectHeaders(object, route, object.size),
  });
}

async function serveRange(
  request: Request,
  env: Env,
  route: FileRoute,
  rangeValue: string,
): Promise<Response> {
  const head = await requireHead(env, route);
  if (etagMatchesIfNoneMatch(request.headers.get("If-None-Match"), head.httpEtag)) {
    return notModified(head, route);
  }

  const ifRange = request.headers.get("If-Range");
  if (ifRange !== null && (!isStrongEtag(ifRange) || ifRange !== head.httpEtag)) {
    return serveFull(request, env, route);
  }

  const range = parseRange(rangeValue, head.size);
  const object = await env.ARCHIVE_BUCKET.get(route.key, {
    onlyIf: { etagMatches: head.etag },
    range: { offset: range.start, length: range.length },
  });
  if (object === null) {
    throw new HttpError(404, "object_not_found");
  }
  if (!("body" in object)) {
    throw new HttpError(503, "object_changed_during_read");
  }

  const returnedRange = object.range;
  if (
    object.etag !== head.etag ||
    object.size !== head.size ||
    returnedRange === undefined ||
    !("offset" in returnedRange) ||
    returnedRange.offset !== range.start ||
    returnedRange.length !== range.length
  ) {
    await cancelBody(object);
    throw new HttpError(503, "range_response_mismatch");
  }

  return new Response(object.body, {
    status: 206,
    headers: objectHeaders(object, route, range.length, range),
  });
}

async function handleRequest(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  if (url.search !== "") {
    throw new HttpError(400, "query_not_supported");
  }
  if (request.method !== "GET" && request.method !== "HEAD") {
    throw new HttpError(405, "method_not_allowed", { Allow: "GET, HEAD" });
  }

  const route = parseRoute(url.pathname);
  if (request.method === "HEAD") {
    return serveHead(request, env, route);
  }

  const range = request.headers.get("Range");
  return range === null
    ? serveFull(request, env, route)
    : serveRange(request, env, route, range);
}

function errorResponse(error: HttpError): Response {
  const headers = new Headers(error.headers);
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Cache-Control", "no-store");
  headers.set("Content-Type", "application/json; charset=utf-8");
  headers.set("X-Content-Type-Options", "nosniff");
  return Response.json(
    { error: error.code },
    { status: error.status, headers },
  );
}

const worker = {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      return await handleRequest(request, env);
    } catch (error) {
      if (error instanceof HttpError) {
        if (error.status >= 500) {
          console.error(
            JSON.stringify({
              event: "archive_sample_gateway_error",
              code: error.code,
              method: request.method,
              pathname: new URL(request.url).pathname,
            }),
          );
        }
        return errorResponse(error);
      }

      console.error(
        JSON.stringify({
          event: "archive_sample_gateway_error",
          code: "internal_error",
          error: error instanceof Error ? error.name : "unknown",
          method: request.method,
          pathname: new URL(request.url).pathname,
        }),
      );
      return errorResponse(new HttpError(500, "internal_error"));
    }
  },
} satisfies ExportedHandler<Env>;

export {
  COMPACT_V2_FILES,
  INDEXER_V3_FILES,
  PUBLISHED_EPOCHS,
  parseRange,
  parseRoute,
};
export default worker;
