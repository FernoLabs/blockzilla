import assert from "node:assert/strict";
import test from "node:test";

import worker, {
  COMPACT_V2_FILES,
  INDEXER_V3_FILES,
  PUBLISHED_EPOCHS,
  parseRange,
  parseRoute,
} from "../src/index.ts";

const SIZE = 100;
const ETAG = "sample-etag";
const HTTP_ETAG = `"${ETAG}"`;

function metadata({ key = "test/key", size = SIZE } = {}) {
  return {
    checksums: {},
    customMetadata: {},
    etag: ETAG,
    httpEtag: HTTP_ETAG,
    httpMetadata: {},
    key,
    size,
    storageClass: "Standard",
    uploaded: new Date("2026-08-31T00:00:00Z"),
    version: "test-version",
    writeHttpMetadata() {},
  };
}

function bodyObject({ body = "sample", key, range, size } = {}) {
  return {
    ...metadata({ key, size }),
    body: new Blob([body]).stream(),
    bodyUsed: false,
    range,
  };
}

function env(bucket) {
  return { ARCHIVE_BUCKET: bucket };
}

function fileUrl(format, epoch, name) {
  return `https://samples.example/${format}/${epoch}/${name}`;
}

test("the epoch allowlist contains only the eleven sample epochs", () => {
  assert.deepEqual([...PUBLISHED_EPOCHS], [
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
});

test("every fixed V2 and V3 name maps directly to its public path", () => {
  assert.equal(COMPACT_V2_FILES.size, 11);
  assert.equal(INDEXER_V3_FILES.size, 24);

  for (const [format, files] of [
    ["compact-v2", COMPACT_V2_FILES],
    ["indexer-v3", INDEXER_V3_FILES],
  ]) {
    for (const name of files) {
      const route = parseRoute(new URL(fileUrl(format, 900, name)).pathname);
      assert.equal(route.key, `${format}/900/${name}`);
      assert.equal(`archive/${route.key}`, `archive${new URL(fileUrl(format, 900, name)).pathname}`);
    }
  }
});

test("CAR accepts only its two epoch-derived file names", () => {
  for (const name of ["epoch-900.car", "epoch-900-slot-ranges.raw"]) {
    assert.equal(
      parseRoute(new URL(fileUrl("car", 900, name)).pathname).key,
      `car/900/${name}`,
    );
  }

  assert.throws(
    () => parseRoute(new URL(fileUrl("car", 900, "epoch-800.car")).pathname),
    /file_not_published/,
  );
});

test("epoch 0 does not publish a previous-blockhash tail", async () => {
  for (const format of ["compact-v2", "indexer-v3"]) {
    let calls = 0;
    const response = await worker.fetch(
      new Request(fileUrl(format, 0, "prev_blockhash_tail.bin"), {
        method: "HEAD",
      }),
      env({
        async head() {
          calls += 1;
          return null;
        },
      }),
    );

    assert.equal(response.status, 404, format);
    assert.deepEqual(await response.json(), { error: "file_not_published" });
    assert.equal(calls, 0, format);
    assert.equal(
      parseRoute(
        new URL(fileUrl(format, 100, "prev_blockhash_tail.bin")).pathname,
      ).name,
      "prev_blockhash_tail.bin",
    );
  }
});

for (const name of [
  "registry_counts.bin",
  "archive-v2-retained-sidecars.candidate.json",
  "manifest.json",
  "hashes.json",
  "epoch.seal",
  "benchmark-report.json",
  "schema.marker",
  "COMPLETE",
]) {
  test(`does not expose ${name}`, async () => {
    let calls = 0;
    const format = name === "archive-v2-retained-sidecars.candidate.json"
      ? "indexer-v3"
      : "compact-v2";
    const response = await worker.fetch(
      new Request(fileUrl(format, 900, name), { method: "HEAD" }),
      env({
        async head() {
          calls += 1;
          return null;
        },
      }),
    );

    assert.equal(response.status, 404);
    assert.equal(calls, 0);
  });
}

test("an epoch outside the allowlist cannot reach R2", async () => {
  let calls = 0;
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 901, "registry.bin")),
    env({
      async get() {
        calls += 1;
        return null;
      },
    }),
  );

  assert.equal(response.status, 404);
  assert.deepEqual(await response.json(), { error: "epoch_not_published" });
  assert.equal(calls, 0);
});

test("the old nested route does not reach R2", async () => {
  let calls = 0;
  const response = await worker.fetch(
    new Request(
      "https://samples.example/compact-v2/v1/epochs/900/files/registry.bin",
    ),
    env({
      async get() {
        calls += 1;
        return null;
      },
    }),
  );

  assert.equal(response.status, 404);
  assert.deepEqual(await response.json(), { error: "not_found" });
  assert.equal(calls, 0);
});

test("HEAD returns full object metadata and no body", async () => {
  const key = "compact-v2/900/registry.bin";
  let requestedKey;
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry.bin"), { method: "HEAD" }),
    env({
      async head(candidate) {
        requestedKey = candidate;
        return metadata({ key });
      },
    }),
  );

  assert.equal(requestedKey, key);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("accept-ranges"), "bytes");
  assert.equal(response.headers.get("content-length"), String(SIZE));
  assert.equal(response.headers.get("content-type"), "application/octet-stream");
  assert.equal(response.headers.get("etag"), HTTP_ETAG);
  assert.equal(response.headers.get("last-modified"), "Mon, 31 Aug 2026 00:00:00 GMT");
  assert.equal(await response.text(), "");
});

test("a full GET streams the R2 body", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("car", 900, "epoch-900.car")),
    env({
      async get(key) {
        return bodyObject({ body: "car", key, size: 3 });
      },
    }),
  );

  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-length"), "3");
  assert.equal(response.headers.get("content-type"), "application/vnd.ipld.car");
  assert.equal(await response.text(), "car");
});

for (const [label, rangeHeader, expected] of [
  ["closed", "bytes=10-19", { start: 10, end: 19, length: 10 }],
  ["open", "bytes=90-", { start: 90, end: 99, length: 10 }],
  ["suffix", "bytes=-8", { start: 92, end: 99, length: 8 }],
  ["clamped", "bytes=95-999", { start: 95, end: 99, length: 5 }],
]) {
  test(`serves a ${label} single range`, async () => {
    let getOptions;
    const response = await worker.fetch(
      new Request(fileUrl("indexer-v3", 900, "signatures.bin"), {
        headers: { Range: rangeHeader },
      }),
      env({
        async head(key) {
          return metadata({ key });
        },
        async get(key, options) {
          getOptions = options;
          return bodyObject({
            body: "x".repeat(expected.length),
            key,
            range: { offset: expected.start, length: expected.length },
          });
        },
      }),
    );

    assert.equal(response.status, 206);
    assert.deepEqual(getOptions, {
      onlyIf: { etagMatches: ETAG },
      range: { offset: expected.start, length: expected.length },
    });
    assert.equal(
      response.headers.get("content-range"),
      `bytes ${expected.start}-${expected.end}/${SIZE}`,
    );
    assert.equal(response.headers.get("content-length"), String(expected.length));
    assert.equal((await response.arrayBuffer()).byteLength, expected.length);
  });
}

for (const value of [
  "bytes=",
  "bytes=10-9",
  "bytes=100-100",
  "bytes=-0",
  "bytes=0-1,4-5",
  "items=0-1",
]) {
  test(`rejects invalid or unsatisfied range ${value}`, () => {
    assert.throws(() => parseRange(value, SIZE), /range_not_satisfiable/);
  });
}

test("a 416 response includes the full object size", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry.bin"), {
      headers: { Range: "bytes=100-200" },
    }),
    env({
      async head(key) {
        return metadata({ key });
      },
    }),
  );

  assert.equal(response.status, 416);
  assert.equal(response.headers.get("content-range"), `bytes */${SIZE}`);
  assert.deepEqual(await response.json(), { error: "range_not_satisfiable" });
});

test("If-None-Match returns 304 without a body read", async () => {
  let getCalls = 0;
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry.bin"), {
      method: "HEAD",
      headers: { "If-None-Match": `W/${HTTP_ETAG}` },
    }),
    env({
      async head(key) {
        return metadata({ key });
      },
      async get() {
        getCalls += 1;
        return null;
      },
    }),
  );

  assert.equal(response.status, 304);
  assert.equal(getCalls, 0);
});

test("a stale If-Range value returns the full object", async () => {
  let getOptions;
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry.bin"), {
      headers: { Range: "bytes=0-2", "If-Range": '"old"' },
    }),
    env({
      async head(key) {
        return metadata({ key });
      },
      async get(key, options) {
        getOptions = options;
        return bodyObject({ body: "full", key, size: 4 });
      },
    }),
  );

  assert.equal(response.status, 200);
  assert.equal(getOptions, undefined);
  assert.equal(await response.text(), "full");
});

test("a changed object fails closed during a range read", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry.bin"), {
      headers: { Range: "bytes=0-2" },
    }),
    env({
      async head(key) {
        return metadata({ key });
      },
      async get(key) {
        return metadata({ key });
      },
    }),
  );

  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { error: "object_changed_during_read" });
});

test("missing objects, query strings, and write methods fail closed", async () => {
  const bucket = {
    async get() {
      return null;
    },
    async head() {
      return null;
    },
  };

  const missing = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry.bin")),
    env(bucket),
  );
  assert.equal(missing.status, 404);

  const query = await worker.fetch(
    new Request(`${fileUrl("compact-v2", 900, "registry.bin")}?list=1`),
    env(bucket),
  );
  assert.equal(query.status, 400);

  const write = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry.bin"), {
      method: "PUT",
      body: "no",
    }),
    env(bucket),
  );
  assert.equal(write.status, 405);
  assert.equal(write.headers.get("allow"), "GET, HEAD");
});
