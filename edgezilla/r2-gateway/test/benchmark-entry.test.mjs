import assert from "node:assert/strict";
import test from "node:test";

import worker from "../src/benchmark-entry.js";

const SIZE = 110_392_384;
const ETAG = '"572604dc2579439694b5df574fa91aed"';
const LEGACY_RELEASE_MAP =
  "compact-v2:0=compact-v2/epoch-0,indexer-v3:0=indexer-v3/epoch-0";
const EPOCH_900_RELEASE_MAP =
  `${LEGACY_RELEASE_MAP},` +
  "compact-v2:900=compact-v2/releases/e900-current-typed-errors-v1," +
  "indexer-v3:900=indexer-v3/releases/e900-current-typed-errors-v1";

const COMPACT_FILES = [
  "archive-v2-blocks.index",
  "archive-v2-blocks.zstd",
  "archive-v2-meta.wincode",
  "blockhash_registry.bin",
  "poh.wincode",
  "prev_blockhash_tail.bin",
  "registry.bin",
  "registry.mphf",
  "registry_counts.bin",
  "shredding.wincode",
  "signatures.bin",
  "vote_hash_registry.bin",
];

const INDEXER_V3_FILES = [
  "archive-v2-retained-sidecars.candidate.json",
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
  "archive-v2-meta.wincode",
  "blockhash_registry.bin",
  "poh.wincode",
  "prev_blockhash_tail.bin",
  "registry.bin",
  "registry.mphf",
  "shredding.wincode",
  "signatures.bin",
  "vote_hash_registry.bin",
];

function r2Object({ body, key = "test/key", range, size = SIZE } = {}) {
  return {
    key,
    version: "test",
    size,
    etag: ETAG.slice(1, -1),
    httpEtag: ETAG,
    uploaded: new Date(0),
    storageClass: "Standard",
    checksums: {},
    body,
    range,
  };
}

function env(bucket, releaseMap = LEGACY_RELEASE_MAP) {
  return {
    BENCHMARK_PUBLIC_READ: true,
    BENCHMARK_RELEASE_MAP: releaseMap,
    BENCHMARK_BUCKET: bucket,
  };
}

function fileUrl(format, epoch, name) {
  return `https://example.test/${format}/v1/epochs/${epoch}/files/${name}`;
}

test("maps epoch 0 to both legacy prefixes", async () => {
  const cases = [
    ["compact-v2", "registry.bin", "compact-v2/epoch-0/registry.bin"],
    ["indexer-v3", "signatures.bin", "indexer-v3/epoch-0/signatures.bin"],
  ];

  for (const [format, name, expectedKey] of cases) {
    let requestedKey;
    const response = await worker.fetch(
      new Request(fileUrl(format, 0, name), { method: "HEAD" }),
      env({
        async head(key) {
          requestedKey = key;
          return r2Object({ key });
        },
      }),
    );

    assert.equal(response.status, 200);
    assert.equal(requestedKey, expectedKey);
  }
});

test("does not publish an unmapped epoch", async () => {
  let bucketCalls = 0;
  const bucket = {
    async head() {
      bucketCalls += 1;
      return null;
    },
    async get() {
      bucketCalls += 1;
      return null;
    },
  };
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry.bin"), {
      method: "HEAD",
    }),
    env(bucket),
  );

  assert.equal(response.status, 404);
  assert.deepEqual(await response.json(), {
    error: "benchmark_release_not_published",
  });
  assert.equal(bucketCalls, 0);
});

test("maps every fixed epoch 900 file to its immutable release prefix", async () => {
  for (const [format, names] of [
    ["compact-v2", COMPACT_FILES],
    ["indexer-v3", INDEXER_V3_FILES],
  ]) {
    const prefix = `${format}/releases/e900-current-typed-errors-v1`;
    for (const name of names) {
      let requestedKey;
      const response = await worker.fetch(
        new Request(fileUrl(format, 900, name), { method: "HEAD" }),
        env(
          {
            async head(key) {
              requestedKey = key;
              return r2Object({ key });
            },
          },
          EPOCH_900_RELEASE_MAP,
        ),
      );

      assert.equal(response.status, 200, `${format}/${name}`);
      assert.equal(requestedKey, `${prefix}/${name}`);
    }
  }
});

test("serves registry_counts.bin through a closed range", async () => {
  const key =
    "compact-v2/releases/e900-current-typed-errors-v1/registry_counts.bin";
  let requestedKey;
  let requestedOptions;
  const body = new TextEncoder().encode("count");
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 900, "registry_counts.bin"), {
      headers: { Range: "bytes=3-7" },
    }),
    env(
      {
        async get(candidate, options) {
          requestedKey = candidate;
          requestedOptions = options;
          return r2Object({
            body: new Blob([body]).stream(),
            key,
            range: { offset: 3, length: 5 },
          });
        },
      },
      EPOCH_900_RELEASE_MAP,
    ),
  );

  assert.equal(response.status, 206);
  assert.equal(requestedKey, key);
  assert.deepEqual(requestedOptions, { range: { offset: 3, length: 5 } });
  assert.equal(response.headers.get("content-range"), `bytes 3-7/${SIZE}`);
  assert.equal(response.headers.get("etag"), ETAG);
  assert.equal(response.headers.get("cache-control"), "no-store");
  assert.equal(await response.text(), "count");
});

for (const format of ["compact-v2", "indexer-v3"]) {
  test(`does not expose a ${format} manifest route`, async () => {
    let bucketCalls = 0;
    const response = await worker.fetch(
      new Request(`https://example.test/${format}/v1/epochs/0/manifest`),
      env({
        async get() {
          bucketCalls += 1;
          return null;
        },
        async head() {
          bucketCalls += 1;
          return null;
        },
      }),
    );

    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { error: "not_found" });
    assert.equal(bucketCalls, 0);
  });
}

for (const name of [
  "archive-v2-generation.json",
  "benchmark-manifest.json",
  "archive-v2-message-schema-current-v1.marker",
  "archive-v2-metadata-schema-current-typed-errors-v1.marker",
]) {
  test(`does not expose obsolete indexed object ${name}`, async () => {
    const response = await worker.fetch(
      new Request(fileUrl("compact-v2", 0, name), { method: "HEAD" }),
      env({
        async head() {
          throw new Error("an unlisted object must not reach R2");
        },
      }),
    );

    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { error: "file_not_published" });
  });
}

for (const [label, releaseMap] of [
  ["missing", undefined],
  ["empty", ""],
  ["outer whitespace", ` ${LEGACY_RELEASE_MAP}`],
  ["duplicate route", "compact-v2:0=compact-v2/a,compact-v2:0=compact-v2/b"],
  ["duplicate prefix", "compact-v2:0=compact-v2/a,compact-v2:1=compact-v2/a"],
  ["cross-format prefix", "compact-v2:0=indexer-v3/epoch-0"],
  ["noncanonical epoch", "compact-v2:00=compact-v2/epoch-0"],
  ["epoch overflow", "compact-v2:18446744073709551616=compact-v2/a"],
  ["dot segment", "compact-v2:0=compact-v2/../epoch-0"],
  ["empty segment", "compact-v2:0=compact-v2//epoch-0"],
]) {
  test(`rejects ${label} release-map configuration`, async () => {
    const configuredEnv = env({
      async head() {
        throw new Error("invalid configuration must not reach R2");
      },
    });
    configuredEnv.BENCHMARK_RELEASE_MAP = releaseMap;
    const response = await worker.fetch(
      new Request(fileUrl("compact-v2", 0, "registry.bin"), {
        method: "HEAD",
      }),
      configuredEnv,
    );

    assert.equal(response.status, 500);
    assert.deepEqual(await response.json(), {
      error: "invalid_release_map_configuration",
    });
  });
}

for (const [label, releaseMap] of [
  [
    "more than 64 entries",
    Array.from(
      { length: 65 },
      (_, epoch) => `compact-v2:${epoch}=compact-v2/release-${epoch}`,
    ).join(","),
  ],
  ["a prefix above 512 bytes", `compact-v2:0=compact-v2/${"a".repeat(502)}`],
]) {
  test(`rejects a release map with ${label}`, async () => {
    const response = await worker.fetch(
      new Request(fileUrl("compact-v2", 0, "registry.bin"), {
        method: "HEAD",
      }),
      env({}, releaseMap),
    );

    assert.equal(response.status, 500);
    assert.deepEqual(await response.json(), {
      error: "invalid_release_map_configuration",
    });
  });
}

test("returns object metadata through HEAD", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("indexer-v3", 0, "signatures.bin"), {
      method: "HEAD",
    }),
    env({
      async head(key) {
        return r2Object({ key });
      },
    }),
  );

  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-length"), String(SIZE));
  assert.equal(response.headers.get("accept-ranges"), "bytes");
  assert.equal(response.headers.get("etag"), ETAG);
});

test("rejects an open range and reports the object size", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("indexer-v3", 0, "signatures.bin"), {
      headers: { Range: "bytes=3-" },
    }),
    env({
      async head(key) {
        return r2Object({ key });
      },
    }),
  );

  assert.equal(response.status, 416);
  assert.equal(response.headers.get("content-range"), `bytes */${SIZE}`);
  assert.deepEqual(await response.json(), { error: "range_not_satisfiable" });
});

test("rejects a range larger than 64 MiB", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("indexer-v3", 0, "signatures.bin"), {
      headers: { Range: `bytes=0-${64 * 1024 * 1024}` },
    }),
    env({
      async head(key) {
        return r2Object({ key });
      },
    }),
  );

  assert.equal(response.status, 416);
});

test("requires a range for a large binary GET", async () => {
  let bucketCalls = 0;
  const response = await worker.fetch(
    new Request(fileUrl("indexer-v3", 0, "signatures.bin")),
    env({
      async get() {
        bucketCalls += 1;
        return null;
      },
    }),
  );

  assert.equal(response.status, 400);
  assert.deepEqual(await response.json(), { error: "full_file_get_not_allowed" });
  assert.equal(bucketCalls, 0);
});

test("returns 404 when a mapped R2 object is absent", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 0, "registry.bin"), {
      method: "HEAD",
    }),
    env({
      async head() {
        return null;
      },
    }),
  );

  assert.equal(response.status, 404);
  assert.deepEqual(await response.json(), { error: "benchmark_object_not_found" });
});

test("keeps CAR routing independent from the indexed release map", async () => {
  const key = "car/epoch-900/epoch-900.car";
  let requestedKey;
  const response = await worker.fetch(
    new Request("https://example.test/car/900/epoch-900.car", {
      method: "HEAD",
    }),
    {
      BENCHMARK_PUBLIC_READ: true,
      BENCHMARK_BUCKET: {
        async head(candidate) {
          requestedKey = candidate;
          return r2Object({ key });
        },
      },
    },
  );

  assert.equal(response.status, 200);
  assert.equal(requestedKey, key);
});

test("rejects unsupported methods before an R2 request", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 0, "registry.bin"), {
      method: "POST",
    }),
    env({}),
  );

  assert.equal(response.status, 405);
  assert.equal(response.headers.get("allow"), "GET, HEAD");
});

test("requires explicit public or source-IP access configuration", async () => {
  const response = await worker.fetch(
    new Request(fileUrl("compact-v2", 0, "registry.bin"), {
      method: "HEAD",
    }),
    {
      BENCHMARK_RELEASE_MAP: LEGACY_RELEASE_MAP,
      BENCHMARK_BUCKET: {},
    },
  );

  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), {
    error: "benchmark_access_not_configured",
  });
});
