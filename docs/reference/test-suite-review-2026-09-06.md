# Test suite review — 6 September 2026

The review reduces duplicate test setup and integration linking. It keeps tests
for archive bytes, corrupt input, source identity, signatures, crash recovery,
ordered parallel reads, replay memory access, and public output.

## What changed

`blockzilla-dump` now has one integration target, `tests/integration.rs`, with
private case modules. Previously, Cargo compiled and linked four independent
integration targets. The 47 retained cases still use private temporary folders
and dynamically assigned HTTP ports. The scanner and verifier share their HTTP
range/ETag fixture.

Four tests were removed from the active workspace:

- Two tests checked source text instead of runtime behavior.
- An 8,192-account test checked the resulting count and searched source text for
  clone calls. Existing seed/commit tests cover the behavior. It did not measure
  allocations and took only 0.33 seconds in the isolated check.
- A trait smoke test duplicated the success and failure coverage in the tracker
  recovery test.

The real corruption test remains; its source-text assertion was removed. The
parked `archive-token-events` SDK boundary test was also removed. It checked
source/manifest spelling, including an old indexer name, and was outside the
active workspace. Its removal does not reduce the active test count.

This cleanup removes three active integration link targets and 308 net lines of
test code, including the parked test. New dependency regression tests are a
separate change: they cover actual API and format boundaries found during the
[dependency review](dependency-review-2026-09-06.md).

## Why the rest remains

The earlier full run passed 3,563 tests in 131 test targets, with one ignored
test. Summed test execution was about 54.7 seconds; compilation and linking took
2 minutes 2 seconds for that particular cached build. These are separate
measurements. A fresh build after many dependency upgrades has a different cache
state and cannot measure the benefit of this cleanup.

Similar-looking tests in the legacy and current readers exercise different
compatibility code. The larger scheduler, publication, source-binding, and
recovery fixtures test failure behavior that a small happy-path fixture cannot
replace. Tests that launch fake operational commands check arguments and
failure handling; they are not source-text checks.

The CLI test profile retains its existing optimization setting because the
large generated command parser needs it to avoid excessive debug stack frames.
Deleting a few assertions does not remove that compilation cost.

## Validation

The final test totals and build results are recorded with the completed
dependency review. Compare the cleanup's four removed active tests separately
from regression tests added for newly identified dependency behavior.
