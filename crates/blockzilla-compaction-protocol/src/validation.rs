//! Cross-object publication validation.
//!
//! Individual constructors validate object-local shape. These functions are
//! the mandatory acceptance boundary that binds a COMPLETE candidate and the
//! reader-visible completion back to the same job and finality bytes.

use crate::{
    CandidateManifestV1, CompactionJobV1, CompactionOutcomeV1, CompactionResultV1,
    CompletionManifestV1, FinalityManifestV1, ProtocolError, Result,
};

/// Validate a COMPLETE worker result and candidate against the currently
/// active fenced job and exact finality manifest.
///
/// This is the mandatory worker-output acceptance boundary. Validating only
/// the immutable job spec is insufficient because a superseded attempt has the
/// same job ID and spec hash.
pub fn validate_complete_candidate_v1(
    active_job: &CompactionJobV1,
    finality: &FinalityManifestV1,
    result: &CompactionResultV1,
    candidate: &CandidateManifestV1,
) -> Result<()> {
    let job = active_job.spec();
    if result.job_id() != job.job_id()
        || result.job_spec_hash() != job.job_spec_hash()
        || result.fence() != active_job.fence()
    {
        return Err(ProtocolError::InvalidField {
            field: "CompactionResultV1.active_job_binding",
            reason: "job ID/hash and fence must exactly equal the active fenced job",
        });
    }
    if result.outcome() != CompactionOutcomeV1::Complete {
        return Err(ProtocolError::InvalidField {
            field: "CompactionResultV1.outcome",
            reason: "only COMPLETE may proceed to candidate acceptance",
        });
    }
    let candidate_ref = result
        .candidate_manifest()
        .ok_or(ProtocolError::InvalidField {
            field: "CompactionResultV1.candidate_manifest",
            reason: "COMPLETE must reference exactly one candidate manifest",
        })?;
    candidate_ref.verify_bytes(&candidate.encode())?;

    finality.validate_against_job(job)?;
    let (produced_count, skipped_count) = finality.complete_coverage_counts()?;

    if candidate.job_id() != job.job_id()
        || candidate.job_spec_hash() != job.job_spec_hash()
        || candidate.fence() != active_job.fence()
        || candidate.epoch() != job.epoch()
        || candidate.slots() != job.slots()
    {
        return Err(ProtocolError::InvalidField {
            field: "CandidateManifestV1.active_job_binding",
            reason: "job ID/hash, fence, epoch, and slots must exactly equal the active job",
        });
    }
    if candidate.finality_manifest() != job.finality_manifest() {
        return Err(ProtocolError::InvalidField {
            field: "CandidateManifestV1.finality_manifest",
            reason: "must exactly equal the job finality ObjectRef",
        });
    }
    if candidate.produced_count() != produced_count || candidate.skipped_count() != skipped_count {
        return Err(ProtocolError::InvalidField {
            field: "CandidateManifestV1.coverage_counts",
            reason: "must exactly match produced and skipped finality dispositions",
        });
    }
    Ok(())
}

/// Validate the exact Blockzilla-created completion before catalog publication.
///
/// The published finality locator may use a different reader-visible key, but
/// must resolve to byte-identical finality bytes.
pub fn validate_completion_manifest_v1(
    active_job: &CompactionJobV1,
    finality: &FinalityManifestV1,
    result: &CompactionResultV1,
    candidate: &CandidateManifestV1,
    completion: &CompletionManifestV1,
) -> Result<()> {
    validate_complete_candidate_v1(active_job, finality, result, candidate)?;
    let job = active_job.spec();
    let candidate_ref = result
        .candidate_manifest()
        .expect("validated COMPLETE result carries a candidate manifest");

    if completion.catalog_generation() != job.expected_catalog_generation()
        || completion.catalog_predecessor() != job.expected_catalog_predecessor()
    {
        return Err(ProtocolError::InvalidField {
            field: "CompletionManifestV1.catalog_binding",
            reason: "generation and predecessor must exactly equal the immutable job",
        });
    }
    if completion.job_id() != job.job_id()
        || completion.job_spec_hash() != job.job_spec_hash()
        || completion.epoch() != job.epoch()
        || completion.slots() != job.slots()
    {
        return Err(ProtocolError::InvalidField {
            field: "CompletionManifestV1.job_binding",
            reason: "job ID/hash, epoch, and slots must exactly equal the immutable job",
        });
    }
    completion
        .job_spec()
        .verify_bytes(&job.job_spec_object_bytes())?;
    if completion.candidate_manifest() != candidate_ref {
        return Err(ProtocolError::InvalidField {
            field: "CompletionManifestV1.candidate_manifest",
            reason: "must exactly equal the accepted candidate ObjectRef",
        });
    }
    completion
        .published_finality_manifest()
        .verify_bytes(&finality.encode())?;
    if completion.produced_count() != candidate.produced_count()
        || completion.skipped_count() != candidate.skipped_count()
    {
        return Err(ProtocolError::InvalidField {
            field: "CompletionManifestV1.coverage_counts",
            reason: "must exactly equal the validated candidate/finality counts",
        });
    }
    if completion.objects() != candidate.objects() {
        return Err(ProtocolError::InvalidField {
            field: "CompletionManifestV1.objects",
            reason: "must be byte-for-byte the candidate's ordered object vector",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        CompactionJobSpecV1, CompactionJobV1, CompactionResultV1, FinalizedBlockIdentityV1,
        FinalizedParentAnchorV1, FinalizedSlotV1, HashedDescriptorV1, InputObjectV1, LeaseFenceV1,
        NamedObjectRefV1, OLD_FAITHFUL_CAR_LOGICAL_NAME_V1, ObjectRefV1, SlotRangeV1,
    };
    use sha2::{Digest, Sha256};

    use super::*;

    fn descriptor(bytes: &[u8]) -> HashedDescriptorV1 {
        HashedDescriptorV1::new(bytes.to_vec()).unwrap()
    }

    fn object(key: &[u8], bytes: &[u8]) -> ObjectRefV1 {
        ObjectRefV1::new(
            key.to_vec(),
            None,
            bytes.len() as u64,
            Sha256::digest(bytes).into(),
        )
        .unwrap()
    }

    struct Fixture {
        job: CompactionJobSpecV1,
        active_job: CompactionJobV1,
        finality: FinalityManifestV1,
        candidate: CandidateManifestV1,
        result: CompactionResultV1,
        completion: CompletionManifestV1,
    }

    fn fixture() -> Fixture {
        let input_bytes = b"car";
        let input = InputObjectV1::new(
            OLD_FAITHFUL_CAR_LOGICAL_NAME_V1.to_vec(),
            descriptor(b"car-v1"),
            object(b"input.car", input_bytes),
        )
        .unwrap();
        let slots = SlotRangeV1::new(10, 12).unwrap();
        let finality = FinalityManifestV1::new(
            [1; 32],
            7,
            slots,
            slots,
            slots,
            descriptor(b"authority-v1"),
            Vec::new(),
            vec![input.clone()],
            Some(FinalizedParentAnchorV1::new(
                9,
                FinalizedBlockIdentityV1::new([2; 32], None),
            )),
            vec![
                FinalizedSlotV1::produced(10, FinalizedBlockIdentityV1::new([3; 32], None)),
                FinalizedSlotV1::skipped(11),
            ],
        )
        .unwrap();
        let finality_bytes = finality.encode();
        let finality_ref = object(b"private/finality", &finality_bytes);
        let job = CompactionJobSpecV1::new(
            [4; 16],
            [1; 32],
            7,
            slots,
            Vec::new(),
            vec![input],
            None,
            finality_ref.clone(),
            descriptor(b"selection-v1"),
            descriptor(b"normalization-v1"),
            descriptor(b"archive-v2"),
            descriptor(b"epoch-schedule-v1"),
            None,
            0,
            b"archive/".to_vec(),
        )
        .unwrap();
        let objects = vec![
            NamedObjectRefV1::new(b"blocks".to_vec(), object(b"candidate/blocks", b"blocks"))
                .unwrap(),
        ];
        let candidate = CandidateManifestV1::new(
            job.job_id(),
            job.job_spec_hash(),
            LeaseFenceV1::new(5),
            job.epoch(),
            job.slots(),
            finality_ref,
            1,
            1,
            objects.clone(),
        )
        .unwrap();
        let candidate_ref = object(b"candidate/manifest", &candidate.encode());
        let active_job = CompactionJobV1::new(job.clone(), candidate.fence());
        let result = CompactionResultV1::complete(
            job.job_id(),
            job.job_spec_hash(),
            active_job.fence(),
            candidate_ref.clone(),
        );
        let job_spec_ref = object(b"catalog/job-spec", &job.job_spec_object_bytes());
        let completion = CompletionManifestV1::new(
            0,
            None,
            job.job_id(),
            job.job_spec_hash(),
            job.epoch(),
            job.slots(),
            job_spec_ref,
            candidate_ref.clone(),
            object(b"catalog/finality", &finality_bytes),
            1,
            1,
            objects,
        )
        .unwrap();
        Fixture {
            job,
            active_job,
            finality,
            candidate,
            result,
            completion,
        }
    }

    #[test]
    fn exact_candidate_and_completion_binding_passes() {
        let fixture = fixture();
        validate_complete_candidate_v1(
            &fixture.active_job,
            &fixture.finality,
            &fixture.result,
            &fixture.candidate,
        )
        .unwrap();
        validate_completion_manifest_v1(
            &fixture.active_job,
            &fixture.finality,
            &fixture.result,
            &fixture.candidate,
            &fixture.completion,
        )
        .unwrap();
    }

    #[test]
    fn unresolved_finality_and_changed_counts_fail_complete_acceptance() {
        let fixture = fixture();
        let unresolved = FinalityManifestV1::new(
            [1; 32],
            7,
            fixture.job.slots(),
            fixture.job.slots(),
            fixture.job.slots(),
            descriptor(b"authority-v1"),
            Vec::new(),
            Vec::new(),
            None,
            vec![
                FinalizedSlotV1::unresolved(10),
                FinalizedSlotV1::skipped(11),
            ],
        )
        .unwrap();
        assert!(matches!(
            unresolved.complete_coverage_counts(),
            Err(ProtocolError::InvalidField { .. })
        ));

        let changed_counts = CandidateManifestV1::new(
            fixture.candidate.job_id(),
            fixture.candidate.job_spec_hash(),
            fixture.candidate.fence(),
            fixture.candidate.epoch(),
            fixture.candidate.slots(),
            fixture.candidate.finality_manifest().clone(),
            0,
            2,
            fixture.candidate.objects().to_vec(),
        )
        .unwrap();
        let changed_counts_ref = object(b"candidate/changed-counts", &changed_counts.encode());
        let changed_counts_result = CompactionResultV1::complete(
            fixture.job.job_id(),
            fixture.job.job_spec_hash(),
            fixture.active_job.fence(),
            changed_counts_ref,
        );
        assert!(matches!(
            validate_complete_candidate_v1(
                &fixture.active_job,
                &fixture.finality,
                &changed_counts_result,
                &changed_counts,
            ),
            Err(ProtocolError::InvalidField {
                field: "CandidateManifestV1.coverage_counts",
                ..
            })
        ));
    }

    #[test]
    fn stale_candidate_or_result_fence_cannot_pass_acceptance() {
        let fixture = fixture();
        let stale_fence = LeaseFenceV1::new(fixture.active_job.fence().get() - 1);
        let stale_candidate = CandidateManifestV1::new(
            fixture.candidate.job_id(),
            fixture.candidate.job_spec_hash(),
            stale_fence,
            fixture.candidate.epoch(),
            fixture.candidate.slots(),
            fixture.candidate.finality_manifest().clone(),
            fixture.candidate.produced_count(),
            fixture.candidate.skipped_count(),
            fixture.candidate.objects().to_vec(),
        )
        .unwrap();
        let stale_candidate_ref = object(b"candidate/stale", &stale_candidate.encode());
        let current_result_for_stale_candidate = CompactionResultV1::complete(
            fixture.job.job_id(),
            fixture.job.job_spec_hash(),
            fixture.active_job.fence(),
            stale_candidate_ref.clone(),
        );
        let stale_result = CompactionResultV1::complete(
            fixture.job.job_id(),
            fixture.job.job_spec_hash(),
            stale_fence,
            stale_candidate_ref,
        );

        assert!(matches!(
            validate_complete_candidate_v1(
                &fixture.active_job,
                &fixture.finality,
                &current_result_for_stale_candidate,
                &stale_candidate,
            ),
            Err(ProtocolError::InvalidField {
                field: "CandidateManifestV1.active_job_binding",
                ..
            })
        ));
        assert!(matches!(
            validate_complete_candidate_v1(
                &fixture.active_job,
                &fixture.finality,
                &stale_result,
                &stale_candidate,
            ),
            Err(ProtocolError::InvalidField {
                field: "CompactionResultV1.active_job_binding",
                ..
            })
        ));
    }
}
