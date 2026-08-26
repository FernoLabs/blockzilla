//! Read-only verifier for the non-publishable account-projection candidate.

#[path = "archive-v2-account-projection.rs"]
#[allow(dead_code)]
mod account_projection;

fn main() {
    if let Err(error) = account_projection::verify_main() {
        let operational = error.chain().any(|cause| {
            cause.downcast_ref::<std::io::Error>().is_some()
                || cause
                    .downcast_ref::<blockzilla_read_sdk::SourceError>()
                    .is_some()
        });
        let receipt = serde_json::json!({
            "status": "verification-failed-read-only",
            "verification_passed": false,
            "issues_found": 1,
            "publishable": false,
            "candidate_status": "unverified-nonpublishable",
            "failure_class": if operational { "operational" } else { "data-mismatch" },
            "error": format!("{error:#}"),
            "ed25519_signature_verification": "off",
            "output_content_hashing": "none",
            "seal_written": false,
            "mutation": "none",
        });
        println!("{receipt}");
        std::process::exit(if operational { 1 } else { 2 });
    }
}
