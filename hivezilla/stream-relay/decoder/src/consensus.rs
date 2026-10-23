//! Read the independent native consensus fact profile with the shared parser.
//! A bound captured footer is an observation, not a verified BLS certificate.
use anyhow::{Result, ensure};
use blockzilla_bank_stream_v2::{consensus as fact, digest};

/// The caller must supply the exact binding from its trusted execution/evidence
/// receipt. In particular, a footer must not select its own execution dependency.
pub fn decode(wire: &[u8], expected: &fact::FactBinding) -> Result<Vec<u8>> {
    let value = fact::decode_bound(wire, expected)?;
    let output = serde_json::to_vec(&serde_json::json!({
        "profile": fact::PROFILE,
        "binding": value.binding,
        "verification": fact::VERIFICATION,
        "carrier_consensus_block_id": null,
        "footer_bytes": value.footer_wire().len(),
        "footer_sha256": digest(value.footer_wire()),
        "footer": value.footer()?,
    }))?;
    ensure!(output.len() <= 1_048_576, "consensus_output_limit_exceeded");
    Ok(output)
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bz_decode_consensus(
    ptr: u32,
    len: u32,
    binding_ptr: u32,
    binding_len: u32,
) -> u64 {
    super::bank_output((|| {
        ensure!(
            len as usize <= fact::MAX_FACT_BYTES && binding_len as usize <= fact::MAX_BINDING_BYTES,
            "consensus_input_limit_exceeded"
        );
        let wire = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
        let expected: fact::FactBinding = serde_json::from_slice(unsafe {
            std::slice::from_raw_parts(binding_ptr as *const u8, binding_len as usize)
        })?;
        decode(wire, &expected)
    })())
}
