use blockzilla_query_sdk::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt, BlockHeader, BlockSink,
    BlockView, CanonicalBlock, CanonicalTransaction, CpiCoverage, ExecutionStatus,
    InstructionCoverage, OrderedBlockPublisher, Result, ScanReceipt, ScanRequest, SourceIdentity,
    SourceVerification, TransactionHeader,
};

struct PublicFixture {
    identity: SourceIdentity,
    block: CanonicalBlock,
}

impl ArchiveInstructionSource for PublicFixture {
    fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> Result<ScanReceipt> {
        let mut publisher = OrderedBlockPublisher::new(&self.identity, request, sink)?;
        publisher.publish(&self.block)?;
        publisher.finish()
    }
}

fn fixture() -> PublicFixture {
    PublicFixture {
        identity: SourceIdentity {
            format: ArchiveFormat::Car,
            label: "public-fixture".into(),
            cluster_id: Some("mainnet-beta".into()),
            epoch: 0,
            first_slot: 0,
            slots_per_epoch: 432_000,
            block_count: 1,
            verification: SourceVerification::OperatorTrusted,
            binding: Some("fixture-cid".into()),
        },
        block: CanonicalBlock {
            header: BlockHeader {
                epoch: 0,
                block_ordinal: 0,
                slot: 0,
            },
            transactions: vec![CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: 0,
                    status: ExecutionStatus::Succeeded,
                    failed_outer_instruction_index: None,
                    instruction_coverage: InstructionCoverage::Complete,
                    cpi_coverage: CpiCoverage::Complete,
                },
                primary_signature: Some([1; 64]),
                required_signers: vec![[2; 32]],
                instructions: Vec::new(),
            }],
        },
    }
}

#[test]
fn external_adapter_uses_only_the_crate_root() {
    let mut source = fixture();
    let mut seen = Vec::new();
    source
        .for_each_transaction(&ScanRequest::all(), |transaction| {
            seen.push((
                transaction.block.slot,
                transaction.header.tx_index,
                transaction.required_signers[0],
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, [(0, 0, [2; 32])]);
}

#[test]
fn runtime_selected_source_uses_the_short_api() {
    let mut source: Box<dyn ArchiveInstructionSource> = Box::new(fixture());
    let receipt = source
        .for_each_block(&ScanRequest::all(), |block: BlockView<'_>| {
            assert_eq!(block.header.block_ordinal, 0);
            Ok(())
        })
        .unwrap();
    assert_eq!(receipt.blocks, 1);
}
