use mimalloc::MiMalloc;

// Keep the scalable allocator local to this allocation-heavy command.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
