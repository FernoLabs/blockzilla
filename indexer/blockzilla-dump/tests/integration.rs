// One integration target shares the HTTP fixture and dependency link step.
#[path = "cases/database.rs"]
mod database;
#[path = "cases/scanner.rs"]
mod scanner;
#[path = "cases/support.rs"]
mod support;
#[path = "cases/token_event_database.rs"]
mod token_event_database;
#[path = "cases/verifier.rs"]
mod verifier;
