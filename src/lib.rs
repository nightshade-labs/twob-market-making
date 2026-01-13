pub mod accounts;
pub mod instructions;

// Re-export commonly used types
pub use accounts::{AccountResolver, PdaResult};
pub use instructions::*;

/// The TwoB Anchor program ID
pub const TWOB_PROGRAM_ID: &str = "DkjFmy1YNDDDaXoy3ZvuCnpb294UDbpbT457gUyiFS5V";

/// Parse the program ID from the constant string
pub fn program_id() -> anchor_lang::prelude::Pubkey {
    TWOB_PROGRAM_ID.parse().expect("Invalid program ID")
}
