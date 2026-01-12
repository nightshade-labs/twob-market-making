//! PDA (Program Derived Address) resolution utilities.
//!
//! This module provides functions to derive all PDAs used by the Twob Anchor program.
//! PDAs are deterministically derived addresses that the program can sign for.

use anchor_lang::solana_program::pubkey::Pubkey;

/// Seeds used for PDA derivation
pub mod seeds {
    pub const PROGRAM_CONFIG: &[u8] = b"program_config";
    pub const MARKET: &[u8] = b"market";
    pub const BOOKKEEPING: &[u8] = b"bookkeeping";
    pub const LIQUIDITY_POSITION: &[u8] = b"liquidity_position";
    pub const TRADE_POSITION: &[u8] = b"trade_position";
    pub const EXITS: &[u8] = b"exits";
    pub const PRICES: &[u8] = b"prices";
}

/// The Associated Token Program ID
pub const ASSOCIATED_TOKEN_PROGRAM_ID: Pubkey = anchor_spl::associated_token::ID;

/// Helper struct for resolving all program PDAs.
///
/// # Example
/// ```ignore
/// let resolver = AccountResolver::new(program_id);
/// let market_pda = resolver.market_pda(market_id);
/// let (market_address, bump) = market_pda.address_and_bump();
/// ```
#[derive(Debug, Clone)]
pub struct AccountResolver {
    program_id: Pubkey,
}

impl AccountResolver {
    /// Create a new account resolver for the given program ID.
    pub fn new(program_id: Pubkey) -> Self {
        Self { program_id }
    }

    /// Get the program ID this resolver is configured for.
    pub fn program_id(&self) -> &Pubkey {
        &self.program_id
    }

    /// Derive the program config PDA.
    ///
    /// Seeds: `["program_config"]`
    pub fn program_config_pda(&self) -> PdaResult {
        PdaResult::find(&[seeds::PROGRAM_CONFIG], &self.program_id)
    }

    /// Derive a market PDA.
    ///
    /// Seeds: `["market", market_id]`
    pub fn market_pda(&self, market_id: u64) -> PdaResult {
        PdaResult::find(&[seeds::MARKET, &market_id.to_le_bytes()], &self.program_id)
    }

    /// Derive a bookkeeping account PDA.
    ///
    /// Seeds: `["bookkeeping", market]`
    pub fn bookkeeping_pda(&self, market: &Pubkey) -> PdaResult {
        PdaResult::find(&[seeds::BOOKKEEPING, market.as_ref()], &self.program_id)
    }

    /// Derive a liquidity position PDA.
    ///
    /// Seeds: `["liquidity_position", market, authority]`
    pub fn liquidity_position_pda(&self, market: &Pubkey, authority: &Pubkey) -> PdaResult {
        PdaResult::find(
            &[
                seeds::LIQUIDITY_POSITION,
                market.as_ref(),
                authority.as_ref(),
            ],
            &self.program_id,
        )
    }

    /// Derive a trade position PDA.
    ///
    /// Seeds: `["trade_position", market, authority, position_id]`
    pub fn trade_position_pda(
        &self,
        market: &Pubkey,
        authority: &Pubkey,
        position_id: u64,
    ) -> PdaResult {
        PdaResult::find(
            &[
                seeds::TRADE_POSITION,
                market.as_ref(),
                authority.as_ref(),
                &position_id.to_le_bytes(),
            ],
            &self.program_id,
        )
    }

    /// Derive an exits account PDA.
    ///
    /// Seeds: `["exits", market, index]`
    pub fn exits_pda(&self, market: &Pubkey, index: u64) -> PdaResult {
        PdaResult::find(
            &[seeds::EXITS, market.as_ref(), &index.to_le_bytes()],
            &self.program_id,
        )
    }

    /// Derive a prices account PDA.
    ///
    /// Seeds: `["prices", market, index]`
    pub fn prices_pda(&self, market: &Pubkey, index: u64) -> PdaResult {
        PdaResult::find(
            &[seeds::PRICES, market.as_ref(), &index.to_le_bytes()],
            &self.program_id,
        )
    }

    /// Derive an associated token account address.
    ///
    /// This uses the standard Associated Token Program derivation.
    pub fn associated_token_account(&self, wallet: &Pubkey, mint: &Pubkey) -> Pubkey {
        anchor_spl::associated_token::get_associated_token_address(wallet, mint)
    }

    /// Derive a token vault PDA for a market.
    ///
    /// This is an Associated Token Account owned by the market PDA.
    pub fn market_vault(&self, market: &Pubkey, mint: &Pubkey) -> Pubkey {
        self.associated_token_account(market, mint)
    }
}

/// Result of a PDA derivation, containing the address and bump seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PdaResult {
    address: Pubkey,
    bump: u8,
}

impl PdaResult {
    /// Find a PDA given seeds and program ID.
    pub fn find(seeds: &[&[u8]], program_id: &Pubkey) -> Self {
        let (address, bump) = Pubkey::find_program_address(seeds, program_id);
        Self { address, bump }
    }

    /// Get the PDA address.
    pub fn address(&self) -> Pubkey {
        self.address
    }

    /// Get the bump seed.
    pub fn bump(&self) -> u8 {
        self.bump
    }

    /// Get both address and bump as a tuple.
    pub fn address_and_bump(&self) -> (Pubkey, u8) {
        (self.address, self.bump)
    }
}

impl From<PdaResult> for Pubkey {
    fn from(pda: PdaResult) -> Self {
        pda.address
    }
}

impl AsRef<Pubkey> for PdaResult {
    fn as_ref(&self) -> &Pubkey {
        &self.address
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_program_config_pda_is_deterministic() {
        let program_id = Pubkey::new_unique();
        let resolver = AccountResolver::new(program_id);

        let pda1 = resolver.program_config_pda();
        let pda2 = resolver.program_config_pda();

        assert_eq!(pda1.address(), pda2.address());
        assert_eq!(pda1.bump(), pda2.bump());
    }

    #[test]
    fn test_market_pda_differs_by_id() {
        let program_id = Pubkey::new_unique();
        let resolver = AccountResolver::new(program_id);

        let market1 = resolver.market_pda(1);
        let market2 = resolver.market_pda(2);

        assert_ne!(market1.address(), market2.address());
    }

    #[test]
    fn test_liquidity_position_pda_differs_by_authority() {
        let program_id = Pubkey::new_unique();
        let resolver = AccountResolver::new(program_id);
        let market = Pubkey::new_unique();
        let authority1 = Pubkey::new_unique();
        let authority2 = Pubkey::new_unique();

        let pos1 = resolver.liquidity_position_pda(&market, &authority1);
        let pos2 = resolver.liquidity_position_pda(&market, &authority2);

        assert_ne!(pos1.address(), pos2.address());
    }

    #[test]
    fn test_trade_position_pda_differs_by_position_id() {
        let program_id = Pubkey::new_unique();
        let resolver = AccountResolver::new(program_id);
        let market = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let pos1 = resolver.trade_position_pda(&market, &authority, 1);
        let pos2 = resolver.trade_position_pda(&market, &authority, 2);

        assert_ne!(pos1.address(), pos2.address());
    }

    #[test]
    fn test_pda_result_conversions() {
        let program_id = Pubkey::new_unique();
        let resolver = AccountResolver::new(program_id);
        let pda = resolver.program_config_pda();

        let pubkey: Pubkey = pda.into();
        assert_eq!(pubkey, pda.address());

        let pubkey_ref: &Pubkey = pda.as_ref();
        assert_eq!(pubkey_ref, &pda.address());
    }
}
