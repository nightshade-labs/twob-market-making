use std::sync::Arc;

use anchor_client::{Program, solana_sdk::signature::Keypair};
use anchor_lang::prelude::Pubkey;

use crate::{
    AccountResolver,
    twob_anchor::{
        self,
        accounts::{Bookkeeping, LiquidityPosition, Market},
    },
};

pub struct MarketState {
    pub market: Market,
    pub bookkeeping: Bookkeeping,
    pub current_slot: u64,
}

pub async fn fetch_market_state(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
) -> anyhow::Result<MarketState> {
    let resolver = AccountResolver::new(twob_anchor::ID);
    let market_pda = resolver.market_pda(market_id);
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());

    let market = program.account::<Market>(market_pda.address()).await?;
    let bookkeeping = program
        .account::<Bookkeeping>(bookkeeping_pda.address())
        .await?;
    let current_slot = program.rpc().get_slot().await?;

    Ok(MarketState {
        market,
        bookkeeping,
        current_slot,
    })
}

pub async fn fetch_liquidity_position(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    authority: &Pubkey,
) -> anyhow::Result<LiquidityPosition> {
    let resolver = AccountResolver::new(twob_anchor::ID);
    let market_pda = resolver.market_pda(market_id);
    let liquidity_position_pda = resolver.liquidity_position_pda(&market_pda.address(), authority);

    Ok(program
        .account::<LiquidityPosition>(liquidity_position_pda.address())
        .await?)
}
