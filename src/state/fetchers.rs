use std::sync::Arc;

use anchor_client::{
    Program,
    solana_sdk::{account::Account, commitment_config::CommitmentConfig, signature::Keypair},
};
use anchor_lang::{AccountDeserialize, prelude::Pubkey};
use anyhow::{Context, ensure};

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

pub async fn fetch_market_position_state(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    authority: &Pubkey,
) -> anyhow::Result<(MarketState, LiquidityPosition)> {
    let resolver = AccountResolver::new(twob_anchor::ID);
    let market_address = resolver.market_pda(market_id).address();
    let bookkeeping_address = resolver.bookkeeping_pda(&market_address).address();
    let position_address = resolver
        .liquidity_position_pda(&market_address, authority)
        .address();

    let response = program
        .rpc()
        .get_multiple_accounts_with_commitment(
            &[market_address, bookkeeping_address, position_address],
            CommitmentConfig::confirmed(),
        )
        .await
        .context("failed to fetch coherent market and position snapshot")?;
    let mut accounts = response.value.into_iter();
    let market = deserialize_program_account(accounts.next().flatten(), market_address, "market")?;
    let bookkeeping = deserialize_program_account(
        accounts.next().flatten(),
        bookkeeping_address,
        "bookkeeping",
    )?;
    let position = deserialize_program_account(
        accounts.next().flatten(),
        position_address,
        "liquidity position",
    )?;

    Ok((
        MarketState {
            market,
            bookkeeping,
            current_slot: response.context.slot,
        },
        position,
    ))
}

fn deserialize_program_account<T: AccountDeserialize>(
    account: Option<Account>,
    address: Pubkey,
    label: &str,
) -> anyhow::Result<T> {
    let account = account.with_context(|| format!("{label} account {address} was not found"))?;
    ensure!(
        account.owner == twob_anchor::ID,
        "{label} account {address} has unexpected owner {}",
        account.owner
    );

    let mut data = account.data.as_slice();
    T::try_deserialize(&mut data)
        .with_context(|| format!("failed to deserialize {label} account {address}"))
}
