use anchor_client::{Program, solana_sdk::signature::Keypair};
use anchor_lang::prelude::{instruction::Instruction, *};
use std::sync::Arc;

use crate::{
    AccountResolver,
    twob_anchor::{self, client::accounts, client::args},
};

pub fn build_public_stop_liquidity_position_instruction(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    update_flows_args: args::PublicStopLiquidityPosition,
) -> Instruction {
    let resolver = AccountResolver::new(twob_anchor::ID);

    let liquidity_provider = program.payer();
    let market_pda = resolver.market_pda(market_id);
    let liquidity_position_pda =
        resolver.liquidity_position_pda(&market_pda.address(), &liquidity_provider);
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());
    let current_exits_pda =
        resolver.exits_pda(&market_pda.address(), update_flows_args.reference_index);
    let previous_exits_pda =
        resolver.exits_pda(&market_pda.address(), update_flows_args.reference_index - 1);
    let current_prices_pda =
        resolver.prices_pda(&market_pda.address(), update_flows_args.reference_index);
    let previous_prices_pda =
        resolver.prices_pda(&market_pda.address(), update_flows_args.reference_index - 1);

    program
        .request()
        .accounts(accounts::PublicStopLiquidityPosition {
            signer: liquidity_provider,
            position_authority: liquidity_provider,
            market: market_pda.address(),
            liquidity_position: liquidity_position_pda.address(),
            bookkeeping: bookkeeping_pda.address(),
            current_exits: current_exits_pda.address(),
            previous_exits: previous_exits_pda.address(),
            current_prices: current_prices_pda.address(),
            previous_prices: previous_prices_pda.address(),
            system_program: system_program::ID,
        })
        .args(update_flows_args)
        .instructions()
        .unwrap()
        .remove(0)
}
