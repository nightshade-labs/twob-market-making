use anchor_client::{Program, solana_sdk::signature::Keypair};
use anchor_lang::prelude::{instruction::Instruction, *};
use anchor_spl::associated_token::get_associated_token_address_with_program_id;
use std::sync::Arc;

use crate::{
    AccountResolver, get_token_program_id,
    twob_anchor::{
        self,
        accounts::Market,
        client::{accounts, args},
    },
};

pub async fn build_public_stop_liquidity_position_instruction(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    stop_liquidity_position_args: args::PublicStopLiquidityPosition,
) -> Instruction {
    let resolver = AccountResolver::new(twob_anchor::ID);

    let liquidity_provider = program.payer();
    let market_pda = resolver.market_pda(market_id);
    let market = program
        .account::<Market>(market_pda.address())
        .await
        .unwrap();

    let liquidity_position_pda =
        resolver.liquidity_position_pda(&market_pda.address(), &liquidity_provider);
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());
    let current_exits_pda = resolver.exits_pda(
        &market_pda.address(),
        stop_liquidity_position_args.reference_index,
    );
    let previous_exits_pda = resolver.exits_pda(
        &market_pda.address(),
        stop_liquidity_position_args.reference_index - 1,
    );
    let current_prices_pda = resolver.prices_pda(
        &market_pda.address(),
        stop_liquidity_position_args.reference_index,
    );
    let previous_prices_pda = resolver.prices_pda(
        &market_pda.address(),
        stop_liquidity_position_args.reference_index - 1,
    );

    let base_token_program = get_token_program_id(program, &market.base_mint)
        .await
        .unwrap();
    let quote_token_program = get_token_program_id(program, &market.quote_mint)
        .await
        .unwrap();

    let signer_base_token_account = get_associated_token_address_with_program_id(
        &liquidity_provider,
        &market.base_mint,
        &base_token_program,
    );
    let signer_quote_token_account = get_associated_token_address_with_program_id(
        &liquidity_provider,
        &market.quote_mint,
        &quote_token_program,
    );
    let base_vault = get_associated_token_address_with_program_id(
        &market_pda.address(),
        &market.base_mint,
        &base_token_program,
    );
    let quote_vault = get_associated_token_address_with_program_id(
        &market_pda.address(),
        &market.quote_mint,
        &quote_token_program,
    );

    program
        .request()
        .accounts(accounts::PublicStopLiquidityPosition {
            signer: liquidity_provider,
            position_authority: liquidity_provider,
            base_mint: market.base_mint,
            quote_mint: market.quote_mint,
            signer_base_token_account,
            signer_quote_token_account,
            market: market_pda.address(),
            liquidity_position: liquidity_position_pda.address(),
            base_vault,
            quote_vault,
            bookkeeping: bookkeeping_pda.address(),
            current_exits: current_exits_pda.address(),
            previous_exits: previous_exits_pda.address(),
            current_prices: current_prices_pda.address(),
            previous_prices: previous_prices_pda.address(),
            base_token_program,
            quote_token_program,
            associated_token_program: anchor_spl::associated_token::ID,
            system_program: system_program::ID,
        })
        .args(stop_liquidity_position_args)
        .instructions()
        .unwrap()
        .remove(0)
}

pub async fn execute_stop_position(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    reference_index: u64,
    signer: Arc<Keypair>,
) -> anyhow::Result<()> {
    println!("🚨🚨🚨🚨 Position has accumulated debt. Stopping position.");

    let args = args::PublicStopLiquidityPosition { reference_index };
    let ix = build_public_stop_liquidity_position_instruction(program, market_id, args).await;

    program
        .request()
        .instruction(ix)
        .signer(signer)
        .send()
        .await?;

    Ok(())
}
