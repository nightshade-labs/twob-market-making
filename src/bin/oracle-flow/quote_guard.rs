use std::fmt;

const BPS_DENOMINATOR: u128 = 10_000;
const ORACLE_PRICE_SCALE: u128 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteGuardError {
    ZeroBaseFlow,
    ZeroQuoteFlow,
    InvalidOraclePrice,
    OraclePriceOutOfRange,
    OracleTooOld {
        age_secs: u64,
        max_age_secs: u64,
    },
    OracleTooFarInFuture {
        future_secs: u64,
        max_future_skew_secs: u64,
    },
    InvalidMaxDeviationBps {
        max_deviation_bps: u64,
    },
    ArithmeticOverflow,
    PriceDeviationExceeded {
        deviation_bps: u128,
        max_deviation_bps: u64,
    },
}

impl fmt::Display for QuoteGuardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBaseFlow => formatter.write_str("base flow must be greater than zero"),
            Self::ZeroQuoteFlow => formatter.write_str("quote flow must be greater than zero"),
            Self::InvalidOraclePrice => {
                formatter.write_str("oracle price must be finite and greater than zero")
            }
            Self::OraclePriceOutOfRange => {
                formatter.write_str("oracle price cannot be represented safely")
            }
            Self::OracleTooOld {
                age_secs,
                max_age_secs,
            } => write!(
                formatter,
                "oracle price is stale: age={age_secs}s max_age={max_age_secs}s"
            ),
            Self::OracleTooFarInFuture {
                future_secs,
                max_future_skew_secs,
            } => write!(
                formatter,
                "oracle timestamp is too far in the future: future={future_secs}s max_skew={max_future_skew_secs}s"
            ),
            Self::InvalidMaxDeviationBps { max_deviation_bps } => write!(
                formatter,
                "maximum quote deviation must not exceed 10000 bps: {max_deviation_bps}"
            ),
            Self::ArithmeticOverflow => {
                formatter.write_str("quote validation arithmetic overflowed")
            }
            Self::PriceDeviationExceeded {
                deviation_bps,
                max_deviation_bps,
            } => write!(
                formatter,
                "quote price deviation exceeds limit: deviation={deviation_bps}bps max_deviation={max_deviation_bps}bps"
            ),
        }
    }
}

impl std::error::Error for QuoteGuardError {}

pub fn validate_oracle_freshness(
    oracle_price: f64,
    oracle_timestamp: u64,
    now_timestamp: u64,
    max_oracle_age_secs: u64,
    max_future_skew_secs: u64,
) -> Result<(), QuoteGuardError> {
    if !oracle_price.is_finite() || oracle_price <= 0.0 {
        return Err(QuoteGuardError::InvalidOraclePrice);
    }

    if oracle_timestamp > now_timestamp {
        let future_secs = oracle_timestamp
            .checked_sub(now_timestamp)
            .ok_or(QuoteGuardError::ArithmeticOverflow)?;
        if future_secs > max_future_skew_secs {
            return Err(QuoteGuardError::OracleTooFarInFuture {
                future_secs,
                max_future_skew_secs,
            });
        }
        return Ok(());
    }

    let age_secs = now_timestamp
        .checked_sub(oracle_timestamp)
        .ok_or(QuoteGuardError::ArithmeticOverflow)?;
    if age_secs > max_oracle_age_secs {
        return Err(QuoteGuardError::OracleTooOld {
            age_secs,
            max_age_secs: max_oracle_age_secs,
        });
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn validate_quote(
    base_flow: u64,
    quote_flow: u64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    oracle_price: f64,
    oracle_timestamp: u64,
    now_timestamp: u64,
    max_deviation_bps: u64,
    max_oracle_age_secs: u64,
    max_future_skew_secs: u64,
) -> Result<(), QuoteGuardError> {
    validate_oracle_freshness(
        oracle_price,
        oracle_timestamp,
        now_timestamp,
        max_oracle_age_secs,
        max_future_skew_secs,
    )?;

    if base_flow == 0 {
        return Err(QuoteGuardError::ZeroBaseFlow);
    }
    if quote_flow == 0 {
        return Err(QuoteGuardError::ZeroQuoteFlow);
    }
    if max_deviation_bps > BPS_DENOMINATOR as u64 {
        return Err(QuoteGuardError::InvalidMaxDeviationBps { max_deviation_bps });
    }

    let oracle_price_scaled = oracle_price_to_fixed(oracle_price)?;
    let base_scale = checked_decimal_scale(base_token_decimals)?;
    let quote_scale = checked_decimal_scale(quote_token_decimals)?;

    // Compare both prices over a common denominator without using floating-point
    // flow arithmetic:
    //   actual = quote_flow * 10^base_decimals / (base_flow * 10^quote_decimals)
    //   oracle = oracle_price_scaled / ORACLE_PRICE_SCALE
    let actual_value = u128::from(quote_flow)
        .checked_mul(base_scale)
        .and_then(|value| value.checked_mul(ORACLE_PRICE_SCALE))
        .ok_or(QuoteGuardError::ArithmeticOverflow)?;
    let oracle_value = oracle_price_scaled
        .checked_mul(u128::from(base_flow))
        .and_then(|value| value.checked_mul(quote_scale))
        .ok_or(QuoteGuardError::ArithmeticOverflow)?;
    let deviation = actual_value.abs_diff(oracle_value);
    let deviation_scaled = deviation
        .checked_mul(BPS_DENOMINATOR)
        .ok_or(QuoteGuardError::ArithmeticOverflow)?;
    let allowed_deviation = oracle_value
        .checked_mul(u128::from(max_deviation_bps))
        .ok_or(QuoteGuardError::ArithmeticOverflow)?;

    if deviation_scaled > allowed_deviation {
        return Err(QuoteGuardError::PriceDeviationExceeded {
            deviation_bps: checked_ceil_div(deviation_scaled, oracle_value)?,
            max_deviation_bps,
        });
    }

    Ok(())
}

fn oracle_price_to_fixed(oracle_price: f64) -> Result<u128, QuoteGuardError> {
    let scaled = oracle_price * ORACLE_PRICE_SCALE as f64;
    if !scaled.is_finite() {
        return Err(QuoteGuardError::ArithmeticOverflow);
    }
    if scaled < 1.0 || scaled >= u128::MAX as f64 {
        return Err(QuoteGuardError::OraclePriceOutOfRange);
    }

    let rounded = scaled.round();
    if !rounded.is_finite() || rounded < 1.0 || rounded >= u128::MAX as f64 {
        return Err(QuoteGuardError::OraclePriceOutOfRange);
    }

    Ok(rounded as u128)
}

fn checked_decimal_scale(decimals: u8) -> Result<u128, QuoteGuardError> {
    10_u128
        .checked_pow(u32::from(decimals))
        .ok_or(QuoteGuardError::ArithmeticOverflow)
}

fn checked_ceil_div(numerator: u128, denominator: u128) -> Result<u128, QuoteGuardError> {
    if denominator == 0 {
        return Err(QuoteGuardError::ArithmeticOverflow);
    }

    let quotient = numerator / denominator;
    if numerator.checked_rem(denominator) == Some(0) {
        Ok(quotient)
    } else {
        quotient
            .checked_add(1)
            .ok_or(QuoteGuardError::ArithmeticOverflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_800_000_000;
    const MAX_AGE_SECS: u64 = 30;
    const MAX_FUTURE_SKEW_SECS: u64 = 5;

    fn validate_sol_usdc_quote(
        base_flow: u64,
        quote_flow: u64,
        oracle_price: f64,
        max_deviation_bps: u64,
    ) -> Result<(), QuoteGuardError> {
        validate_quote(
            base_flow,
            quote_flow,
            9,
            6,
            oracle_price,
            NOW,
            NOW,
            max_deviation_bps,
            MAX_AGE_SECS,
            MAX_FUTURE_SKEW_SECS,
        )
    }

    #[test]
    fn rejects_incident_quote() {
        let result = validate_sol_usdc_quote(107_950_535, 12_743_301, 97.66, 100);

        assert!(matches!(
            result,
            Err(QuoteGuardError::PriceDeviationExceeded {
                deviation_bps: 2_088..,
                max_deviation_bps: 100,
            })
        ));
    }

    #[test]
    fn accepts_normal_quote() {
        assert_eq!(
            validate_sol_usdc_quote(100_000_000, 9_766_000, 97.66, 10),
            Ok(())
        );
    }

    #[test]
    fn accepts_exact_deviation_boundary_and_rejects_value_above_it() {
        assert_eq!(
            validate_sol_usdc_quote(1_000_000_000, 101_000_000, 100.0, 100),
            Ok(())
        );
        assert!(matches!(
            validate_sol_usdc_quote(1_000_000_000, 101_000_001, 100.0, 100),
            Err(QuoteGuardError::PriceDeviationExceeded { .. })
        ));
    }

    #[test]
    fn respects_token_decimals() {
        assert_eq!(
            validate_quote(
                2_000_000,
                84_000_000_000,
                6,
                9,
                42.0,
                NOW,
                NOW,
                0,
                MAX_AGE_SECS,
                MAX_FUTURE_SKEW_SECS,
            ),
            Ok(())
        );
    }

    #[test]
    fn rejects_zero_flows() {
        assert_eq!(
            validate_sol_usdc_quote(0, 9_766_000, 97.66, 100),
            Err(QuoteGuardError::ZeroBaseFlow)
        );
        assert_eq!(
            validate_sol_usdc_quote(100_000_000, 0, 97.66, 100),
            Err(QuoteGuardError::ZeroQuoteFlow)
        );
    }

    #[test]
    fn rejects_invalid_oracle_prices() {
        for price in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                validate_oracle_freshness(price, NOW, NOW, MAX_AGE_SECS, MAX_FUTURE_SKEW_SECS,),
                Err(QuoteGuardError::InvalidOraclePrice)
            );
        }
    }

    #[test]
    fn accepts_maximum_oracle_age_and_rejects_older_price() {
        assert_eq!(
            validate_oracle_freshness(
                97.66,
                NOW - MAX_AGE_SECS,
                NOW,
                MAX_AGE_SECS,
                MAX_FUTURE_SKEW_SECS,
            ),
            Ok(())
        );
        assert_eq!(
            validate_oracle_freshness(
                97.66,
                NOW - MAX_AGE_SECS - 1,
                NOW,
                MAX_AGE_SECS,
                MAX_FUTURE_SKEW_SECS,
            ),
            Err(QuoteGuardError::OracleTooOld {
                age_secs: MAX_AGE_SECS + 1,
                max_age_secs: MAX_AGE_SECS,
            })
        );
    }

    #[test]
    fn allows_small_future_skew_and_rejects_larger_skew() {
        assert_eq!(
            validate_oracle_freshness(
                97.66,
                NOW + MAX_FUTURE_SKEW_SECS,
                NOW,
                MAX_AGE_SECS,
                MAX_FUTURE_SKEW_SECS,
            ),
            Ok(())
        );
        assert_eq!(
            validate_oracle_freshness(
                97.66,
                NOW + MAX_FUTURE_SKEW_SECS + 1,
                NOW,
                MAX_AGE_SECS,
                MAX_FUTURE_SKEW_SECS,
            ),
            Err(QuoteGuardError::OracleTooFarInFuture {
                future_secs: MAX_FUTURE_SKEW_SECS + 1,
                max_future_skew_secs: MAX_FUTURE_SKEW_SECS,
            })
        );
    }

    #[test]
    fn rejects_unsupported_deviation_limit() {
        assert_eq!(
            validate_sol_usdc_quote(100_000_000, 9_766_000, 97.66, 10_001),
            Err(QuoteGuardError::InvalidMaxDeviationBps {
                max_deviation_bps: 10_001,
            })
        );
    }

    #[test]
    fn rejects_decimal_scale_overflow() {
        assert_eq!(
            validate_quote(
                1,
                1,
                39,
                0,
                1.0,
                NOW,
                NOW,
                100,
                MAX_AGE_SECS,
                MAX_FUTURE_SKEW_SECS,
            ),
            Err(QuoteGuardError::ArithmeticOverflow)
        );
    }

    #[test]
    fn rejects_cross_multiplication_overflow() {
        assert_eq!(
            validate_quote(
                1,
                u64::MAX,
                38,
                0,
                1.0,
                NOW,
                NOW,
                100,
                MAX_AGE_SECS,
                MAX_FUTURE_SKEW_SECS,
            ),
            Err(QuoteGuardError::ArithmeticOverflow)
        );
    }

    #[test]
    fn rejects_oracle_price_conversion_overflow() {
        assert_eq!(
            validate_sol_usdc_quote(100_000_000, 9_766_000, f64::MAX, 100),
            Err(QuoteGuardError::ArithmeticOverflow)
        );
    }
}
