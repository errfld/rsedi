pub(crate) fn usize_to_f64(value: usize) -> f64 {
    let value = u64::try_from(value).unwrap_or(u64::MAX);
    u64_to_f64(value)
}

pub(crate) fn u128_to_f64(value: u128) -> f64 {
    let high = u64::try_from(value >> 64).unwrap_or(u64::MAX);
    let low = u64::try_from(value & u128::from(u64::MAX)).unwrap_or(u64::MAX);
    u64_to_f64(high) * two_pow_64() + u64_to_f64(low)
}

fn u64_to_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * two_pow_32() + f64::from(low)
}

fn two_pow_32() -> f64 {
    2.0_f64.powi(32)
}

fn two_pow_64() -> f64 {
    let two_pow_32 = two_pow_32();
    two_pow_32 * two_pow_32
}
