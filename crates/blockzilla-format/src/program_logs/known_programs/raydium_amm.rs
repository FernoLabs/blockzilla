use data_encoding::BASE64;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

pub const STR_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum RaydiumAmmLog {
    Init(InitLog),
    Deposit(DepositLog),
    Withdraw(WithdrawLog),
    SwapBaseIn(SwapBaseInLog),
    SwapBaseOut(SwapBaseOutLog),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct InitLog {
    pub time: u64,
    pub pc_decimals: u8,
    pub coin_decimals: u8,
    pub pc_lot_size: u64,
    pub coin_lot_size: u64,
    pub pc_amount: u64,
    pub coin_amount: u64,
    pub market: [u8; 32],
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct DepositLog {
    pub max_coin: u64,
    pub max_pc: u64,
    pub base: u64,
    pub pool_coin: u64,
    pub pool_pc: u64,
    pub pool_lp: u64,
    pub calc_pnl_x: u128,
    pub calc_pnl_y: u128,
    pub deduct_coin: u64,
    pub deduct_pc: u64,
    pub mint_lp: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WithdrawLog {
    pub withdraw_lp: u64,
    pub user_lp: u64,
    pub pool_coin: u64,
    pub pool_pc: u64,
    pub pool_lp: u64,
    pub calc_pnl_x: u128,
    pub calc_pnl_y: u128,
    pub out_coin: u64,
    pub out_pc: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SwapBaseInLog {
    pub amount_in: u64,
    pub minimum_out: u64,
    pub direction: u64,
    pub user_source: u64,
    pub pool_coin: u64,
    pub pool_pc: u64,
    pub out_amount: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SwapBaseOutLog {
    pub max_in: u64,
    pub amount_out: u64,
    pub direction: u64,
    pub user_source: u64,
    pub pool_coin: u64,
    pub pool_pc: u64,
    pub deduct_in: u64,
}

impl RaydiumAmmLog {
    pub fn parse(payload: &str) -> Option<Self> {
        let encoded = payload.strip_prefix("ray_log: ")?.trim();
        let decoded_len = BASE64.decode_len(encoded.len()).ok()?;
        let mut bytes = vec![0; decoded_len];
        let used = BASE64.decode_mut(encoded.as_bytes(), &mut bytes).ok()?;
        bytes.truncate(used);

        let mut reader = Reader::new(&bytes);
        let log_type = reader.u8()?;
        let log = match log_type {
            0 => Self::Init(InitLog {
                time: reader.u64()?,
                pc_decimals: reader.u8()?,
                coin_decimals: reader.u8()?,
                pc_lot_size: reader.u64()?,
                coin_lot_size: reader.u64()?,
                pc_amount: reader.u64()?,
                coin_amount: reader.u64()?,
                market: reader.pubkey()?,
            }),
            1 => Self::Deposit(DepositLog {
                max_coin: reader.u64()?,
                max_pc: reader.u64()?,
                base: reader.u64()?,
                pool_coin: reader.u64()?,
                pool_pc: reader.u64()?,
                pool_lp: reader.u64()?,
                calc_pnl_x: reader.u128()?,
                calc_pnl_y: reader.u128()?,
                deduct_coin: reader.u64()?,
                deduct_pc: reader.u64()?,
                mint_lp: reader.u64()?,
            }),
            2 => Self::Withdraw(WithdrawLog {
                withdraw_lp: reader.u64()?,
                user_lp: reader.u64()?,
                pool_coin: reader.u64()?,
                pool_pc: reader.u64()?,
                pool_lp: reader.u64()?,
                calc_pnl_x: reader.u128()?,
                calc_pnl_y: reader.u128()?,
                out_coin: reader.u64()?,
                out_pc: reader.u64()?,
            }),
            3 => Self::SwapBaseIn(SwapBaseInLog {
                amount_in: reader.u64()?,
                minimum_out: reader.u64()?,
                direction: reader.u64()?,
                user_source: reader.u64()?,
                pool_coin: reader.u64()?,
                pool_pc: reader.u64()?,
                out_amount: reader.u64()?,
            }),
            4 => Self::SwapBaseOut(SwapBaseOutLog {
                max_in: reader.u64()?,
                amount_out: reader.u64()?,
                direction: reader.u64()?,
                user_source: reader.u64()?,
                pool_coin: reader.u64()?,
                pool_pc: reader.u64()?,
                deduct_in: reader.u64()?,
            }),
            _ => return None,
        };

        reader.finished().then_some(log)
    }

    pub fn as_str(&self) -> String {
        let mut bytes = Vec::new();
        self.write_bincode_bytes(&mut bytes);
        format!("ray_log: {}", BASE64.encode(&bytes))
    }

    fn write_bincode_bytes(&self, out: &mut Vec<u8>) {
        match self {
            Self::Init(log) => {
                out.push(0);
                put_u64(out, log.time);
                out.push(log.pc_decimals);
                out.push(log.coin_decimals);
                put_u64(out, log.pc_lot_size);
                put_u64(out, log.coin_lot_size);
                put_u64(out, log.pc_amount);
                put_u64(out, log.coin_amount);
                out.extend_from_slice(&log.market);
            }
            Self::Deposit(log) => {
                out.push(1);
                put_u64(out, log.max_coin);
                put_u64(out, log.max_pc);
                put_u64(out, log.base);
                put_u64(out, log.pool_coin);
                put_u64(out, log.pool_pc);
                put_u64(out, log.pool_lp);
                put_u128(out, log.calc_pnl_x);
                put_u128(out, log.calc_pnl_y);
                put_u64(out, log.deduct_coin);
                put_u64(out, log.deduct_pc);
                put_u64(out, log.mint_lp);
            }
            Self::Withdraw(log) => {
                out.push(2);
                put_u64(out, log.withdraw_lp);
                put_u64(out, log.user_lp);
                put_u64(out, log.pool_coin);
                put_u64(out, log.pool_pc);
                put_u64(out, log.pool_lp);
                put_u128(out, log.calc_pnl_x);
                put_u128(out, log.calc_pnl_y);
                put_u64(out, log.out_coin);
                put_u64(out, log.out_pc);
            }
            Self::SwapBaseIn(log) => {
                out.push(3);
                put_u64(out, log.amount_in);
                put_u64(out, log.minimum_out);
                put_u64(out, log.direction);
                put_u64(out, log.user_source);
                put_u64(out, log.pool_coin);
                put_u64(out, log.pool_pc);
                put_u64(out, log.out_amount);
            }
            Self::SwapBaseOut(log) => {
                out.push(4);
                put_u64(out, log.max_in);
                put_u64(out, log.amount_out);
                put_u64(out, log.direction);
                put_u64(out, log.user_source);
                put_u64(out, log.pool_coin);
                put_u64(out, log.pool_pc);
                put_u64(out, log.deduct_in);
            }
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Option<u8> {
        let byte = *self.bytes.get(self.offset)?;
        self.offset += 1;
        Some(byte)
    }

    fn u64(&mut self) -> Option<u64> {
        let bytes = self.take::<8>()?;
        Some(u64::from_le_bytes(bytes))
    }

    fn u128(&mut self) -> Option<u128> {
        let bytes = self.take::<16>()?;
        Some(u128::from_le_bytes(bytes))
    }

    fn pubkey(&mut self) -> Option<[u8; 32]> {
        self.take::<32>()
    }

    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let end = self.offset.checked_add(N)?;
        let bytes = self.bytes.get(self.offset..end)?;
        self.offset = end;
        bytes.try_into().ok()
    }

    fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u128(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raydium_swap_base_in_round_trips_to_bincode_base64_payload() {
        let log = RaydiumAmmLog::SwapBaseIn(SwapBaseInLog {
            amount_in: 1_000_000,
            minimum_out: 990_000,
            direction: 2,
            user_source: 3_000_000,
            pool_coin: 4_000_000,
            pool_pc: 5_000_000,
            out_amount: 995_000,
        });

        let rendered = log.as_str();
        assert_eq!(RaydiumAmmLog::parse(&rendered), Some(log));
    }

    #[test]
    fn malformed_ray_log_is_not_recognized() {
        assert_eq!(RaydiumAmmLog::parse("ray_log: not-base64"), None);
        assert_eq!(RaydiumAmmLog::parse("ray_log: Aw=="), None);
    }
}
