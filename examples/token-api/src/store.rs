use crate::format::{
    BALANCE_CHANGES_FILE, IndexMeta, NO_ID, PUBKEYS_FILE, PubkeyRecord, SWAP_FLAG_QUOTE_IN,
    SWAP_FLAG_QUOTE_OUT, SWAPS_FILE, SwapRecord, TOKEN_ACCOUNTS_FILE, TOKEN_FLAG_QUOTE,
    TOKENS_FILE, TokenAccountRecord, TokenBalanceChangeRecord, TokenInfoRecord, amount_to_ui,
    decode_pubkey, encode_pubkey, read_record_file,
};
use anyhow::{Context, Result, bail};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct TokenStore {
    pub root: PathBuf,
    pub meta: IndexMeta,
    pub pubkeys: HashMap<u32, [u8; 32]>,
    pub ids_by_pubkey: HashMap<[u8; 32], u32>,
    pub tokens: HashMap<u32, TokenInfoRecord>,
    pub token_accounts: Vec<TokenAccountRecord>,
    pub balances: Vec<TokenBalanceChangeRecord>,
    pub swaps: Vec<SwapRecord>,
    quote_mints: HashSet<u32>,
    trades_by_mint: HashMap<u32, Vec<usize>>,
    latest_time: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct PricePoint {
    pub value: f64,
    pub update_unix_time: i64,
    pub price_change_24h: f64,
}

#[derive(Debug, Clone)]
pub struct TokenOverview {
    pub token: TokenInfoRecord,
    pub address: String,
    pub price: Option<PricePoint>,
    pub volume_24h_usd: f64,
    pub trades_24h: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct HistoryPoint {
    pub unix_time: i64,
    pub value: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct OhlcvPoint {
    pub unix_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume_usd: f64,
    pub trades: u64,
}

impl TokenStore {
    pub fn load(root: &Path) -> Result<Self> {
        let meta: IndexMeta = serde_json::from_slice(
            &fs::read(root.join(crate::format::META_FILE)).context("read meta.json")?,
        )
        .context("parse meta.json")?;
        let pubkey_rows = read_record_file::<PubkeyRecord>(&root.join(PUBKEYS_FILE))?;
        let token_rows = read_record_file::<TokenInfoRecord>(&root.join(TOKENS_FILE))?;
        let token_accounts =
            read_record_file::<TokenAccountRecord>(&root.join(TOKEN_ACCOUNTS_FILE))?;
        let balances =
            read_record_file::<TokenBalanceChangeRecord>(&root.join(BALANCE_CHANGES_FILE))?;
        let swaps = read_record_file::<SwapRecord>(&root.join(SWAPS_FILE))?;

        let mut pubkeys = HashMap::with_capacity(pubkey_rows.len());
        let mut ids_by_pubkey = HashMap::with_capacity(pubkey_rows.len());
        for row in pubkey_rows {
            pubkeys.insert(row.id, row.pubkey);
            ids_by_pubkey.insert(row.pubkey, row.id);
        }

        let tokens = token_rows
            .into_iter()
            .map(|token| (token.mint_id, token))
            .collect::<HashMap<_, _>>();
        let quote_mints = tokens
            .values()
            .filter(|token| token.flags & TOKEN_FLAG_QUOTE != 0)
            .map(|token| token.mint_id)
            .chain(
                meta.quote_mints
                    .iter()
                    .filter_map(|value| decode_pubkey(value).ok())
                    .filter_map(|key| ids_by_pubkey.get(&key).copied()),
            )
            .collect::<HashSet<_>>();

        let mut trades_by_mint: HashMap<u32, Vec<usize>> = HashMap::new();
        let mut latest_time = 0i64;
        for (index, swap) in swaps.iter().enumerate() {
            trades_by_mint
                .entry(swap.in_mint_id)
                .or_default()
                .push(index);
            trades_by_mint
                .entry(swap.out_mint_id)
                .or_default()
                .push(index);
            latest_time = latest_time.max(swap.block_time);
        }
        for indexes in trades_by_mint.values_mut() {
            indexes.sort_by_key(|index| {
                let swap = swaps[*index];
                (swap.block_time, swap.slot, swap.tx_index)
            });
        }
        latest_time = latest_time.max(
            tokens
                .values()
                .map(|token| token.last_block_time)
                .max()
                .unwrap_or_default(),
        );

        Ok(Self {
            root: root.to_path_buf(),
            meta,
            pubkeys,
            ids_by_pubkey,
            tokens,
            token_accounts,
            balances,
            swaps,
            quote_mints,
            trades_by_mint,
            latest_time,
        })
    }

    pub fn token_id_for_address(&self, address: &str) -> Result<u32> {
        let key = decode_pubkey(address)?;
        let Some(id) = self.ids_by_pubkey.get(&key).copied() else {
            bail!("token {address} was not observed in this index");
        };
        if !self.tokens.contains_key(&id) {
            bail!("token {address} was not observed in this index");
        }
        Ok(id)
    }

    pub fn address_for_id(&self, id: u32) -> Option<String> {
        self.pubkeys.get(&id).map(encode_pubkey)
    }

    pub fn pubkey_for_id(&self, id: u32) -> Option<[u8; 32]> {
        self.pubkeys.get(&id).copied()
    }

    pub fn price_by_address(&self, address: &str) -> Result<Option<PricePoint>> {
        let id = self.token_id_for_address(address)?;
        Ok(self.price_by_id(id))
    }

    pub fn price_by_id(&self, mint_id: u32) -> Option<PricePoint> {
        if self.quote_mints.contains(&mint_id) {
            return Some(PricePoint {
                value: 1.0,
                update_unix_time: self.latest_time,
                price_change_24h: 0.0,
            });
        }

        let indexes = self.trades_by_mint.get(&mint_id)?;
        let latest = indexes
            .iter()
            .rev()
            .filter_map(|index| self.swap_price_for_mint(self.swaps[*index], mint_id))
            .next()?;
        let prior_cutoff = latest.update_unix_time - 86_400;
        let prior = indexes
            .iter()
            .rev()
            .filter_map(|index| self.swap_price_for_mint(self.swaps[*index], mint_id))
            .find(|point| point.update_unix_time <= prior_cutoff);
        let price_change_24h = prior
            .filter(|point| point.value > 0.0)
            .map(|point| ((latest.value - point.value) / point.value) * 100.0)
            .unwrap_or(0.0);

        Some(PricePoint {
            value: latest.value,
            update_unix_time: latest.update_unix_time,
            price_change_24h,
        })
    }

    pub fn token_overview(&self, mint_id: u32) -> Option<TokenOverview> {
        let token = *self.tokens.get(&mint_id)?;
        let address = self.address_for_id(mint_id)?;
        let price = self.price_by_id(mint_id);
        let cutoff = self.latest_time.saturating_sub(86_400);
        let mut volume_24h_usd = 0.0;
        let mut trades_24h = 0u64;
        if let Some(indexes) = self.trades_by_mint.get(&mint_id) {
            for index in indexes.iter().rev() {
                let swap = self.swaps[*index];
                if swap.block_time < cutoff {
                    break;
                }
                if let Some(volume) = self.swap_volume_usd(swap, mint_id) {
                    volume_24h_usd += volume;
                    trades_24h += 1;
                }
            }
        }
        Some(TokenOverview {
            token,
            address,
            price,
            volume_24h_usd,
            trades_24h,
        })
    }

    pub fn token_list(&self, offset: usize, limit: usize) -> Vec<TokenOverview> {
        let mut rows = self
            .tokens
            .keys()
            .filter_map(|mint_id| self.token_overview(*mint_id))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            right
                .volume_24h_usd
                .partial_cmp(&left.volume_24h_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.address.cmp(&right.address))
        });
        rows.into_iter().skip(offset).take(limit).collect()
    }

    pub fn history(
        &self,
        mint_id: u32,
        time_from: Option<i64>,
        time_to: Option<i64>,
        interval_seconds: i64,
    ) -> Vec<HistoryPoint> {
        let mut buckets: BTreeMap<i64, f64> = BTreeMap::new();
        let Some(indexes) = self.trades_by_mint.get(&mint_id) else {
            return Vec::new();
        };
        for index in indexes {
            let swap = self.swaps[*index];
            if !time_in_range(swap.block_time, time_from, time_to) {
                continue;
            }
            let Some(point) = self.swap_price_for_mint(swap, mint_id) else {
                continue;
            };
            let bucket = bucket_time(point.update_unix_time, interval_seconds);
            buckets.insert(bucket, point.value);
        }
        buckets
            .into_iter()
            .map(|(unix_time, value)| HistoryPoint { unix_time, value })
            .collect()
    }

    pub fn ohlcv(
        &self,
        mint_id: u32,
        time_from: Option<i64>,
        time_to: Option<i64>,
        interval_seconds: i64,
    ) -> Vec<OhlcvPoint> {
        let Some(indexes) = self.trades_by_mint.get(&mint_id) else {
            return Vec::new();
        };
        let mut buckets: BTreeMap<i64, OhlcvPoint> = BTreeMap::new();
        for index in indexes {
            let swap = self.swaps[*index];
            if !time_in_range(swap.block_time, time_from, time_to) {
                continue;
            }
            let Some(price) = self.swap_price_for_mint(swap, mint_id) else {
                continue;
            };
            let bucket = bucket_time(price.update_unix_time, interval_seconds);
            let volume = self.swap_volume_usd(swap, mint_id).unwrap_or_default();
            buckets
                .entry(bucket)
                .and_modify(|row| {
                    row.high = row.high.max(price.value);
                    row.low = row.low.min(price.value);
                    row.close = price.value;
                    row.volume_usd += volume;
                    row.trades += 1;
                })
                .or_insert(OhlcvPoint {
                    unix_time: bucket,
                    open: price.value,
                    high: price.value,
                    low: price.value,
                    close: price.value,
                    volume_usd: volume,
                    trades: 1,
                });
        }
        buckets.into_values().collect()
    }

    pub fn trades(
        &self,
        mint_id: u32,
        offset: usize,
        limit: usize,
        descending: bool,
    ) -> Vec<SwapRecord> {
        let Some(indexes) = self.trades_by_mint.get(&mint_id) else {
            return Vec::new();
        };
        let iter: Box<dyn Iterator<Item = &usize>> = if descending {
            Box::new(indexes.iter().rev())
        } else {
            Box::new(indexes.iter())
        };
        iter.skip(offset)
            .take(limit)
            .map(|index| self.swaps[*index])
            .collect()
    }

    pub fn swap_price_for_mint(&self, swap: SwapRecord, mint_id: u32) -> Option<PricePoint> {
        if swap.price_micros == 0 {
            return None;
        }
        let value = swap.price_micros as f64 / 1_000_000.0;
        if swap.in_mint_id == mint_id && swap.flags & SWAP_FLAG_QUOTE_OUT != 0 {
            return Some(PricePoint {
                value,
                update_unix_time: swap.block_time,
                price_change_24h: 0.0,
            });
        }
        if swap.out_mint_id == mint_id && swap.flags & SWAP_FLAG_QUOTE_IN != 0 {
            return Some(PricePoint {
                value,
                update_unix_time: swap.block_time,
                price_change_24h: 0.0,
            });
        }
        None
    }

    pub fn swap_volume_usd(&self, swap: SwapRecord, mint_id: u32) -> Option<f64> {
        if swap.in_mint_id == mint_id && swap.flags & SWAP_FLAG_QUOTE_OUT != 0 {
            return Some(amount_to_ui(swap.amount_out, swap.out_decimals));
        }
        if swap.out_mint_id == mint_id && swap.flags & SWAP_FLAG_QUOTE_IN != 0 {
            return Some(amount_to_ui(swap.amount_in, swap.in_decimals));
        }
        let price = self.swap_price_for_mint(swap, mint_id)?;
        let amount = if swap.in_mint_id == mint_id {
            amount_to_ui(swap.amount_in, swap.in_decimals)
        } else if swap.out_mint_id == mint_id {
            amount_to_ui(swap.amount_out, swap.out_decimals)
        } else {
            return None;
        };
        Some(amount * price.value)
    }

    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }

    pub fn latest_time(&self) -> i64 {
        self.latest_time
    }

    pub fn id_or_zero_address(&self, id: u32) -> String {
        if id == NO_ID {
            String::new()
        } else {
            self.address_for_id(id).unwrap_or_default()
        }
    }
}

pub fn parse_interval(value: Option<&str>) -> i64 {
    match value.unwrap_or("1m") {
        "1s" => 1,
        "15s" => 15,
        "30s" => 30,
        "1m" => 60,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1_800,
        "1h" => 3_600,
        "2h" => 7_200,
        "4h" => 14_400,
        "8h" => 28_800,
        "12h" => 43_200,
        "1d" | "24h" => 86_400,
        _ => 60,
    }
}

fn bucket_time(unix_time: i64, interval_seconds: i64) -> i64 {
    if interval_seconds <= 1 {
        unix_time
    } else {
        unix_time - unix_time.rem_euclid(interval_seconds)
    }
}

fn time_in_range(value: i64, from: Option<i64>, to: Option<i64>) -> bool {
    from.is_none_or(|from| value >= from) && to.is_none_or(|to| value <= to)
}
