import { readFileSync, writeFileSync } from "node:fs";

const [reportPath, templatePath, outputPath] = process.argv.slice(2);
if (!reportPath || !templatePath || !outputPath) {
  throw new Error("usage: node build-spyx-token-report.mjs REPORT TEMPLATE OUTPUT");
}

const source = JSON.parse(readFileSync(reportPath, "utf8"));
const template = readFileSync(templatePath, "utf8");
const marker = "__SPYX_REPORT_DATA__";
if (template.split(marker).length !== 2) {
  throw new Error("visual template must contain exactly one data marker");
}

if (
  source.schema_version !== 1 ||
  source.artifact_kind !== "token_public_balance_history" ||
  source.bounded_selected_dump_scan_complete !== true ||
  source.metadata_balance_chain_continuous_from_spyx_mint_creation !== true ||
  source.instruction_replay_performed !== false ||
  source.daily_public_balance_series_complete !== true ||
  source.daily_selected_transaction_counts_complete !== true ||
  source.final_public_balance.decimals !== 8 ||
  !Array.isArray(source.daily) ||
  source.daily.length === 0
) {
  throw new Error("token history report is incomplete or incompatible with this SPYx visual");
}

const finalBalance = source.final_public_balance;
const finalSupplyRaw = BigInt(finalBalance.public_raw_balance_sum.raw_amount);
const groupDigits = value => {
  const [whole, fraction] = String(value).split(".");
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return fraction === undefined ? grouped : `${grouped}.${fraction}`;
};
const percent4Floor = raw => {
  if (finalSupplyRaw === 0n) return "0.0000";
  const scaled = BigInt(raw) * 1_000_000n / finalSupplyRaw;
  return `${scaled / 10_000n}.${String(scaled % 10_000n).padStart(4, "0")}`;
};
const shortAddress = value => `${value.slice(0, 6)}…${value.slice(-6)}`;
const finalDay = source.daily.at(-1);
if (
  finalDay.positive_public_balance_holders !== finalBalance.positive_public_balance_holders ||
  finalDay.active_public_token_accounts !== finalBalance.active_public_token_accounts ||
  finalDay.public_raw_balance_sum.raw_amount !== finalBalance.public_raw_balance_sum.raw_amount
) {
  throw new Error("last daily public balance does not match the final public balance");
}
const smallest = finalBalance.smallest_25_positive_holders[0];
if (!smallest) {
  throw new Error("token history report has no smallest positive holder row");
}
const sourceDays = new Map(source.daily.map(day => [day.utc_date, day]));
const firstDate = new Date(`${source.daily[0].utc_date}T00:00:00Z`);
const lastDate = new Date(`${source.daily.at(-1).utc_date}T00:00:00Z`);
const completeDaily = [];
let previousDay;
for (let cursor = firstDate; cursor <= lastDate; cursor = new Date(cursor.getTime() + 86_400_000)) {
  const utcDate = cursor.toISOString().slice(0, 10);
  const observedDay = sourceDays.get(utcDate);
  if (observedDay) previousDay = observedDay;
  if (!previousDay) throw new Error(`no balance state for ${utcDate}`);
  completeDaily.push({
    utc_date: utcDate,
    positive_public_balance_holders: previousDay.positive_public_balance_holders,
    active_public_token_accounts: previousDay.active_public_token_accounts,
    public_bilateral_movement: observedDay?.public_bilateral_movement ?? { base_units: "0" },
    top_1_concentration: previousDay.top_1_concentration,
    top_10_concentration: previousDay.top_10_concentration,
    top_100_concentration: previousDay.top_100_concentration
  });
}
const data = {
  summary: {
    holders: finalBalance.positive_public_balance_holders,
    accounts: finalBalance.active_public_token_accounts,
    publicBalance: groupDigits(finalBalance.public_raw_balance_sum.base_units),
    top1Share: (finalBalance.top_1_concentration.supply_share_parts_per_million_floor / 10_000).toFixed(4),
    top10Share: (finalBalance.top_10_concentration.supply_share_parts_per_million_floor / 10_000).toFixed(4),
    publicMovement: groupDigits(source.public_volume_totals.public_bilateral_movement.base_units)
  },
  daily: completeDaily.map(day => [
    day.utc_date,
    day.positive_public_balance_holders,
    day.active_public_token_accounts,
    Number(day.public_bilateral_movement.base_units),
    day.top_1_concentration.supply_share_parts_per_million_floor / 10_000,
    day.top_10_concentration.supply_share_parts_per_million_floor / 10_000,
    day.top_100_concentration.supply_share_parts_per_million_floor / 10_000
  ]),
  bands: source.final_public_balance.balance_distribution.map(band => ({
    label: ({
      greater_than_0_and_less_than_1: ">0–<1",
      "1_to_less_than_10": "1–<10",
      "10_to_less_than_100": "10–<100",
      "100_to_less_than_1000": "100–<1k",
      "1000_to_less_than_10000": "1k–<10k",
      "10000_to_less_than_100000": "10k–<100k",
      "100000_to_less_than_1000000": "100k–<1m",
      "1000000_or_more": "1m+"
    })[band.base_unit_range],
    count: band.holder_count,
    amount: Number(band.public_balance.base_units)
  })),
  largest: source.final_public_balance.largest_25_holders.slice(0, 5).map(holder => ({
    owner: holder.owner,
    short: shortAddress(holder.owner),
    amount: groupDigits(holder.public_balance.base_units),
    share: percent4Floor(holder.public_balance.raw_amount)
  })),
  smallest: {
    owner: smallest.owner,
    short: shortAddress(smallest.owner),
    tokenAccounts: smallest.token_account_count,
    rawAmount: smallest.public_balance.raw_amount,
    amount: groupDigits(smallest.public_balance.base_units)
  },
  peakDays: source.top_25_volume_days.slice(0, 5).map(day => ({
    date: day.utc_date,
    movement: groupDigits(day.public_bilateral_movement.base_units),
    selectedTransactions: day.selected_transactions,
    balanceChangingTransactions: day.public_balance_changing_transactions
  }))
};

const output = template.replace(marker, JSON.stringify(data));
if (Buffer.byteLength(output) >= 1_000_000) {
  throw new Error("visual output exceeds the 1 MB limit");
}
writeFileSync(outputPath, output);
