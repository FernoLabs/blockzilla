export function formatInteger(value: number | string): string {
  const number = Number(value);
  return Number.isFinite(number) ? number.toLocaleString('en-US', { maximumFractionDigits: 0 }) : '—';
}

export function formatCompact(value: number | string): string {
  const number = Number(value);
  return Number.isFinite(number)
    ? Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 }).format(number)
    : '—';
}

export function formatBaseUnits(value: string, maximumFractionDigits = 8): string {
  const number = Number(value);
  return Number.isFinite(number)
    ? number.toLocaleString('en-US', {
        minimumFractionDigits: 0,
        maximumFractionDigits
      })
    : '—';
}

export function formatRawAmount(
  rawAmount: string,
  decimals: number,
  maximumFractionDigits = 6
): string {
  if (!/^\d+$/.test(rawAmount) || !Number.isInteger(decimals) || decimals < 0 || decimals > 255) {
    return '—';
  }
  const padded = rawAmount.padStart(decimals + 1, '0');
  const whole = decimals === 0 ? padded : padded.slice(0, -decimals);
  const fraction = decimals === 0 ? '' : padded.slice(-decimals);
  const visibleFraction = fraction.slice(0, maximumFractionDigits).replace(/0+$/, '');
  const groupedWhole = whole.replace(/^0+(?=\d)/, '').replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return visibleFraction ? `${groupedWhole}.${visibleFraction}` : groupedWhole;
}

export function formatPercentFromPpm(value: number): string {
  return `${(value / 10_000).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4
  })}%`;
}

export function formatBytes(value: number): string {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let amount = value;
  let unit = 0;
  while (amount >= 1024 && unit < units.length - 1) {
    amount /= 1024;
    unit += 1;
  }
  return `${amount.toLocaleString('en-US', { maximumFractionDigits: unit ? 3 : 0 })} ${units[unit]}`;
}

export function formatDate(value: string): string {
  return new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC'
  }).format(new Date(`${value}T00:00:00Z`));
}

export function shortAddress(value: string): string {
  return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value;
}

export function rangeLabel(value: string): string {
  const labels: Record<string, string> = {
    greater_than_0_and_less_than_1: '>0 to <1',
    '1_to_less_than_10': '1 to <10',
    '10_to_less_than_100': '10 to <100',
    '100_to_less_than_1000': '100 to <1,000',
    '1000_to_less_than_10000': '1,000 to <10,000',
    '10000_to_less_than_100000': '10,000 to <100,000',
    '100000_to_less_than_1000000': '100,000 to <1,000,000',
    '1000000_or_more': '1,000,000 or more'
  };
  return labels[value] ?? value;
}
