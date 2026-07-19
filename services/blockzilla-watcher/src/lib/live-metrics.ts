export type LiveMetricCapture = {
  state: string;
  eta_secs?: number | null;
  slots_per_sec?: number | null;
  rss_bytes?: number | null;
  peak_rss_bytes?: number | null;
  progress: {
    eta_secs?: number | null;
    blocks_per_sec?: number | null;
    rss_bytes?: number | null;
    peak_rss_bytes?: number | null;
  };
};

export type LiveEtaStatus = 'estimated' | 'stalled' | 'complete' | 'unknown';

function finiteMetric(value: number | null | undefined) {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
}

export function liveEtaSecs(capture: LiveMetricCapture) {
  return finiteMetric(capture.eta_secs) ?? finiteMetric(capture.progress.eta_secs);
}

export function liveRate(capture: LiveMetricCapture) {
  if (capture.state === 'capturing') return finiteMetric(capture.slots_per_sec);
  if (capture.state === 'packaging') return finiteMetric(capture.progress.blocks_per_sec);
  return null;
}

export function liveRssBytes(capture: LiveMetricCapture) {
  return finiteMetric(capture.rss_bytes) ?? finiteMetric(capture.progress.rss_bytes);
}

export function livePeakRssBytes(capture: LiveMetricCapture) {
  return finiteMetric(capture.peak_rss_bytes) ?? finiteMetric(capture.progress.peak_rss_bytes);
}

export function liveEtaStatus(capture: LiveMetricCapture): LiveEtaStatus {
  if (capture.state === 'complete') return 'complete';
  if (!['capturing', 'packaging'].includes(capture.state)) return 'unknown';
  if (liveEtaSecs(capture) !== null) return 'estimated';
  const rate = liveRate(capture);
  if (rate === 0) return 'stalled';
  return 'unknown';
}
