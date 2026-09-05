import { asset } from '$app/paths';
import type { LayoutLoad } from './$types';
import type { SpyxReport } from '$lib/types';

export const ssr = false;
export const prerender = true;

export const load: LayoutLoad = async ({ fetch }) => {
  const response = await fetch(asset('/data/spyx-summary.json'));
  if (!response.ok) {
    throw new Error(`SPYx summary request failed with HTTP ${response.status}`);
  }

  return { report: (await response.json()) as SpyxReport };
};
