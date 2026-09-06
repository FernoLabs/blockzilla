import { asset } from '$app/paths';
import type { PageLoad } from './$types';
import type { ProgramReport } from '$lib/types';

export const load: PageLoad = async ({ fetch }) => {
  const response = await fetch(asset('/data/spyx-programs.json'));
  if (!response.ok) {
    throw new Error(`SPYx program summary request failed with HTTP ${response.status}`);
  }
  return { programReport: (await response.json()) as ProgramReport };
};
