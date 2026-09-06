export const UNLABELED_PROGRAM = 'Unlabeled program';

/**
 * Return a display name without using it as program identity.
 *
 * @param {string | null | undefined} name
 * @returns {string}
 */
export function programDisplayName(name) {
  return typeof name === 'string' && name.trim() ? name.trim() : UNLABELED_PROGRAM;
}

/**
 * Keep the complete program address visible in a filter option.
 *
 * @param {string} programId
 * @param {string | null | undefined} name
 * @returns {string}
 */
export function programOptionLabel(programId, name) {
  return `${programDisplayName(name)} — ${programId}`;
}

/**
 * Build program choices from program IDs. Labels can add text, but cannot
 * add, remove, or reorder a program.
 *
 * @param {Array<{ program_id: string, program_name?: string | null }>} programRows
 * @param {Array<{ pda_program_id?: string | null, pda_program_name?: string | null }>} holderRows
 * @returns {Array<{ id: string, name: string | null, label: string }>}
 */
export function buildProgramOptions(programRows, holderRows = []) {
  /** @type {Map<string, string | null>} */
  const namesById = new Map();

  for (const row of programRows) {
    addProgram(namesById, row.program_id, row.program_name);
  }
  for (const row of holderRows) {
    addProgram(namesById, row.pda_program_id, row.pda_program_name);
  }

  return [...namesById.entries()]
    .sort(([leftId], [rightId]) => leftId.localeCompare(rightId))
    .map(([id, name]) => ({ id, name, label: programOptionLabel(id, name) }));
}

/**
 * @param {Map<string, string | null>} namesById
 * @param {string | null | undefined} programId
 * @param {string | null | undefined} name
 */
function addProgram(namesById, programId, name) {
  if (typeof programId !== 'string' || !programId) return;
  const normalizedName = typeof name === 'string' && name.trim() ? name.trim() : null;
  const previous = namesById.get(programId);
  if (!namesById.has(programId) || (previous === null && normalizedName !== null)) {
    namesById.set(programId, normalizedName);
  }
}
