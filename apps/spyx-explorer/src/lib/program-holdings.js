/**
 * Merge parser PDA attribution and runtime Account.owner evidence by program ID.
 * Each owner contributes to a program total once, even when both evidence sources
 * link that owner to the same program.
 *
 * Program names are display data. They cannot create, remove, or merge rows.
 *
 * @param {Array<{
 *   owner: string,
 *   token_account_count: number,
 *   public_balance: { raw_amount: string },
 *   public_activity_volume?: { raw_amount: string },
 *   pda_program_id?: string | null,
 *   pda_program_name?: string | null
 * }>} parserHolders
 * @param {Array<{
 *   owner: string,
 *   token_account_count: number,
 *   public_balance: { raw_amount: string },
 *   public_activity_volume?: { raw_amount: string },
 *   supplemental_program_attribution?: {
 *     account_exists: boolean,
 *     runtime_owner_program_id: string | null,
 *     runtime_owner_program_name: string | null
 *   }
 * }>} runtimeHolders
 * @returns {Array<{
 *   program_id: string,
 *   program_name: string | null,
 *   holder_count: number,
 *   parser_holder_count: number,
 *   runtime_holder_count: number,
 *   overlap_holder_count: number,
 *   token_account_count: number,
 *   public_balance_raw_amount: string,
 *   public_activity_raw_amount: string | null
 * }>}
 */
export function buildProgramHoldings(parserHolders, runtimeHolders) {
  /** @type {Map<string, {
   *   program_id: string,
   *   program_name: string | null,
   *   holders: Map<string, {
   *     token_account_count: number,
   *     public_balance_raw_amount: string,
   *     public_activity_raw_amount: string | null,
   *     parser: boolean,
   *     runtime: boolean
   *   }>
   * }>} */
  const programs = new Map();

  for (const holder of parserHolders) {
    addHolder(
      programs,
      holder.pda_program_id,
      holder.pda_program_name,
      holder,
      'parser'
    );
  }

  for (const holder of runtimeHolders) {
    const attribution = holder.supplemental_program_attribution;
    if (!attribution?.account_exists) continue;
    addHolder(
      programs,
      attribution.runtime_owner_program_id,
      attribution.runtime_owner_program_name,
      holder,
      'runtime'
    );
  }

  return [...programs.values()].map((program) => {
    let parserHolderCount = 0;
    let runtimeHolderCount = 0;
    let overlapHolderCount = 0;
    let tokenAccountCount = 0;
    let publicBalance = 0n;
    let publicActivity = 0n;
    let hasCompleteActivity = true;

    for (const holder of program.holders.values()) {
      if (holder.parser) parserHolderCount += 1;
      if (holder.runtime) runtimeHolderCount += 1;
      if (holder.parser && holder.runtime) overlapHolderCount += 1;
      tokenAccountCount += holder.token_account_count;
      publicBalance += BigInt(holder.public_balance_raw_amount);
      if (holder.public_activity_raw_amount === null) {
        hasCompleteActivity = false;
      } else {
        publicActivity += BigInt(holder.public_activity_raw_amount);
      }
    }

    return {
      program_id: program.program_id,
      program_name: program.program_name,
      holder_count: program.holders.size,
      parser_holder_count: parserHolderCount,
      runtime_holder_count: runtimeHolderCount,
      overlap_holder_count: overlapHolderCount,
      token_account_count: tokenAccountCount,
      public_balance_raw_amount: publicBalance.toString(),
      public_activity_raw_amount: hasCompleteActivity ? publicActivity.toString() : null
    };
  });
}

/**
 * Group the complete PDA and off-curve custody cohort by exact program ID.
 * Parser attribution has priority over later runtime-owner evidence. Rows with
 * no program evidence remain visible in one unlinked group.
 *
 * Program names are display data. They cannot create, remove, merge, or order
 * groups.
 *
 * @param {Array<{
 *   owner: string,
 *   token_account_count: number,
 *   public_balance: { raw_amount: string },
 *   public_activity_volume?: { raw_amount: string },
 *   pda_program_id?: string | null,
 *   pda_program_name?: string | null,
 *   supplemental_program_attribution?: {
 *     account_exists: boolean,
 *     runtime_owner_program_id: string | null,
 *     runtime_owner_program_name: string | null
 *   }
 * }>} holders
 * @returns {Array<{
 *   program_id: string | null,
 *   program_name: string | null,
 *   owner_ids: string[],
 *   holder_count: number,
 *   token_account_count: number,
 *   public_balance_raw_amount: string,
 *   public_activity_raw_amount: string | null
 * }>}
 */
export function buildCustodyProgramHoldings(holders) {
  /** @type {Map<string, {
   *   program_id: string | null,
   *   program_name: string | null,
   *   holders: Map<string, {
   *     token_account_count: number,
   *     public_balance_raw_amount: string,
   *     public_activity_raw_amount: string | null
   *   }>
   * }>} */
  const groups = new Map();

  for (const holder of holders) {
    const program = custodyProgramEvidence(holder);
    const key = program?.id ?? '__program_not_linked__';
    const group = groups.get(key) ?? {
      program_id: program?.id ?? null,
      program_name: program?.name ?? null,
      holders: new Map()
    };
    if (group.program_name === null && program?.name) group.program_name = program.name;
    if (!group.holders.has(holder.owner)) {
      group.holders.set(holder.owner, {
        token_account_count: holder.token_account_count,
        public_balance_raw_amount: holder.public_balance.raw_amount,
        public_activity_raw_amount: holder.public_activity_volume?.raw_amount ?? null
      });
    }
    groups.set(key, group);
  }

  return [...groups.values()]
    .sort((left, right) => compareProgramIds(left.program_id, right.program_id))
    .map((group) => {
      let tokenAccountCount = 0;
      let publicBalance = 0n;
      let publicActivity = 0n;
      let hasCompleteActivity = true;

      for (const holder of group.holders.values()) {
        tokenAccountCount += holder.token_account_count;
        publicBalance += BigInt(holder.public_balance_raw_amount);
        if (holder.public_activity_raw_amount === null) {
          hasCompleteActivity = false;
        } else {
          publicActivity += BigInt(holder.public_activity_raw_amount);
        }
      }

      return {
        program_id: group.program_id,
        program_name: group.program_name,
        owner_ids: [...group.holders.keys()].sort((left, right) => left.localeCompare(right)),
        holder_count: group.holders.size,
        token_account_count: tokenAccountCount,
        public_balance_raw_amount: publicBalance.toString(),
        public_activity_raw_amount: hasCompleteActivity ? publicActivity.toString() : null
      };
    });
}

/**
 * @param {{
 *   pda_program_id?: string | null,
 *   pda_program_name?: string | null,
 *   supplemental_program_attribution?: {
 *     account_exists: boolean,
 *     runtime_owner_program_id: string | null,
 *     runtime_owner_program_name: string | null
 *   }
 * }} holder
 * @returns {{ id: string, name: string | null } | null}
 */
function custodyProgramEvidence(holder) {
  if (typeof holder.pda_program_id === 'string' && holder.pda_program_id) {
    return {
      id: holder.pda_program_id,
      name: normalizedProgramName(holder.pda_program_name)
    };
  }
  const runtime = holder.supplemental_program_attribution;
  if (
    runtime?.account_exists === true &&
    typeof runtime.runtime_owner_program_id === 'string' &&
    runtime.runtime_owner_program_id
  ) {
    return {
      id: runtime.runtime_owner_program_id,
      name: normalizedProgramName(runtime.runtime_owner_program_name)
    };
  }
  return null;
}

/** @param {string | null | undefined} value */
function normalizedProgramName(value) {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

/** @param {string | null} left @param {string | null} right */
function compareProgramIds(left, right) {
  if (left === null && right !== null) return 1;
  if (left !== null && right === null) return -1;
  return (left ?? '').localeCompare(right ?? '');
}

/**
 * @param {Map<string, any>} programs
 * @param {string | null | undefined} programId
 * @param {string | null | undefined} programName
 * @param {{
 *   owner: string,
 *   token_account_count: number,
 *   public_balance: { raw_amount: string },
 *   public_activity_volume?: { raw_amount: string }
 * }} holder
 * @param {'parser' | 'runtime'} evidence
 */
function addHolder(programs, programId, programName, holder, evidence) {
  if (typeof programId !== 'string' || !programId) return;
  const normalizedName =
    typeof programName === 'string' && programName.trim() ? programName.trim() : null;
  const program = programs.get(programId) ?? {
    program_id: programId,
    program_name: normalizedName,
    holders: new Map()
  };
  if (program.program_name === null && normalizedName !== null) {
    program.program_name = normalizedName;
  }

  const previous = program.holders.get(holder.owner);
  if (previous) {
    previous[evidence] = true;
  } else {
    program.holders.set(holder.owner, {
      token_account_count: holder.token_account_count,
      public_balance_raw_amount: holder.public_balance.raw_amount,
      public_activity_raw_amount: holder.public_activity_volume?.raw_amount ?? null,
      parser: evidence === 'parser',
      runtime: evidence === 'runtime'
    });
  }
  programs.set(programId, program);
}
