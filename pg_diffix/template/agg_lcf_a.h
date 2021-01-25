/*
 * Accepts AID type, generates standard diffix_lcf UDFs.
 * Does not clean up input macros.
 *
 * Input macros:
 *
 * AGG_AID_LABEL
 * AGG_AID_TYPE
 * AGG_AID_FMT
 * AGG_AID_EQUAL(a, b)
 * AGG_AID_HASH(aid)
 * AGG_AID_GETARG(a)
 * AGG_INIT_AID_HASH(state, aid, aid_hash) - optional
 * AGG_INIT_ENTRY(state, entry)            - optional
 *
 * Output:
 * diffix_<AID_LABEL>_lcf_<function>
 */

#ifndef PG_DIFFIX_AGG_LCF_A_H
#define PG_DIFFIX_AGG_LCF_A_H

#include "postgres.h"

#include <math.h>
#include <inttypes.h>

#include "fmgr.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"

#include "pg_diffix/random.h"

typedef struct LcfResult
{
  uint64 random_seed;
  int noisy_threshold;
  bool passes_lcf;
} LcfResult;

#endif /* PG_DIFFIX_AGG_LCF_A_H */

#define AGG_CONCAT_HELPER(a, b) CppConcat(a, b)
#define AGG_MAKE_NAME(name) \
  AGG_CONCAT_HELPER(AGG_CONCAT_HELPER(diffix_, AGG_AID_LABEL), AGG_CONCAT_HELPER(_, name))

/* Contribution state */
#define CS_PREFIX AGG_MAKE_NAME(lcf)
#define CS_AID_TYPE AGG_AID_TYPE
#define CS_AID_EQUAL(a, b) AGG_AID_EQUAL(a, b)
#define CS_AID_HASH(aid) AGG_AID_HASH(aid)
#define CS_SEED_INITIAL 0xe97c4b2d
#define CS_SCOPE static inline
#define CS_DECLARE
#define CS_DEFINE
#ifdef AGG_INIT_AID_HASH
#define CS_INIT_AID_HASH(state, aid, aid_hash) AGG_INIT_AID_HASH(state, aid, aid_hash)
#endif
#ifdef AGG_INIT_ENTRY
#define CS_INIT_ENTRY(state, entry) AGG_INIT_ENTRY(state, entry)
#endif
#include "pg_diffix/template/contribution_state.h"

/* Exported UDFs */
#define AGG_TRANSFN AGG_MAKE_NAME(lcf_transfn)
#define AGG_FINALFN AGG_MAKE_NAME(lcf_finalfn)
#define AGG_EXPLAIN_FINALFN AGG_MAKE_NAME(lcf_explain_finalfn)

/* Functions from contribution_state.h */
#define AGG_CONTRIBUTION_STATE AGG_MAKE_NAME(lcf_ContributionState)
#define AGG_GET_STATE AGG_MAKE_NAME(lcf_state_getarg)
#define AGG_UPDATE_AID AGG_MAKE_NAME(lcf_state_update_aid)

/* Helpers */
#define ARGS fcinfo
#define AGG_CALCULATE_FINAL AGG_MAKE_NAME(lcf_calculcate_final)
static inline LcfResult AGG_CALCULATE_FINAL(AGG_CONTRIBUTION_STATE *state);

PG_FUNCTION_INFO_V1(AGG_TRANSFN);
PG_FUNCTION_INFO_V1(AGG_FINALFN);
PG_FUNCTION_INFO_V1(AGG_EXPLAIN_FINALFN);

Datum AGG_TRANSFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS, 0);

  if (!PG_ARGISNULL(1))
  {
    AGG_UPDATE_AID(state, AGG_AID_GETARG(state, 1));
  }

  PG_RETURN_POINTER(state);
}

Datum AGG_FINALFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS, 0);
  LcfResult result = AGG_CALCULATE_FINAL(state);
  PG_RETURN_BOOL(result.passes_lcf);
}

Datum AGG_EXPLAIN_FINALFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS, 0);
  StringInfoData string;
  LcfResult result = AGG_CALCULATE_FINAL(state);

  /*
   * Split seed into 4 shorts, because only the first 3 will be effective in the rng.
   */
  uint16 *random_seed = (uint16 *)(&result.random_seed);

  initStringInfo(&string);

  appendStringInfo(&string, "uniq=%" PRIu64, state->distinct_aids);

  /* Print only effective part of the seed. */
  appendStringInfo(&string,
                   ", seed=%04" PRIx16 "%04" PRIx16 "%04" PRIx16,
                   random_seed[0], random_seed[1], random_seed[2]);

  appendStringInfo(&string, "\nthresh=%i, pass=%s",
                   result.noisy_threshold,
                   result.passes_lcf ? "true" : "false");

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static inline LcfResult AGG_CALCULATE_FINAL(AGG_CONTRIBUTION_STATE *state)
{
  LcfResult result;
  uint64 seed = make_seed(state->aid_seed);

  result.random_seed = seed;

  result.noisy_threshold = next_uniform_int(
      &seed,
      Config.low_count_threshold_min,
      Config.low_count_threshold_max + 1);

  result.passes_lcf = state->distinct_aids >= result.noisy_threshold;

  return result;
}

/* Clean up only locally used macros. */
#undef AGG_CONCAT_HELPER
#undef AGG_MAKE_NAME

#undef AGG_TRANSFN
#undef AGG_FINALFN
#undef AGG_EXPLAIN_FINALFN

#undef AGG_CONTRIBUTION_STATE
#undef AGG_GET_STATE
#undef AGG_UPDATE_AID

#undef ARGS
#undef AGG_CALCULATE_FINAL
