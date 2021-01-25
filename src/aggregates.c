#include "postgres.h"
#include "fmgr.h"
#include "common/hashfn.h"
#include "utils/builtins.h"

/* ----------------------------------------------------------------
 * AID: int4
 * ----------------------------------------------------------------
 */

#define AGG_AID_LABEL int4
#define AGG_AID_FMT "%i"
#define AGG_AID_TYPE int32
#define AGG_AID_EQUAL(a, b) (a == b)
#define AGG_AID_HASH(aid) hash_bytes_uint32(aid)
#define AGG_AID_GETARG(state, arg) PG_GETARG_INT32(arg)

#include "pg_diffix/template/agg_count_a.h"

#undef AGG_AID_LABEL
#undef AGG_AID_FMT
#undef AGG_AID_TYPE
#undef AGG_AID_EQUAL
#undef AGG_AID_HASH
#undef AGG_AID_GETARG

/* ----------------------------------------------------------------
 * AID: text
 * ----------------------------------------------------------------
 */

typedef char *cstr;
#define AGG_AID_LABEL text
#define AGG_AID_FMT "%s"
#define AGG_AID_TYPE cstr
#define AGG_AID_EQUAL(a, b) (strcmp(a, b) == 0)
#define AGG_AID_HASH(aid) hash_bytes((unsigned char *)aid, strlen(aid))
#define AGG_AID_GETARG(state, arg) text_to_cstring(PG_GETARG_TEXT_PP(arg))

/*
 * Notice that we deliberately do not wrap this in a block.
 * We need the cached_strlen variable in scope for AGG_INIT_ENTRY.
 */
#define AGG_INIT_AID_HASH(state, aid, aid_hash) \
  size_t cached_strlen = strlen(aid);           \
  aid_hash = hash_bytes((unsigned char *)aid, cached_strlen)

/*
 * We initially palloc the cstring in a local function context.
 * If we know that we have to hold onto it, we move it to state->context.
 */
#define AGG_INIT_ENTRY(state, entry)                                        \
  do                                                                        \
  {                                                                         \
    char *aid_copy = MemoryContextAlloc(state->context, cached_strlen + 1); \
    strcpy(aid_copy, entry->aid);                                           \
    entry->aid = aid_copy;                                                  \
  } while (0)

#include "pg_diffix/template/agg_count_a.h"

#undef AGG_AID_LABEL
#undef AGG_AID_FMT
#undef AGG_AID_TYPE
#undef AGG_AID_EQUAL
#undef AGG_AID_HASH
#undef AGG_AID_GETARG
#undef AGG_INIT_AID_HASH
#undef AGG_INIT_ENTRY
