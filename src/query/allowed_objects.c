#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_objects.h"
#include "pg_diffix/utils.h"

static const char *const g_allowed_casts[] = {
    "i2tod", "i2tof", "i2toi4", "i4toi2", "i4tod", "i4tof", "i8tod", "i8tof", "int48", "int84",
    "ftod", "dtof",
    "int4_numeric", "float4_numeric", "float8_numeric",
    "numeric_float4", "numeric_float8",
    "date_timestamptz",
    /**/
};

typedef struct FunctionByName
{
  const char *name;
  int primary_arg;
} FunctionByName;

typedef struct FunctionByOid
{
  Oid funcid;
  int primary_arg;
} FunctionByOid;

static const FunctionByName g_allowed_builtins[] = {
    /* rounding casts */
    (FunctionByName){.name = "ftoi2", .primary_arg = 0},
    (FunctionByName){.name = "ftoi4", .primary_arg = 0},
    (FunctionByName){.name = "ftoi8", .primary_arg = 0},
    (FunctionByName){.name = "dtoi2", .primary_arg = 0},
    (FunctionByName){.name = "dtoi4", .primary_arg = 0},
    (FunctionByName){.name = "dtoi8", .primary_arg = 0},
    (FunctionByName){.name = "numeric_int2", .primary_arg = 0},
    (FunctionByName){.name = "numeric_int4", .primary_arg = 0},
    (FunctionByName){.name = "numeric_int8", .primary_arg = 0},
    /* substring */
    (FunctionByName){.name = "text_substr", .primary_arg = 0},
    (FunctionByName){.name = "text_substr_no_len", .primary_arg = 0},
    (FunctionByName){.name = "bytea_substr", .primary_arg = 0},
    (FunctionByName){.name = "bytea_substr_no_len", .primary_arg = 0},
    /* numeric generalization */
    (FunctionByName){.name = "dround", .primary_arg = 0},
    (FunctionByName){.name = "numeric_round", .primary_arg = 0},
    (FunctionByName){.name = "dceil", .primary_arg = 0},
    (FunctionByName){.name = "numeric_ceil", .primary_arg = 0},
    (FunctionByName){.name = "dfloor", .primary_arg = 0},
    (FunctionByName){.name = "numeric_floor", .primary_arg = 0},
    /* width_bucket */
    (FunctionByName){.name = "width_bucket_float8", .primary_arg = 0},
    (FunctionByName){.name = "width_bucket_numeric", .primary_arg = 0},
    /* date_trunc */
    (FunctionByName){.name = "timestamptz_trunc", .primary_arg = 1},
    (FunctionByName){.name = "timestamp_trunc", .primary_arg = 1},
    /* extract & date_part*/
    (FunctionByName){.name = "extract_date", .primary_arg = 1},
    (FunctionByName){.name = "extract_timestamp", .primary_arg = 1},
    (FunctionByName){.name = "extract_timestamptz", .primary_arg = 1},
    (FunctionByName){.name = "timestamp_part", .primary_arg = 1},
    (FunctionByName){.name = "timestamptz_part", .primary_arg = 1},
    /**/
};

static const char *const g_substring_builtins[] = {
    "text_substr", "text_substr_no_len", "bytea_substr", "bytea_substr_no_len"
    /**/
};

/* Only those allowed in `anonymized_untrusted` access level. */
static const char *const g_implicit_range_builtins_untrusted[] = {
    "dround", "numeric_round", "dfloor", "numeric_floor",
    /**/
};

/* Some allowed functions don't appear in the builtins catalog, so we must allow them manually by OID. */
#define F_NUMERIC_ROUND_INT 1708
/*
 * `date_part` for `date` is a SQL builtin and doesn't show up in `fmgr_isbuiltin`.
 *  PG 14 has the define, but PG 13 doesn't.
 */
#if PG_MAJORVERSION_NUM < 14
#define F_DATE_PART_TEXT_DATE 1384
#endif

static const FunctionByOid g_allowed_builtins_extra[] = {
    (FunctionByOid){.funcid = F_NUMERIC_ROUND_INT, .primary_arg = 0},
    (FunctionByOid){.funcid = F_DATE_PART_TEXT_DATE, .primary_arg = 1},
    /**/
};

typedef struct AllowedCols
{
  const char *rel_name;             /* Name of the relation */
  Bitmapset *cols;                  /* Indices of the allowed columns of the relation */
  const char *const col_names[100]; /* Names of columns in the relation */
} AllowedCols;

static const char *const g_pg_catalog_allowed_rels[] = {
    "pg_aggregate", "pg_am", "pg_attrdef", "pg_attribute", "pg_auth_members", "pg_authid", "pg_available_extension_versions",
    "pg_available_extensions", "pg_cast", "pg_collation", "pg_constraint", "pg_database", "pg_db_role_setting", "pg_default_acl",
    "pg_depend", "pg_depend", "pg_description", "pg_event_trigger", "pg_extension", "pg_foreign_data_wrapper", "pg_foreign_server",
    "pg_foreign_table", "pg_index", "pg_inherits", "pg_language", "pg_largeobject_metadata", "pg_locks", "pg_namespace",
    "pg_opclass", "pg_operator", "pg_opfamily", "pg_policy", "pg_prepared_statements", "pg_prepared_xacts", "pg_publication",
    "pg_publication_rel", "pg_rewrite", "pg_roles", "pg_seclabel", "pg_seclabels", "pg_sequence", "pg_settings", "pg_shadow",
    "pg_shdepend", "pg_shdescription", "pg_shseclabel", "pg_stat_gssapi", "pg_subscription", "pg_subscription_rel", "pg_tablespace",
    "pg_trigger", "pg_ts_config", "pg_ts_dict", "pg_ts_parser", "pg_ts_template", "pg_type", "pg_user", "pg_tables", "pg_matviews",
    "pg_indexes", "pg_class", "pg_enum", "pg_proc" /* `pg_proc` contains `procost` and `prorows` but both seem to be fully static data. */
};

static AllowedCols g_pg_catalog_allowed_cols[] = {
    {.rel_name = "pg_statistic_ext", .col_names = {"tableoid", "oid", "stxrelid", "stxname", "stxnamespace", "stxstattarget", "stxkeys", "stxkind"}},
    {.rel_name = "pg_stat_activity", .col_names = {"datname", "pid", "usename", "application_name", "client_addr", "backend_start", "xact_start", "query_start", "state_change", "wait_event_type", "wait_event", "state", "query", "backend_type", "client_hostname", "client_port", "backend_start", "backend_xid", "backend_xmin"}},
    /*
     * In `pg_stat_database` there are also `tup_*` and `blks_*` columns, but blocking them doesn't break clients
     * dramatically, so opting to leave them out to err on the safe side.
     */
    {.rel_name = "pg_stat_database", .col_names = {"datname", "xact_commit", "xact_rollback"}},
    /**/
};

static const char *const g_decimal_integer_casts[] = {
    "numeric_int2", "numeric_int4", "numeric_int8", "dtoi2", "dtoi4", "dtoi8", "ftoi2", "ftoi4", "ftoi8",
    /**/
};

static const char *const g_extract_functions[] = {
    "extract_date", "extract_timestamp", "extract_timestamptz", "timestamp_part", "timestamptz_part",
    /**/
};

static const char *const g_integer_extract_fields[] = {
    "minute", "hour", "day", "dow", "isodow", "doy", "week", "month", "quarter", "year", "isoyear", "decade",
    "century", "millennium",
    /**/
};

static void prepare_pg_catalog_allowed(Oid relation_oid, AllowedCols *allowed_cols)
{
  MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
  for (int i = 0; allowed_cols->col_names[i] != NULL; i++)
  {
    int attnum = get_attnum(relation_oid, allowed_cols->col_names[i]) - FirstLowInvalidHeapAttributeNumber;
    allowed_cols->cols = bms_add_member(allowed_cols->cols, attnum);
  }
  MemoryContextSwitchTo(old_context);
}

/* Pointers to OID cache which is populated at runtime. */
static const Oid *const g_implicit_range_udfs[] = {
    &g_oid_cache.round_by_nn,
    &g_oid_cache.round_by_dd,
    &g_oid_cache.ceil_by_nn,
    &g_oid_cache.ceil_by_dd,
    &g_oid_cache.floor_by_nn,
    &g_oid_cache.floor_by_dd,
};

/* Only those allowed in `anonymized_untrusted` access level. */
static const Oid *const g_implicit_range_udfs_untrusted[] = {
    &g_oid_cache.round_by_nn,
    &g_oid_cache.round_by_dd,
    &g_oid_cache.floor_by_nn,
    &g_oid_cache.floor_by_dd,
};

/* Taken from fmgr.c. */
static const FmgrBuiltin *fmgr_isbuiltin(Oid id)
{
  if (id > fmgr_last_builtin_oid)
    return NULL;

  uint16 index = fmgr_builtin_oid_index[id];
  if (index == InvalidOidBuiltinMapping)
    return NULL;

  return &fmgr_builtins[index];
}

static bool is_member_of(const char *s, const char *const array[], int length)
{
  for (int i = 0; i < length; i++)
  {
    if (strcmp(array[i], s) == 0)
      return true;
  }
  return false;
}

static bool is_func_member_of(Oid funcoid, const FunctionByName func_array[], int length)
{
  const FmgrBuiltin *fmgr_builtin = fmgr_isbuiltin(funcoid);
  if (fmgr_builtin != NULL)
  {
    for (int i = 0; i < length; i++)
    {
      if (strcmp(func_array[i].name, fmgr_builtin->funcName) == 0)
        return true;
    }
  }

  return false;
}

static bool is_funcname_member_of(Oid funcoid, const char *const name_array[], int length)
{
  const FmgrBuiltin *fmgr_builtin = fmgr_isbuiltin(funcoid);
  if (fmgr_builtin != NULL)
    return is_member_of(fmgr_builtin->funcName, name_array, length);

  return false;
}

int primary_arg_index(Oid funcoid)
{
  for (int i = 0; i < ARRAY_LENGTH(g_implicit_range_udfs); i++)
  {
    /* We ensured that our UDFs have the primary arg first. */
    if (*g_implicit_range_udfs[i] == funcoid)
      return 0;
  }

  const FmgrBuiltin *fmgr_builtin = fmgr_isbuiltin(funcoid);
  if (fmgr_builtin != NULL)
  {
    for (int i = 0; i < ARRAY_LENGTH(g_allowed_builtins); i++)
    {
      if (strcmp(g_allowed_builtins[i].name, fmgr_builtin->funcName) == 0)
        return g_allowed_builtins[i].primary_arg;
    }
  }

  for (int i = 0; i < ARRAY_LENGTH(g_allowed_builtins_extra); i++)
  {
    if (g_allowed_builtins_extra[i].funcid == funcoid)
      return g_allowed_builtins_extra[i].primary_arg;
  }

  FAILWITH("Cannot identify the primary argument position for funcid %u.", funcoid);
}

bool is_allowed_cast(const FuncExpr *func_expr)
{
  if (is_funcname_member_of(func_expr->funcid, g_allowed_casts, ARRAY_LENGTH(g_allowed_casts)))
  {
    return true;
  }
  else if (is_funcname_member_of(func_expr->funcid, g_decimal_integer_casts, ARRAY_LENGTH(g_decimal_integer_casts)))
  {
    /*
     * Special case, where a `numeric_int4` cast is called on variants of `extract` which return
     * integer numbers, e.g. `cast(extract(minute from ...) as integer)`.
     */
    Node *cast_arg = linitial(func_expr->args);
    if (IsA(cast_arg, FuncExpr))
    {
      FuncExpr *cast_arg_expr = (FuncExpr *)cast_arg;
      if (is_funcname_member_of(cast_arg_expr->funcid, g_extract_functions, ARRAY_LENGTH(g_extract_functions)) ||
          cast_arg_expr->funcid == F_DATE_PART_TEXT_DATE)
      {
        Node *extract_field = linitial(cast_arg_expr->args);
        if (IsA(extract_field, Const))
        {
          Const *extract_field_const = (Const *)extract_field;
          Assert(extract_field_const->consttype == TEXTOID);
          const char *field = TextDatumGetCString(extract_field_const->constvalue);

          return is_member_of(field, g_integer_extract_fields, ARRAY_LENGTH(g_integer_extract_fields));
        }
      }
    }
  }
  return false;
}

bool is_implicit_range_udf_untrusted(Oid funcoid)
{
  for (int i = 0; i < ARRAY_LENGTH(g_implicit_range_udfs_untrusted); i++)
  {
    if (*g_implicit_range_udfs_untrusted[i] == funcoid)
      return true;
  }
  return false;
}

bool is_allowed_function(Oid funcoid)
{
  for (int i = 0; i < ARRAY_LENGTH(g_implicit_range_udfs); i++)
  {
    if (*g_implicit_range_udfs[i] == funcoid)
      return true;
  }

  if (is_func_member_of(funcoid, g_allowed_builtins, ARRAY_LENGTH(g_allowed_builtins)))
    return true;

  for (int i = 0; i < ARRAY_LENGTH(g_allowed_builtins_extra); i++)
  {
    if (g_allowed_builtins_extra[i].funcid == funcoid)
      return true;
  }

  DEBUG_LOG("Rejecting usage of function %u.", funcoid);
  return false;
}

bool is_substring_builtin(Oid funcoid)
{
  return is_funcname_member_of(funcoid, g_substring_builtins, ARRAY_LENGTH(g_substring_builtins));
}

bool is_implicit_range_builtin_untrusted(Oid funcoid)
{
  return is_funcname_member_of(funcoid, g_implicit_range_builtins_untrusted, ARRAY_LENGTH(g_implicit_range_builtins_untrusted));
}

bool is_allowed_pg_catalog_rte(Oid relation_oid, const Bitmapset *selected_cols)
{
  /* First handle `SELECT count(*) FROM pg_catalog.x`. */
  if (selected_cols == NULL)
    return true;

  char *rel_name = get_rel_name(relation_oid);

  /* Then check if the entire relation is allowed. */
  if (is_member_of(rel_name, g_pg_catalog_allowed_rels, ARRAY_LENGTH(g_pg_catalog_allowed_rels)))
  {
    pfree(rel_name);
    return true;
  }

  /* Otherwise specific selected columns must be checked against the allow-list. */
  bool allowed = false;
  for (int i = 0; i < ARRAY_LENGTH(g_pg_catalog_allowed_cols); i++)
  {
    if (strcmp(g_pg_catalog_allowed_cols[i].rel_name, rel_name) != 0)
      continue;

    if (g_pg_catalog_allowed_cols[i].cols == NULL)
      prepare_pg_catalog_allowed(relation_oid, &g_pg_catalog_allowed_cols[i]);

    allowed = bms_is_subset(selected_cols, g_pg_catalog_allowed_cols[i].cols);
    break;
  }

  pfree(rel_name);
  return allowed;
}
