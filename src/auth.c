#include "postgres.h"

#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* Security labels type definitions */
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "commands/seclabel.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"

static const char *PROVIDER_TAG = "pg_diffix";

static void object_relabel(const ObjectAddress *object, const char *seclabel);

void auth_init(void)
{
  register_label_provider(PROVIDER_TAG, object_relabel);
}

static inline bool is_personal_label(const char *seclabel)
{
  return pg_strcasecmp(seclabel, "personal") == 0;
}

static inline bool is_public_label(const char *seclabel)
{
  return pg_strcasecmp(seclabel, "public") == 0;
}

static inline bool is_aid_label(const char *seclabel)
{
  return pg_strcasecmp(seclabel, "aid") == 0;
}

static inline bool is_not_filterable_label(const char *seclabel)
{
  return pg_strcasecmp(seclabel, "not_filterable") == 0;
}

static inline bool is_anonymized_trusted_label(const char *seclabel)
{
  return pg_strcasecmp(seclabel, "anonymized_trusted") == 0;
}

static inline bool is_anonymized_untrusted_label(const char *seclabel)
{
  return pg_strcasecmp(seclabel, "anonymized_untrusted") == 0;
}

static inline bool is_direct_label(const char *seclabel)
{
  return pg_strcasecmp(seclabel, "direct") == 0;
}

#define FAIL_ON_INVALID_LABEL(seclabel) \
  FAILWITH_CODE(ERRCODE_INVALID_NAME, "'%s' is not a valid anonymization label", seclabel);

AccessLevel get_user_access_level(void)
{
  Oid user_id = GetSessionUserId();
  ObjectAddress user_object = {.classId = AuthIdRelationId, .objectId = user_id, .objectSubId = 0};
  const char *seclabel = GetSecurityLabel(&user_object, PROVIDER_TAG);

  if (seclabel == NULL)
    return (AccessLevel)g_config.default_access_level;
  else if (is_direct_label(seclabel))
    return ACCESS_DIRECT;
  else if (is_anonymized_trusted_label(seclabel))
    return ACCESS_ANONYMIZED_TRUSTED;
  else if (is_anonymized_untrusted_label(seclabel))
    return ACCESS_ANONYMIZED_UNTRUSTED;
  else
    FAIL_ON_INVALID_LABEL(seclabel);
}

AccessLevel get_session_access_level(void)
{
  /* If the extension wasn't activated, force the current session into direct access mode. */
  if (!is_pg_diffix_active())
    return ACCESS_DIRECT;

  AccessLevel user_level = get_user_access_level();
  if (is_higher_access_level(g_config.session_access_level, user_level))
  {
    /* We always limit the current access level to the maximum allowed user access level. */
    g_config.session_access_level = user_level;
  }
  return (AccessLevel)g_config.session_access_level;
}

static bool is_metadata_relation(Oid relation_oid)
{
  Oid namespace_oid = get_rel_namespace(relation_oid);

  if (namespace_oid == PG_CATALOG_NAMESPACE)
    return true; /* PG_CATALOG relations are checked in `ExecutorCheckPerms` hook. */

  char *namespace_name = get_namespace_name(namespace_oid);
  bool result = strcmp(namespace_name, "information_schema") == 0; /* INFORMATION_SCHEMA relations are safe to query. */
  pfree(namespace_name);

  return result;
}

bool is_personal_relation(Oid relation_oid)
{
  ObjectAddress relation_object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = 0};
  const char *seclabel = GetSecurityLabel(&relation_object, PROVIDER_TAG);

  if (seclabel == NULL)
    if (g_config.treat_unmarked_tables_as_public || is_metadata_relation(relation_oid))
      return false;
    else
      FAILWITH_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE,
                    "Tables without an anonymization label can't be accessed in anonymized mode.");
  else if (is_personal_label(seclabel))
    return true;
  else if (is_public_label(seclabel))
    return false;
  else
    FAIL_ON_INVALID_LABEL(seclabel);
}

bool is_aid_column(Oid relation_oid, AttrNumber attnum)
{
  ObjectAddress object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = attnum};
  const char *seclabel = GetSecurityLabel(&object, PROVIDER_TAG);
  return seclabel != NULL && is_aid_label(seclabel);
}

bool is_not_filterable_column(Oid relation_oid, AttrNumber attnum)
{
  ObjectAddress object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = attnum};
  const char *seclabel = GetSecurityLabel(&object, PROVIDER_TAG);
  return seclabel != NULL && is_not_filterable_label(seclabel);
}

#define FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object)                   \
  FAILWITH_CODE(                                                        \
      ERRCODE_FEATURE_NOT_SUPPORTED,                                    \
      "Anonymization label `%s` not supported on objects of type `%s`", \
      seclabel, getObjectTypeDescription(object))

static void verify_pg_features(Oid relation_oid)
{
  if (has_subclass(relation_oid) || has_superclass(relation_oid))
    FAILWITH("Anonymization over tables using inheritance is not supported.");
}

static bool is_aid_type_supported(Oid relation_oid, AttrNumber attnum)
{
  switch (get_atttype(relation_oid, attnum))
  {
  case INT4OID:
  case INT8OID:
  case TEXTOID:
  case VARCHAROID:
    return true;
  default:
    return false;
  }
}

static void object_relabel(const ObjectAddress *object, const char *seclabel)
{
  if (!superuser())
    FAILWITH_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE, "Only a superuser can set anonymization labels");

  if (seclabel == NULL)
    return;

  if (is_personal_label(seclabel) || is_public_label(seclabel))
  {
    if (is_personal_label(seclabel))
      verify_pg_features(object->objectId);

    if (object->classId == RelationRelationId && object->objectSubId == 0)
      return;

    FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object);
  }
  else if (is_aid_label(seclabel))
  {
    if (object->classId == RelationRelationId && object->objectSubId != 0)
    {
      if (!is_aid_type_supported(object->objectId, object->objectSubId))
        FAILWITH_CODE(ERRCODE_FEATURE_NOT_SUPPORTED,
                      "AID label can not be set on target column because the type is unsupported");
      return;
    }

    FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object);
  }
  else if (is_not_filterable_label(seclabel))
  {
    if (object->classId == RelationRelationId && object->objectSubId != 0)
      return;

    FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object);
  }
  else if (is_anonymized_trusted_label(seclabel) || is_anonymized_untrusted_label(seclabel) || is_direct_label(seclabel))
  {
    if (object->classId == AuthIdRelationId)
    {
      if (superuser_arg(object->objectId))
        FAILWITH_CODE(ERRCODE_FEATURE_NOT_SUPPORTED, "Anonymization labels can not be set on superusers");
      return;
    }

    FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object);
  }

  FAIL_ON_INVALID_LABEL(seclabel);
}

static char *level_to_string(AccessLevel level)
{
  switch (level)
  {
  case ACCESS_DIRECT:
    return "direct";
  case ACCESS_ANONYMIZED_TRUSTED:
    return "anonymized_trusted";
  case ACCESS_ANONYMIZED_UNTRUSTED:
    return "anonymized_untrusted";
  default:
    return NULL;
  }
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(access_level);

Datum access_level(PG_FUNCTION_ARGS)
{
  AccessLevel level = get_session_access_level();
  char *str = level_to_string(level);
  if (str != NULL)
    PG_RETURN_TEXT_P(cstring_to_text(str));
  else
    PG_RETURN_NULL();
}
