#include "postgres.h"

#include "catalog/pg_inherits.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"

/* Security labels type definitions */
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "commands/seclabel.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"

static const char *PROVIDER_TAG = "pg_diffix";
static const char *PERSONAL_LABEL_PREFIX = "personal:";

static void object_relabel(const ObjectAddress *object, const char *seclabel);

void auth_init(void)
{
  register_label_provider(PROVIDER_TAG, object_relabel);
}

static inline bool is_personal_label(const char *seclabel)
{
  return strncasecmp(seclabel, PERSONAL_LABEL_PREFIX, strlen(PERSONAL_LABEL_PREFIX)) == 0;
}

static inline bool is_public_label(const char *seclabel)
{
  return strcasecmp(seclabel, "public") == 0;
}

static inline bool is_aid_label(const char *seclabel)
{
  return strcasecmp(seclabel, "aid") == 0;
}

static inline bool is_publish_trusted_label(const char *seclabel)
{
  return strcasecmp(seclabel, "publish_trusted") == 0;
}

static inline bool is_publish_untrusted_label(const char *seclabel)
{
  return strcasecmp(seclabel, "publish_untrusted") == 0;
}

static inline bool is_direct_label(const char *seclabel)
{
  return strcasecmp(seclabel, "direct") == 0;
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
  else if (is_publish_trusted_label(seclabel))
    return ACCESS_PUBLISH_TRUSTED;
  else if (is_publish_untrusted_label(seclabel))
    return ACCESS_PUBLISH_UNTRUSTED;
  else
    FAIL_ON_INVALID_LABEL(seclabel);
}

AccessLevel get_session_access_level(void)
{
  AccessLevel user_level = get_user_access_level();
  if (is_higher_access_level(g_config.session_access_level, user_level))
  {
    /* We always limit the current access level to the maximum allowed user access level. */
    g_config.session_access_level = user_level;
  }
  return (AccessLevel)g_config.session_access_level;
}

bool is_personal_relation(Oid relation_oid)
{
  ObjectAddress relation_object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = 0};
  const char *seclabel = GetSecurityLabel(&relation_object, PROVIDER_TAG);

  if (seclabel == NULL)
    return false;
  else if (is_personal_label(seclabel))
    return true;
  else if (is_public_label(seclabel))
    return false;
  else
    FAIL_ON_INVALID_LABEL(seclabel);
}

static const char *seclabel_to_salt(const char *seclabel)
{
  /* Verify that the security label is personal and the salt is non-empty. */
  if (!is_personal_label(seclabel) || seclabel[strlen(PERSONAL_LABEL_PREFIX)] == 0)
    FAILWITH_CODE(ERRCODE_INVALID_NAME, "Table has invalid anonymization label.");
  return seclabel + strlen(PERSONAL_LABEL_PREFIX);
}

const char *get_salt_for_relation(Oid relation_oid)
{
  ObjectAddress relation_object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = 0};
  return seclabel_to_salt(GetSecurityLabel(&relation_object, PROVIDER_TAG));
}

bool is_aid_column(Oid relation_oid, AttrNumber attnum)
{
  ObjectAddress object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = attnum};
  const char *seclabel = GetSecurityLabel(&object, PROVIDER_TAG);

  if (seclabel == NULL)
    return false;
  else if (is_aid_label(seclabel))
    return true;
  else
    FAIL_ON_INVALID_LABEL(seclabel);
}

#define FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object)                   \
  FAILWITH_CODE(                                                        \
      ERRCODE_FEATURE_NOT_SUPPORTED,                                    \
      "Anonymization label `%s` not supported on objects of type `%s`", \
      seclabel, getObjectTypeDescription(object))

static void verify_pg_features(Oid relation_id)
{
  if (has_subclass(relation_id) || has_superclass(relation_id))
    FAILWITH("Anonymization over tables using inheritance is not supported.");
}

static void object_relabel(const ObjectAddress *object, const char *seclabel)
{
  if (!superuser())
    FAILWITH_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE, "only a superuser can set anonymization labels");

  if (seclabel == NULL)
    return;

  if (is_personal_label(seclabel) || is_public_label(seclabel))
  {
    if (is_personal_label(seclabel))
    {
      verify_pg_features(object->objectId);
      seclabel_to_salt(seclabel); /* Verify salt exists. */
    }

    if (object->classId == RelationRelationId && object->objectSubId == 0)
      return;

    FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object);
  }
  else if (is_aid_label(seclabel))
  {
    if (object->classId == RelationRelationId && object->objectSubId != 0)
      return;
    FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object);
  }
  else if (is_publish_trusted_label(seclabel) || is_publish_untrusted_label(seclabel) || is_direct_label(seclabel))
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
  case ACCESS_PUBLISH_TRUSTED:
    return "publish_trusted";
  case ACCESS_PUBLISH_UNTRUSTED:
    return "publish_untrusted";
  default:
    return NULL;
  }
}

PG_FUNCTION_INFO_V1(access_level);

Datum access_level(PG_FUNCTION_ARGS)
{
  AccessLevel level = get_session_access_level();
  char *str = level_to_string(level);
  if (str != NULL)
    PG_RETURN_TEXT_P(cstring_to_text(str));
  else
    PG_RETURN_NULL();
}
