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

static void object_relabel(const ObjectAddress *object, const char *seclabel);

void auth_init(void)
{
  register_label_provider(PROVIDER_TAG, object_relabel);
}

/*
 * Returns the nth token from a seclabel formatted like `token_0:token_1:token_2:...:token_n:...`. The input seclabel is
 * not modified. The returned `token` is palloc'ed. If there is no nth token, NULL is returned.
 */
static inline char *seclabel_token(const char *str, int n)
{
  if (str == NULL)
    return NULL;

  char *token = palloc(sizeof(char) * (strlen(str) + 1));
  strcpy(token, str);
  char *saveptr;
  token = __strtok_r(token, ":", &saveptr);
  for (int i = 0; i < n; i++)
  {
    token = __strtok_r(NULL, ":", &saveptr);
  }
  return token;
}

static inline bool is_sensitive_label(const char *seclabel)
{
  char *label = seclabel_token(seclabel, 0);
  bool result = strcasecmp(label, "sensitive") == 0;
  pfree(label);
  return result;
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

#define FAIL_ON_INVALID_LABEL(seclabel)                                                                                \
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

bool is_sensitive_relation(Oid relation_oid)
{
  ObjectAddress relation_object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = 0};
  const char *seclabel = GetSecurityLabel(&relation_object, PROVIDER_TAG);

  if (seclabel == NULL)
    return false;
  else if (is_sensitive_label(seclabel))
    return true;
  else if (is_public_label(seclabel))
    return false;
  else
    FAIL_ON_INVALID_LABEL(seclabel);
}

static char *get_salt_from_seclabel(const char *seclabel)
{
  char *salt = seclabel_token(seclabel, 1);
  if (salt == NULL)
    return NULL;

  /* Truncate the salt to 32-bytes (hex encoded). */
  if (strlen(salt) > 64)
    salt[64] = 0;

  hex_decode(salt, strlen(salt), salt);
  salt[strlen(salt) / 2] = 0;
  return salt;
}

char *get_salt_for_relation(Oid relation_oid)
{
  ObjectAddress relation_object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = 0};
  return get_salt_from_seclabel(GetSecurityLabel(&relation_object, PROVIDER_TAG));
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

#define FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object)                                                                  \
  FAILWITH_CODE(ERRCODE_FEATURE_NOT_SUPPORTED, "Anonymization label `%s` not supported on objects of type `%s`",       \
                seclabel, getObjectTypeDescription(object))

static void verify_pg_features(Oid relation_id)
{
  if (has_subclass(relation_id) || has_superclass(relation_id))
    FAILWITH("Anonymization over tables using inheritance is not supported.");
}

static void verify_salt_suffix(const char *seclabel)
{
  if (get_salt_from_seclabel(seclabel) == NULL)
    FAILWITH("Expected format for relations security label: 'sensitive:<hex-encoded-salt>'");
}

static void object_relabel(const ObjectAddress *object, const char *seclabel)
{
  if (!superuser())
    FAILWITH_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE, "only a superuser can set anonymization labels");

  if (seclabel == NULL)
    return;

  if (is_sensitive_label(seclabel) || is_public_label(seclabel))
  {
    if (is_sensitive_label(seclabel))
    {
      verify_pg_features(object->objectId);
      verify_salt_suffix(seclabel);
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
