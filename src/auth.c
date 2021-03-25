#include "postgres.h"
#include "miscadmin.h"
#include "utils/acl.h"

/* Security labels type definitions */
#include "commands/seclabel.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/utils.h"

static const char *PUBLISH_ROLE = "diffix_publish";
static const char *PROVIDER_TAG = "pg_diffix";

static bool has_publish_access(Oid member)
{
  Oid role_oid = get_role_oid(PUBLISH_ROLE, true);
  return OidIsValid(role_oid)
             ? is_member_of_role_nosuper(member, role_oid)
             : false;
}

AccessLevel get_access_level(void)
{
  Oid user_id = GetSessionUserId();

  if (has_publish_access(user_id))
    return ACCESS_PUBLISH;

  return (AccessLevel)g_config.default_access_level;
}

static void object_relabel(const ObjectAddress *object, const char *seclabel);

void auth_init(void)
{
  register_label_provider(PROVIDER_TAG, object_relabel);
}

static inline bool is_sensitive_label(const char *seclabel)
{
  return strcasecmp(seclabel, "sensitive") == 0;
}

static inline bool is_public_label(const char *seclabel)
{
  return strcasecmp(seclabel, "public") == 0;
}

static inline bool is_aid_label(const char *seclabel)
{
  return strcasecmp(seclabel, "aid") == 0;
}

bool is_sensitive_relation(Oid relation_oid, Oid namespace_oid)
{
  ObjectAddress object = {.classId = RelationRelationId, .objectId = relation_oid, .objectSubId = 0};
  const char *seclabel = GetSecurityLabel(&object, PROVIDER_TAG);

  if (seclabel == NULL)
  {
    ObjectAddress object = {.classId = NamespaceRelationId, .objectId = namespace_oid, .objectSubId = 0};
    seclabel = GetSecurityLabel(&object, PROVIDER_TAG);
  }

  if (seclabel == NULL)
    return false;
  else if (is_sensitive_label(seclabel))
    return true;
  else if (is_public_label(seclabel))
    return false;
  else
    FAILWITH_CODE(ERRCODE_INVALID_NAME, "'%s' is not a valid label", seclabel);
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
    FAILWITH_CODE(ERRCODE_INVALID_NAME, "'%s' is not a valid label", seclabel);
}

#define FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object)                   \
  FAILWITH_CODE(                                                        \
      ERRCODE_FEATURE_NOT_SUPPORTED,                                    \
      "Anonymization label `%s` not supported on objects of type `%s`", \
      seclabel, getObjectTypeDescription(object))

static void object_relabel(const ObjectAddress *object, const char *seclabel)
{
  if (!superuser())
    FAILWITH_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE, "only a superuser can set anonymization labels");

  if (seclabel == NULL)
    return;

  if (is_sensitive_label(seclabel) || is_public_label(seclabel))
  {
    if ((object->classId == NamespaceRelationId ||
         object->classId == RelationRelationId) &&
        object->objectSubId == 0)
      return;
    FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object);
  }

  if (is_aid_label(seclabel))
  {
    if (object->classId == RelationRelationId && object->objectSubId != 0)
      return;
    FAIL_ON_INVALID_OBJECT_TYPE(seclabel, object);
  }

  FAILWITH_CODE(ERRCODE_INVALID_NAME, "'%s' is not a valid anonymization label", seclabel);
}
