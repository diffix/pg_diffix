#include "postgres.h"
#include "miscadmin.h"
#include "utils/acl.h"

#include "pg_diffix/auth.h"

static const char *PUBLISH_ROLE = "diffix_publish";

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
