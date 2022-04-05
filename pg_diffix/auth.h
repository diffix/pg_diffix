#ifndef PG_DIFFIX_AUTH_H
#define PG_DIFFIX_AUTH_H

#include "access/attnum.h"

extern void auth_init(void);

typedef enum AccessLevel
{
  ACCESS_DIRECT,           /* No protection - access to raw data. */
  ACCESS_PUBLISH_TRUSTED,  /* Publish access level, trusted analyst; protects against accidental re-identification. */
  ACCESS_PUBLISH_UNTRUSTED /* Publish access level, untrusted analyst; protects against intentional re-identification. */
} AccessLevel;

/* Returns true if the first access level is higher privilege than the second one. */
static inline bool is_higher_access_level(AccessLevel subject, AccessLevel target)
{
  /* The integer values of the access levels are in reverse order to privilege. */
  return subject < target;
}

/*
 * Returns the maximum access level for the current user.
 */
extern AccessLevel get_user_access_level(void);

/*
 * Returns the access level for the current session.
 */
extern AccessLevel get_session_access_level(void);

/*
 * Returns true if the relation has been labeled as sensitive, false otherwise.
 */
extern bool is_sensitive_relation(Oid relation_oid);

/*
 * Returns true if the column has been labeled as an AID, false otherwise.
 */
extern bool is_aid_column(Oid relation_oid, AttrNumber attnum);

#endif /* PG_DIFFIX_AUTH_H */
