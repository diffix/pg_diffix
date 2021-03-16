#ifndef PG_DIFFIX_AUTH_H
#define PG_DIFFIX_AUTH_H

#include "config.h"

/*
 * Returns access level for current session user.
 */
extern AccessLevel get_access_level(void);

#endif /* PG_DIFFIX_AUTH_H */
