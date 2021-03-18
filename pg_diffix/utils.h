#ifndef PG_DIFFIX_UTILS_H
#define PG_DIFFIX_UTILS_H

#include "utils/elog.h"

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

#define FAILWITH_LOCATION(cursorpos, ...) \
  ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__), errposition((cursorpos) + 1)))

#define DEBUG

#ifdef DEBUG

#define DEBUG_LOG(...) ereport(LOG, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))
#define DEBUG_DUMP_NODE(label, node) ereport(LOG, (errmsg("[PG_DIFFIX] %s %s", label, nodeToString(node))))

#else

#define DEBUG_LOG(...)
#define DEBUG_DUMP_NODE(label, node)

#endif

#endif /* PG_DIFFIX_UTILS_H */
