#ifndef PG_OPENDIFFIX_UTILS_H
#define PG_OPENDIFFIX_UTILS_H

#define DEBUG

#ifdef DEBUG

#include "utils/elog.h"

#define DEBUG_LOG(...) ereport(LOG, (errmsg("[PG_OPENDIFFIX] " __VA_ARGS__)))
#define DEBUG_DUMP_NODE(label, node) ereport(LOG, (errmsg("[PG_OPENDIFFIX] %s %s", label, nodeToString(node))))

#else

#define DEBUG_LOG(...)
#define DEBUG_DUMP_NODE(label, node)

#endif

#endif /* PG_OPENDIFFIX_UTILS_H */
