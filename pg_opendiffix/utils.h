#ifndef PG_OPENDIFFIX_UTILS_H
#define PG_OPENDIFFIX_UTILS_H

#define DEBUG

#ifdef DEBUG
#include "utils/elog.h"

#define LOG_DEBUG(...) ereport(LOG, (errmsg("[PG_OPENDIFFIX] " __VA_ARGS__)))
#define DUMP_NODE(label, node) ereport(LOG, (errmsg("[PG_OPENDIFFIX] %s %s", label, nodeToString(node))))

#else

#define LOG_DEBUG(...)
#define DUMP_NODE(label, node)

#endif

#endif /* PG_OPENDIFFIX_UTILS_H */
