#ifndef PG_OPENDIFFIX_UTILS_H
#define PG_OPENDIFFIX_UTILS_H

#define DEBUG

#ifdef DEBUG
#define LOG_DEBUG(...) ereport(LOG, (errmsg(__VA_ARGS__)))
#else
#define LOG_DEBUG(...)
#endif

#endif /* PG_OPENDIFFIX_UTILS_H */
