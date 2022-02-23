#ifndef PG_DIFFIX_REGEX_UTILS_H
#define PG_DIFFIX_REGEX_UTILS_H

/*
 * Initializes regular expression patterns.
 */
extern void regex_init(void);

/*
 * Releases regular expression patterns.
 */
extern void regex_cleanup(void);

/*
 * Whether `string` matches the generalization pattern of "money-style" numbers, i.e. 1, 2, or 5 preceeded by or
 * followed by zeros: ⟨... 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, ...⟩
 */
extern bool generalization_regex_match(const char *string);

#endif /* PG_DIFFIX_REGEX_UTILS_H */
