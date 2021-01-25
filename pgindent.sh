#!/bin/sh

#
# Based on Andrew Dunstan's blog post ideas:
# - http://adpgtech.blogspot.com/2015/05/running-pgindent-on-non-core-code-or.html
#

FILES=${1:-'pg_diffix.c pg_diffix/*.c pg_diffix/*.h'}

if [ ! -f pg_diffix.so ]; then
    echo "ERROR: File pg_diffix.so not found! Run make and try again"
    exit 1
fi

objdump -W pg_diffix.so | \
    egrep -A3 DW_TAG_typedef | \
    perl -e ' while (<>) { chomp; @flds = split;next unless (1 < @flds); \
        next if $flds[0]  ne "DW_AT_name" && $flds[1] ne "DW_AT_name"; \
        next if $flds[-1] =~ /^DW_FORM_str/; \
        print $flds[-1],"\n"; }'  | \
    sort | uniq > pg_diffix.typedefs

for FILE in ${FILES}; do
    pgindent --typedefs=pg_diffix.typedefs $FILE
done
