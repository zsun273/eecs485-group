#!/usr/bin/env python3
"""Word count reducer."""
import sys
import itertools
from collections import defaultdict


def main():
    """Divide sorted lines into groups that share a key."""
    for _, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(group)


def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def reduce_one_group(group):
    """Reduce one group."""
    #        d_i%3  \t  t_k        idf_k      d_i         tk_ik     normal
    output = defaultdict(list)
    for line in group:
        values = line.partition("\t")[2].strip()
        items = values.split(" ")
        dict_key = (items[0], items[1])
        output[dict_key].append([items[2], items[3], items[4]])

    for k in output:
        final_output = k[0] + " " + k[1]
        for value_list in output[k]:
            values = " ".join(value_list)
            final_output = final_output + " " + values
        print(final_output)


if __name__ == "__main__":
    main()
