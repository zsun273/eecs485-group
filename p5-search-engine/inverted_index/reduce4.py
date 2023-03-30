#!/usr/bin/env python3
"""Word count reducer."""
import sys
import itertools


def main():
    """Divide sorted lines into groups that share a key."""
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def reduce_one_group(key, group):
    """Reduce one group."""
    # {di}\t{tk} {tfik} {idfk} {w_ik}
    normalization = 0
    output = []
    for line in group:
        values = line.partition("\t")[2].strip()
        items = values.split(" ")
        output.append([items[0], items[1], items[2]])
        normalization += float(items[3])**2
    for t_k, tf_ik, idf_k in output:
        # {di} {tk} {tfik} {idfk} {normal}
        print(f"{key} {t_k} {tf_ik} {idf_k} {normalization}")


if __name__ == "__main__":
    main()
