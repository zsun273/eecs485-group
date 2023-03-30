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
    for line in group:
        values = line.partition("\t")[2].strip()
        tf_ik = values.split(" ")[0]
        idf_k = values.split(" ")[1]
        w_ik = float(tf_ik) * float(idf_k)
        print(f"{key} {tf_ik} {idf_k} {w_ik}")  # {tk} {di} {tfik} {nk}


if __name__ == "__main__":
    main()
