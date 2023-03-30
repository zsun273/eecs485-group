#!/usr/bin/env python3
"""Word count reducer."""
import math
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
    count = 0  # nk
    output = []
    for line in group:
        value = line.partition("\t")[2].strip()
        output.append([key, value])
        count += 1
    n_doc = 0
    with open("total_document_count.txt", "r", encoding="utf-8") as total:
        for line in total:
            n_doc = int(line.strip())
    idfk = math.log(n_doc/count, 10)
    for k, value in output:
        print(f"{k} {value} {idfk}")  # {tk} {di} {tfik} {idfk}


if __name__ == "__main__":
    main()
