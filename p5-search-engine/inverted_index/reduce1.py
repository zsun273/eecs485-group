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
    return line.partition("\t")[0]      # eg. build 2, \t, 1


def reduce_one_group(key, group):
    """Reduce one group."""
    tfik = 0                # term frequency in document di
    for line in group:
        count = line.partition("\t")[2]
        tfik += int(count)
    print(f"{key}\t{tfik}")  # output <tk, di> : tfik


if __name__ == "__main__":
    main()
