#!/usr/bin/env python3
"""Reduce 0."""
import sys


def main():
    """Count number of documents."""
    num_doc = 0
    for line in sys.stdin:
        line.strip()
        num_doc += 1
    print(num_doc)


if __name__ == "__main__":
    main()
