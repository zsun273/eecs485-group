#!/usr/bin/env python3
"""Word count mapper."""
import sys

for line in sys.stdin:
    line = line.strip()
    # {tk} {di} {tfik} {idfk} {w_ik}
    items = line.split(" ")
    # output {di}\t{tk} {tfik} {idfk} {w_ik}
    print(f"{items[1]}\t{items[0]} {items[2]} {items[3]} {items[4]}")
