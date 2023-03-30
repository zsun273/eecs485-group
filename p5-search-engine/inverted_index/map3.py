#!/usr/bin/env python3
"""Word count mapper."""
import sys

for line in sys.stdin:
    line = line.strip()
    # {tk} {di} {tfik} {idfk}
    items = line.split(" ")
    # output {tk} {di}\t {tfik} {idfk}
    print(f"{items[0]} {items[1]}\t{items[2]} {items[3]}")
