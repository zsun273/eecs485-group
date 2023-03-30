#!/usr/bin/env python3
"""Word count mapper."""
import sys

for line in sys.stdin:
    line = line.strip()
    # {di} {tk} {tfik} {idfk} {normal}
    items = line.split(" ")
    key = int(items[0]) % 3
    #        d_i%3    t_k        idf_k      d_i         tk_ik     normal
    print(f"{key}\t{items[1]} {items[3]} {items[0]} {items[2]} {items[4]}")
