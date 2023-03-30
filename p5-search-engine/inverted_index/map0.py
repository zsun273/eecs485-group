#!/usr/bin/env python3

"""Map 0. Read input from /input. Write output to output0."""

import csv
import sys


csv.field_size_limit(sys.maxsize)
for line in sys.stdin:
    print("doc_count\t1")
