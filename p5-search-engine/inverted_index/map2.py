#!/usr/bin/env python3
"""Word count mapper."""
import sys

for line in sys.stdin:
    line = line.strip()
    # {tk} {di}\t{tfik}
    tfik = line.partition("\t")[2]
    term_n_id = line.partition("\t")[0]
    term, doc_id = term_n_id.split(" ")
    print(f"{term}\t{doc_id} {tfik}")        # output {tk} \t {di} {tfik}
