#!/usr/bin/env python3
"""Word count mapper."""
import csv
import re
import sys

csv.field_size_limit(sys.maxsize)
stops = set()
with open("stopwords.txt", "r", encoding="utf-8") as stopwords:
    for line in stopwords:
        stops.add(line.strip())

for line in sys.stdin:
    doc_id, title, body = line.split('",')
    # combine title and body
    text = title + " " + body
    # remove non-alphanumeric
    doc_id = re.sub(r"[^a-zA-Z0-9 ]+", "", doc_id)
    text = re.sub(r"[^a-zA-Z0-9 ]+", "", text)
    # covert to lowercase
    text = text.casefold()
    terms = text.split(" ")
    for term in terms:
        if term in stops:
            continue
        if len(term) == 0:
            continue
        print(f"{term} {doc_id}\t1")
