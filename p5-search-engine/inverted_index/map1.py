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
    csv_array = []
    for row in csv.reader(line.strip()):
        content = re.sub(r"[^a-zA-Z0-9 ]+", "", row[0])
        content = content.strip()
        if not content or len(content) == 0:
            continue
        csv_array.append(content)
    doc_id = csv_array[0]
    title = csv_array[1]
    body = csv_array[2]
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
