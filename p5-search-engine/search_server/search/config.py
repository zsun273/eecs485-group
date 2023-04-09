"""Search server development configuration."""


import pathlib


# Root of this application, useful if it doesn't occupy an entire domain
APPLICATION_ROOT = '/'
SERACH_ROOT = pathlib.Path(__file__).resolve().parent.parent
DATABASE_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SEARCH_INDEX_SEGMENT_API_URLS = [
    "http://localhost:9000/api/v1/hits/",
    "http://localhost:9001/api/v1/hits/",
    "http://localhost:9002/api/v1/hits/",
]


# Database file is var/search.sqlite3
DATABASE_FILENAME = DATABASE_ROOT/'var'/'search.sqlite3'
