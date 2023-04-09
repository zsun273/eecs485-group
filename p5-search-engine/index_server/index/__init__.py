"""Index server package initializer."""
import os
from pathlib import Path
import flask


# app is a single object used by all the code modules in this package
# pylint: disable=invalid-name
app = flask.Flask(__name__)

# Load inverted index, stopwords, and pagerank into memory
app.config["INDEX_PATH"] = os.getenv("INDEX_PATH", "inverted_index_1.txt")
app.config["PATH"] = str(Path(__file__).resolve().parent)

# Tell our app about views and model.  This is dangerously close to a
# circular import, which is naughty, but Flask was designed that way.
# (Reference http://flask.pocoo.org/docs/patterns/packages/)  We're
# going to tell pylint and pycodestyle to ignore this coding style violation.
import index.api  # noqa: E402  pylint: disable=wrong-import-position
