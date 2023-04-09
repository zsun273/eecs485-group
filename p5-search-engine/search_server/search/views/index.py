"""
Search server index (main) view.

URLs include:
/
"""
import heapq
import threading
import flask
import requests
import search


class Search:
    """A class to share information among different threads."""

    def __init__(self, index_urls, query, weight):
        """Construct a class."""
        self.threads = []
        self.hits = []
        for index_url in index_urls:
            self.threads.append(threading.Thread(
                target=self.query_index, args=(index_url, query, weight, )))
        self.wait()

    def query_index(self, index_url, query, weight):
        """Query index server."""
        params = {'q': query, 'w': weight}
        response = requests.get(index_url, params=params,
                                timeout=1).json()
        for hit in response['hits']:
            self.hits.append((-1*hit['score'], hit['docid']))

    def wait(self):
        """Wait for threads running."""
        for thread in self.threads:
            thread.start()
        for thread in self.threads:
            thread.join()


@search.app.route('/', methods=["GET"])
def show_index():
    """Return a html for searching page."""
    # Make concurrent requests
    index_urls = search.app.config['SEARCH_INDEX_SEGMENT_API_URLS']
    query = flask.request.args.get('q')
    if not query:
        context = {"docs": [], "query": "", "weight": 0.5}
        return flask.render_template("index.html", **context)
    weight = flask.request.args.get('w')
    if not weight:
        weight = 0.5
    result = Search(index_urls, query, weight)
    # Get top 10 documents
    hits = result.hits
    heapq.heapify(hits)
    max_iter = 10
    if len(hits) < 10:
        max_iter = len(hits)
    # Access documents from database
    connection = search.model.get_db()
    docs = []
    for _ in range(max_iter):
        cur = connection.execute(
            "SELECT * "
            "FROM Documents "
            "WHERE docid == ? ",
            (heapq.heappop(hits)[1],)
        )
        docs.append(cur.fetchone())
    context = {"docs": docs, "query": query, "weight": weight}
    return flask.render_template("index.html", **context)
