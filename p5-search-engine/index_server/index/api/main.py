"""REST API for resources urls."""
import flask
import index
import math
import re


@index.app.route('/api/v1/')
def get_api():
    """Return a list of services available."""
    context = {
        "hits": "/api/v1/hits/",
        "url": "/api/v1/"
    }
    return flask.jsonify(**context), 200


@index.app.route('/api/v1/hits/')
def get_api_hits():
    """Return a list of hits with doc ID and score."""
    query = flask.request.args.get('q')
    weight = flask.request.args.get('w', default=0.5, type=float)
    hits = get_hits(query, weight)
    context = {
        "hits": hits
    }
    return flask.jsonify(**context), 200


def get_hits(query, weight):
    """Return a list of hits given query and weight."""
    # Query processing
    word_count = {}
    query = re.sub(r"[^a-zA-Z0-9 ]+", "", query)
    query = query.casefold()
    query = list(query.split(' '))
    for word in query:
        if word in stopwords:
            continue
        if word not in word_count:
            word_count[word] = 0
        word_count[word] += 1
    terms = list(word_count.keys())
    # Return empty hits if there are no valid terms
    if not terms:
        return []

    # Select documents that contain every word in the cleaned query
    try:
        doc_ids = set(inverted_index[terms[0]][1].keys())
        for term in terms:
            doc_ids = doc_ids.intersection(set(inverted_index[term][1].keys()))
    except KeyError:
        return []
    # Return empty hits if there are no such documents
    if not doc_ids:
        return []
    
    # Calculate pagerank scores
    hits = []
    query_vector = [(word_count[term] * inverted_index[term][0]) for term in terms]
    query_norm = math.sqrt(sum(x**2 for x in query_vector))
    for doc_id in doc_ids:
        document_vector = [(inverted_index[term][1][doc_id][0] * inverted_index[term][0]) for term in terms]
        dot_prod = sum(query_vector[i] * document_vector[i] for i in range(len(query_vector)))
        document_norm = math.sqrt(inverted_index[terms[0]][1][doc_id][1])
        tf_idf = dot_prod / (query_norm * document_norm)
        score = weight * pagerank[doc_id] + (1 - weight) * tf_idf
        hits.append({"docid": doc_id, "score": score})
    hits.sort(key=lambda hit: (hit['score'], -1 * hit['docid']), reverse=True)
    return hits


def load_index(path):
    """Load inverted index, stopwords, and pagerank into memory."""
    global inverted_index
    global stopwords
    global pagerank

    # inverted_index = { term : [idf, { doc_id : [tf, norm] }] }
    inverted_index = {}
    with open(path + '/inverted_index/'+ index.app.config["INDEX_PATH"], 'r') as file_ii:
        for line in file_ii:
            term, idf, *docs_list = line.strip().split()
            inverted_index[term] = [float(idf), {}]
            for i in range(0, len(docs_list), 3):
                doc_id, tf, norm = docs_list[i:i+3]
                inverted_index[term][1][int(doc_id)] = [float(tf), float(norm)]

    # stopwords = set(stopword)
    with open(path + '/stopwords.txt', 'r') as file_sw:
        stopwords = set(stopword.strip() for stopword in file_sw)
        
    # pagerank = { doc_id : rank }
    pagerank = {}
    with open(path + '/pagerank.out', 'r') as file_pr:
        for line in file_pr:
            doc_id, rank = line.strip().split(',')
            pagerank[int(doc_id)] = float(rank)

    return


