"""REST API for posts."""
import sys

import flask
from flask import request, session, url_for
import insta485
from insta485 import invalid_usage


@insta485.app.route('/api/v1/posts/<int:postid_url_slug>/')
def get_post(postid_url_slug):
    """Return post on postid.

    Example:
    {
      "created": "2017-09-28 04:33:28",
      "imgUrl": "/uploads/122a7d27ca1d7420a1072f695d9290fad4501a41.jpg",
      "owner": "awdeorio",
      "ownerImgUrl": "/uploads/e1a7c5c32973862ee15173b0259e3efdb6a391af.jpg",
      "ownerShowUrl": "/users/awdeorio/",
      "postShowUrl": "/posts/1/",
      "url": "/api/v1/posts/1/"
    }
    """
    context = {
        "created": "2017-09-28 04:33:28",
        "imgUrl": "/uploads/122a7d27ca1d7420a1072f695d9290fad4501a41.jpg",
        "owner": "awdeorio",
        "ownerImgUrl": "/uploads/e1a7c5c32973862ee15173b0259e3efdb6a391af.jpg",
        "ownerShowUrl": "/users/awdeorio/",
        "postid": f"/posts/{postid_url_slug}/",
        "url": flask.request.path,
    }
    return flask.jsonify(**context)


@insta485.app.route('/api/v1/posts/')
def get_top_posts():
    """Return 10 newest posts made by logged-in user or who they follow."""
    if session:
        if 'username' not in session:
            raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
        username = session['username']

    elif request.authorization:
        username = request.authorization['username']
        password = request.authorization['password']
        exist = insta485.model.query_db('SELECT password '
                                        'FROM users WHERE username=?',
                                        (username,))
        if not exist:
            raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
        _, salt_pass, encrpt_password = exist[0]['password'].split('$')
        paswd_entered = insta485.model.encrypt_with_salt(password, salt_pass)
        if paswd_entered != encrpt_password:
            raise invalid_usage.InvalidUsage('Forbidden', status_code=403)

    else:
        raise invalid_usage.InvalidUsage('Forbidden', status_code=403)

    postid_limit = request.args.get('postid_lte',
                                    default=sys.maxsize, type=int)
    size = request.args.get('size', default=10, type=int)
    page = request.args.get('page', default=0, type=int)

    if size < 0 or page < 0:
        raise invalid_usage.InvalidUsage('Bad Request', status_code=400)

    results = insta485.model.query_db(
        "SELECT DISTINCT p.postid "
        "FROM posts p, following f "
        "WHERE (p.owner == ? "
        "OR (f.username1 == ? "
        "AND f.username2 == p.owner)) "
        "AND p.postid <= ?"
        "ORDER BY p.postid DESC "
        "LIMIT ? "
        "OFFSET ?",
        (username, username, postid_limit, size, size * page)
    )

    latest_postid = request.args.get("postid_lte")
    if results and (not latest_postid):
        latest_postid = max(item["postid"] for item in results)
    for item in results:
        item["url"] = "/api/v1/posts/" + str(item["postid"]) + "/"

    # next field
    next_field = ""
    if len(results) >= size:
        next_field = url_for("get_top_posts",
                             size=size,
                             page=page + 1,
                             postid_lte=latest_postid)

    context = {
        "next": next_field,
        "results": results,
        "url": url_for("get_top_posts",
                       size=request.args.get("size"),
                       page=request.args.get("page"),
                       postid_lte=request.args.get("postid_lte"))
    }
    return flask.jsonify(**context)
