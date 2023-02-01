"""REST API for posts."""
import sys

import flask
from flask import request, session, url_for
import insta485
from insta485 import invalid_usage


@insta485.app.route('/api/v1/posts/<int:postid>/')
def get_post(postid):
    """Return details for one post."""
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

    if postid not in [post["postid"] for post in insta485.model.query_db(
            "SELECT p.postid "
            "FROM posts p ", )]:
        raise invalid_usage.InvalidUsage('Not Found', status_code=404)

    comments = insta485.model.query_db(
        "SELECT c.commentid, c.owner, c.text "
        "FROM comments c "
        "WHERE c.postid = ?"
        "ORDER BY c.commentid ASC ",
        (postid,)
    )
    for comment in comments:
        comment["lognameOwnsThis"] = (username == comment["owner"])
        comment["ownerShowUrl"] = url_for("show_user",
                                          username=comment["owner"])
        comment["url"] = "/api/v1/comments/" + str(comment["commentid"]) + "/"

    post = insta485.model.query_db(
        "SELECT p.owner, p.filename, p.created, "
        "u.filename AS 'owner_img_url' "
        "FROM posts p, users u "
        "WHERE p.postid = ? "
        "AND  p.owner = u.username ",
        (postid,), one=True
    )

    likes_data = insta485.model.query_db(
        "SELECT l.likeid, l.owner "
        "FROM likes l "
        "WHERE l.postid = ? ",
        (postid,)
    )
    logname_like = (username in [like["owner"] for like in likes_data])
    like_id_logname = [like["likeid"] for like in likes_data
                       if like["owner"] == username]
    likes = {"lognameLikesThis": logname_like,
             "numLikes": len(likes_data),
             "url": f"/api/v1/likes/{like_id_logname[0]}/"
             if logname_like else None}

    context = {
        "comments": comments,
        "comments_url": "/api/v1/comments/?postid=" + str(postid),
        "created": post["created"],
        "imgUrl": url_for('uploads', filename=post["filename"]),
        "likes": likes,
        "owner": post["owner"],
        "ownerImgUrl": url_for('uploads', filename=post["owner_img_url"]),
        "ownerShowUrl": url_for("show_user", username=post["owner"]),
        "postShowUrl": url_for("show_post", postid=postid),
        "postid": postid,
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
