"""Comment Rest API."""
import flask
import insta485
from insta485 import invalid_usage


@insta485.app.route('/api/v1/comments/', methods=['POST'])
def create_comment():
    """Create a Comment."""
    username = insta485.model.check_authorization()
    postid = flask.request.args.get('postid', None)
    text = flask.request.json.get('text', None)

    # query database
    database = insta485.model.get_db()
    post = database.execute(
        "SELECT * "
        "FROM posts "
        "WHERE postid = ?",
        (postid,)
    ).fetchone()

    # check if the postid exists
    if not post:
        raise invalid_usage.InvalidUsage('Not Found', status_code=404)

    # update database
    if not text:
        raise invalid_usage.InvalidUsage('Bad Request', status_code=400)
    database.execute(
        "INSERT INTO comments(owner,postid,text) "
        "VALUES (?, ?, ?) ",
        (username, postid, text,)
    )
    database.commit()

    # query database
    commentid = database.execute(
        "SELECT last_insert_rowid() "
        "FROM comments "
    ).fetchone()['last_insert_rowid()']

    context = {
        "commentid": commentid,
        "lognameOwnsThis": True,
        "owner": username,
        "ownerShowUrl": f"/users/{username}/",
        "text": text,
        "url": f"/api/v1/comments/{commentid}/"
    }
    return flask.jsonify(**context), 201


@insta485.app.route('/api/v1/comments/<int:commentid>/', methods=['DELETE'])
def delete_comment(commentid):
    """Delete a Comment."""
    username = insta485.model.check_authorization()

    # query database
    ans = insta485.model.query_db(
        "SELECT * "
        "FROM comments "
        "WHERE commentid = ?",
        (commentid,),
        one=True
    )

    # check if comment exist
    if not ans:
        raise invalid_usage.InvalidUsage('Not Found', status_code=404)

    # check if the logname owns the comment
    if username != ans['owner']:
        raise invalid_usage.InvalidUsage('Forbidden', status_code=403)

    # update database
    insta485.model.update_db(
        "DELETE FROM comments "
        "WHERE commentid = ? ",
        (commentid,)
    )

    # return success 204 and no content
    return flask.jsonify({}), 204
