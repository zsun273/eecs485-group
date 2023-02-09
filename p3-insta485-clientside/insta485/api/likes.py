"""Likes Rest API."""
import flask
import insta485
from insta485 import invalid_usage


@insta485.app.route('/api/v1/likes/', methods=['POST'])
def create_like():
    """Create a Like."""
    username = insta485.model.check_authorization()

    postid = flask.request.args.get('postid', None)
    database = insta485.model.get_db()

    # check if the postid exists
    if not database.execute(
            "SELECT * from posts WHERE postid = ? ", (postid,)):
        raise invalid_usage.InvalidUsage('Not Found', status_code=404)

    like_id = database.execute(
        "SELECT likeid from likes WHERE owner = ? AND postid = ? ",
        (username, postid,)
    ).fetchone()

    # create a dictionary:
    if like_id:
        likeid = like_id["likeid"]
        context = {
            "likeid": likeid,
            "url": f"/api/v1/likes/{likeid}/"
        }
        return flask.jsonify(**context), 200

    database.execute(
        "INSERT into likes (owner, postid) VALUES (?, ?)",
        (username, postid,),
    )
    database.commit()

    this_like = database.execute(
        "SELECT likeid from likes WHERE owner = ? AND postid = ? ",
        (username, postid,)
    ).fetchone()
    likeid = this_like["likeid"]

    context = {
        "likeid": likeid,
        "url": f"/api/v1/likes/{likeid}/"
    }
    return flask.jsonify(**context), 201


@insta485.app.route('/api/v1/likes/<likeid>/', methods=['DELETE'])
def delete_like(likeid):
    """Delete a Like."""
    username = insta485.model.check_authorization()

    database = insta485.model.get_db()
    exist = database.execute(
        "SELECT * from likes WHERE likeid = ?", (likeid,)
    ).fetchone()
    # check if like exist
    if exist is None:
        raise invalid_usage.InvalidUsage('Not Found', status_code=404)

    # check if the logname owns the like
    owner = database.execute(
        "SELECT owner from likes WHERE likeid = ?", (likeid,)
    ).fetchone()

    if username != owner['owner']:
        raise invalid_usage.InvalidUsage('Forbidden', status_code=403)

    # update/delete the like in database
    database.execute(
        "DELETE from likes WHERE  likeid = ? AND owner = ?",
        (likeid, username,),
    )
    database.commit()
    # return success 204 and no content
    return flask.jsonify({}), 204
