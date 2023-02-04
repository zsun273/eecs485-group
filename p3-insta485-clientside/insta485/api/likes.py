import flask
from flask import flask, request, session, url_for
import insta485
from insta485 import invalid_usage

app = Flask(_name_)

@insta485.app.route('/api/v1/likes/?postid=<postid>/', methods = ['POST'])
def create_like():
    """Create a Like"""
    if session is None:
        raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
    else if flask.session.get('username') is None:
        logname = flask.request.authorization.get('username')
    else:
        logname = flask.session['username']

    postid = flask.request.args.get('postid')
    db = insta485.model.get_db()
    #check if the postid still exisgt exists
    the_post = db.execute (
        "SELECT from likes WHERE postid = ? ", (postid)
    ).fetchone()
    if the_post is not None:
        like_id = db.execute (
            "SELECT likeid from likes WHERE owner = ? AND postid = ? ", (logname, postid)
        ).fetchone()
    #create a dictionary:
    context = {
        "likeid" : like_id,
        "url" : f"/api/v1/likes/{like_id}/"
    }
    #if that likeid already exist:
    if like_id is not None:
        return flask.jsonify(**context), 200
    else:
        #finally uptading the database:
    db.execute (
            "INSERT into likes (owner, postid) VALUES (?, ?)", (logname, postid),
        )
        db.commit()
    return flask.jsonify(**context), 201


@insta485.app.route('/api/v1/likes/<likeid>/', methods = ['DELETE'])
def delete_like(likeid):
    """Delete a Like"""
    #first check things
    if session is None:
        raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
    else if flask.session.get('username') is None:
        logname = flask.request.authorization.get('username')
    else:
        logname = flask.session['username']

    db = insta485.model.get_db()
    exist = db.execute(
        "SELECT from likes WHERE likeid = ?", (likeid)
    ).fetchone()
    #check if like exist 
    if exist is None:
        return flask.abort(404)
    #check if the logname owns the like
    owner = db.execute (
        "SELECT owner from likes WHERE likeid = ?", (likeid)
    ).fetchone()
    if logname is not owner:
        raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
    #update/delete the like in database
    db.execute (
        "DELETE from likes WHERE  likeid = ? AND owner = ?", (likeid, logname),
    )
    db.commit()
    #return success 204 and no content
    return 204
