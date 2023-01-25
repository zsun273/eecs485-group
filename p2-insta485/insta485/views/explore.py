"""
Insta485 explore view.

URLs include:
/explore/
"""
import flask
import insta485


@insta485.app.route('/explore/', methods=['GET'])
def show_explore():
    """Show explore page."""
    # Redirecting
    if 'username' in flask.session:
        logname = flask.session['username']
    else:
        return flask.redirect(flask.url_for('login'))

    # Query database
    users = insta485.model.query_db(
        "SELECT DISTINCT users.username, users.filename "
        "FROM users "
        "WHERE users.username <> ? "
        "AND users.username NOT IN "
        "(SELECT following.username2 "
        "FROM following WHERE following.username1 = ?)",
        (logname, logname,)
    )

    context = {"users": users, "logname": logname}

    return flask.render_template("explore.html", **context)
