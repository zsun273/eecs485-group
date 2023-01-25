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

    # Connect to database
    connection = insta485.model.get_db()

    # Query database
    cur = connection.execute(
        "SELECT DISTINCT users.username, users.filename "
        "FROM users "
        "WHERE users.username <> ? "
        "AND users.username NOT IN "
        "(SELECT following.username2 FROM following WHERE following.username1 == ?)",
        (logname, logname,)
    )
    users = cur.fetchall()

    context = {"users": users, "logname": logname}

    return flask.render_template("explore.html", **context)
