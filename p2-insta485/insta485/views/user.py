"""
Insta485 users view.

URLs include:
/users/*
"""
import flask
import insta485


@insta485.app.route('/users/<username>/', methods=['GET'])
def show_user(username):
    """Show user page."""
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))
    # Connect to database
    connection = insta485.model.get_db()
    logname = flask.session['username']

    users = connection.execute(
        "SELECT  * "
        "FROM users "
        "WHERE users.username == ?",
        (username,)
    ).fetchall()

    if len(users) == 0:
        print("User not found in db.")
        flask.abort(404)

    posts = connection.execute(
        "SELECT * "
        "FROM posts "
        "WHERE posts.owner == ? "
        "ORDER BY posts.postid",
        (username,)
    ).fetchall()

    followings = connection.execute(
        "SELECT COUNT(DISTINCT following.username2) AS 'no_following' "
        "FROM following "
        "WHERE following.username1 == ? ",
        (username,)
    ).fetchall()[0]['no_following']

    follower = connection.execute(
        "SELECT COUNT(DISTINCT following.username1) AS 'no_followers' "
        "FROM following "
        "WHERE following.username2 == ? ",
        (username,)
    ).fetchall()[0]['no_followers']

    people_following = connection.execute(
        "SELECT following.username2 AS 'followers' "
        "FROM following "
        "WHERE following.username1 == ? ",
        (logname,)
    ).fetchall()
    people_following = [people['followers'] for people in people_following]

    context = {"users": users[0], "posts": posts, "logname": logname,
               "total_posts": len(posts), "people_following": people_following,
               "following": followings, "followers": follower}

    return flask.render_template("user.html", **context)


@insta485.app.route('/users/<username>/followers/', methods=['GET'])
def followers(username):
    """Show follower page."""
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))

    logname = flask.session['username']
    connection = insta485.model.get_db()

    users = connection.execute(
        "SELECT  * "
        "FROM users "
        "WHERE users.username == ?",
        (username,)
    ).fetchall()

    if len(users) == 0:
        print("User not found in db.")
        flask.abort(404)

    follower = connection.execute(
        "SELECT f.username1 as 'username', u.filename "
        "FROM following f "
        "JOIN users u ON u.username == f.username1 "
        "WHERE f.username2 == ? ",
        (username,)
    ).fetchall()

    people_followers = connection.execute(
        "SELECT f.username1 AS 'followers' "
        "FROM following f "
        "WHERE f.username2 == ? ",
        (logname,)
    ).fetchall()
    people_followers = [people['followers'] for people in people_followers]

    context = {"logname": logname, "followers": follower,
               "people_followers": people_followers,
               "current_username": username}

    return flask.render_template("followers.html", **context)


@insta485.app.route('/users/<username>/following/', methods=['GET'])
def following(username):
    """Show following page."""
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))

    logname = flask.session['username']
    connection = insta485.model.get_db()

    users = connection.execute(
        "SELECT  * "
        "FROM users "
        "WHERE users.username == ?",
        (username,)
    ).fetchall()

    if len(users) == 0:
        print("User not found in db.")
        flask.abort(404)

    followings = connection.execute(
        "SELECT f.username2 as 'username', u.filename "
        "FROM following f "
        "JOIN users u ON u.username == f.username2 "
        "WHERE f.username1 == ? ",
        (username,)
    ).fetchall()

    people_following = connection.execute(
        "SELECT f.username2 AS 'following' "
        "FROM following f "
        "WHERE f.username1 == ? ",
        (logname,)
    ).fetchall()
    people_following = [people['following'] for people in people_following]

    context = {"logname": logname, "following": followings,
               "people_following": people_following,
               "current_username": username}

    return flask.render_template("following.html", **context)
