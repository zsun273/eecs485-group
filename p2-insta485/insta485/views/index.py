"""
Insta485 index (main) view.

URLs include:
/
"""
import flask
import arrow
import insta485


@insta485.app.route('/', methods=['GET'])
def show_index():
    """Display / route."""
    # if 'username' not in flask.session:
    #     return flask.redirect(flask.url_for('login'))

    # Connect to database
    connection = insta485.model.get_db()

    # Query database
    # logname = flask.session['username']
    logname = "awdeorio"
    cur = connection.execute(
        "SELECT DISTINCT posts.postid, posts.owner, "
        "posts.filename, posts.created "
        "FROM posts, following "
        "WHERE posts.owner == ? "
        "OR (following.username1 == ? "
        "AND following.username2 == posts.owner)"
        "ORDER BY posts.postid DESC",
        (logname, logname,)
    )
    posts = cur.fetchall()

    for post in posts:
        post['created'] = arrow.get(post['created']).humanize()
        postid = post['postid']
        username = post['owner']
        comments = connection.execute(
            "SELECT comments.owner, comments.text "
            "FROM comments "
            "WHERE comments.postid == ?"
            "ORDER BY comments.commentid ",
            (postid,)
        ).fetchall()
        post['comments'] = comments

        likes = connection.execute(
            "SELECT COUNT(DISTINCT likes.owner) AS 'number' "
            "FROM likes "
            "WHERE likes.postid == ?",
            (postid,)
        ).fetchall()
        post['likes'] = likes[0]['number']

        people_liked = connection.execute(
            "SELECT likes.owner "
            "FROM likes "
            "WHERE likes.postid == ?",
            (postid,)
        ).fetchall()
        post['people_liked'] = [people['owner'] for people in people_liked]

        users = connection.execute(
            "SELECT  users.filename "
            "FROM users "
            "WHERE users.username == ?",
            (username,)
        ).fetchall()
        post['owner_img_url'] = users[0]['filename']

    # Add database info to context
    context = {"posts": posts, "logname": logname}

    print(context)
    # print(flask.session)

    return flask.render_template("index.html", **context)


@insta485.app.route('/likes/', methods=['GET', 'POST'])
def handle_likes():
    """Handle likes request."""
    target = flask.request.args.get('target')
    print("target: ", target)
    if 'like' in flask.request.form:
        print("like action!")
    elif 'unlike' in flask.request.form:
        print("unlike action!")
    if not target:
        return flask.redirect(flask.url_for('show_index'))
    return flask.redirect(target)


@insta485.app.route('/comments/', methods=['GET', 'POST'])
def handle_comments():
    """Handle comments request."""
    target = flask.request.args.get('target')
    print("target: ", target)
    if not target:
        return flask.redirect(flask.url_for('show_index'))
    return flask.redirect(target)


@insta485.app.route('/uploads/<filename>/')
def uploads(filename):
    """Handle uploads request from upload folder."""
    return flask.send_from_directory(
        insta485.app.config["UPLOAD_FOLDER"], filename)


@insta485.app.route('/static/<filename>/')
def static_uploads(filename):
    """Handle uploads request from static folder."""
    return flask.send_from_directory(
        insta485.app.config["STATIC_FOLDER"], filename)
