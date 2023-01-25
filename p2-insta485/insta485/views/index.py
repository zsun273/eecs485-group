"""
Insta485 index (main) view.

URLs include:
/
"""
import os
import pathlib
import uuid
import flask
import arrow
import insta485


@insta485.app.route('/', methods=['GET'])
def show_index():
    """Display / route."""
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))
    # Connect to database
    connection = insta485.model.get_db()

    # Query database
    logname = flask.session['username']

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

    # print(context)

    return flask.render_template("index.html", **context)


@insta485.app.route('/likes/', methods=['POST'])
def handle_likes():
    """Handle likes request."""
    # Check login status
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))
    logname = flask.session['username']

    # Get current likes information
    ans = insta485.model.query_db(
        "SELECT * "
        "FROM likes "
        "WHERE owner = ? AND postid = ? ",
        (logname, flask.request.form['postid'],)
    )

    # Handle operations
    if flask.request.form['operation'] == 'like':
        if ans:
            return flask.abort(409)
        insta485.model.update_db(
            "INSERT INTO likes(owner,postid) "
            "VALUES (?, ?) ",
            (logname, flask.request.form['postid'],)
        )
    if flask.request.form['operation'] == 'unlike':
        if not ans:
            return flask.abort(409)
        insta485.model.update_db(
            "DELETE FROM likes "
            "WHERE owner = ? AND postid = ? ",
            (logname, flask.request.form['postid'],)
        )

    # Get target
    target = flask.request.args.get('target')
    if not target:
        return flask.redirect(flask.url_for('show_index'))
    return flask.redirect(target)


@insta485.app.route('/comments/', methods=['POST'])
def handle_comments():
    """Handle comments request."""
    # Check login status
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))
    logname = flask.session['username']

    # Handle operations
    if flask.request.form['operation'] == 'create':
        if not flask.request.form['text']:
            return flask.abort(400)
        insta485.model.update_db(
            "INSERT INTO comments(owner,postid,text) "
            "VALUES (?, ?, ?) ",
            (logname, flask.request.form['postid'],
             flask.request.form['text'],)
        )
    if flask.request.form['operation'] == 'delete':
        ans = insta485.model.query_db(
            "SELECT * "
            "FROM comments "
            "WHERE owner = ? AND commentid = ? ",
            (logname, flask.request.form['commentid'],)
        )
        print(logname, flask.request.form['commentid'])
        if not ans:
            return flask.abort(403)
        insta485.model.update_db(
            "DELETE FROM comments "
            "WHERE commentid = ? ",
            (flask.request.form['commentid'],)
        )

    # Get target
    target = flask.request.args.get('target')
    if not target:
        return flask.redirect(flask.url_for('show_index'))
    return flask.redirect(target)


@insta485.app.route('/posts/', methods=['POST'])
def handle_posts():
    """Handle posts request."""
    # Check login status
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))
    logname = flask.session['username']

    # Handle operations
    if flask.request.form['operation'] == 'create':
        fileobj = flask.request.files["file"]
        if not fileobj:
            return flask.abort(400)
        filename = fileobj.filename
        stem = uuid.uuid4().hex
        suffix = pathlib.Path(filename).suffix.lower()
        uuid_basename = f"{stem}{suffix}"
        path = insta485.app.config["UPLOAD_FOLDER"]/uuid_basename
        fileobj.save(path)
        insta485.model.update_db(
            "INSERT INTO posts(owner,filename) "
            "VALUES (?, ?) ",
            (logname, uuid_basename,)
        )
    if flask.request.form['operation'] == 'delete':
        ans = insta485.model.query_db(
            "SELECT filename "
            "FROM posts "
            "WHERE owner = ? AND postid = ? ",
            (logname, flask.request.form['postid'],),
            one=True
        )
        if not ans:
            return flask.abort(403)
        os.remove(insta485.app.config["UPLOAD_FOLDER"]/ans['filename'])
        insta485.model.update_db(
            "DELETE FROM posts "
            "WHERE postid = ? ",
            (flask.request.form['postid'],)
        )

    # Get target
    target = flask.request.args.get('target')
    if not target:
        return flask.redirect(flask.url_for('show_user', username=logname))
    return flask.redirect(target)


@insta485.app.route('/following/', methods=['POST'])
def handle_following():
    """Handle following request."""
    # Check login status
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))
    logname = flask.session['username']

    # Get current following information
    ans = insta485.model.query_db(
        "SELECT * "
        "FROM following "
        "WHERE username1 = ? AND username2 = ? ",
        (logname, flask.request.form['username'],)
    )

    # Handle operations
    if flask.request.form['operation'] == 'follow':
        if ans:
            return flask.abort(409)
        insta485.model.update_db(
            "INSERT INTO following(username1,username2) "
            "VALUES (?, ?) ",
            (logname, flask.request.form['username'],)
        )
    if flask.request.form['operation'] == 'unfollow':
        if not ans:
            return flask.abort(409)
        insta485.model.update_db(
            "DELETE FROM following "
            "WHERE username1 = ? AND username2 = ? ",
            (logname, flask.request.form['username'],)
        )

    # Get target
    target = flask.request.args.get('target')
    if not target:
        return flask.redirect(flask.url_for('show_index'))
    return flask.redirect(target)


@insta485.app.route('/uploads/<filename>')
def uploads(filename):
    """Handle uploads request from upload folder."""
    if 'username' not in flask.session:
        flask.abort(403)

    path = insta485.app.config['UPLOAD_FOLDER']/filename
    if not os.path.exists(path):
        flask.abort(404)

    return flask.send_from_directory(
        insta485.app.config["UPLOAD_FOLDER"], filename)


@insta485.app.route('/static/<filename>')
def static_uploads(filename):
    """Handle uploads request from static folder."""
    return flask.send_from_directory(
        insta485.app.config["STATIC_FOLDER"], filename)
