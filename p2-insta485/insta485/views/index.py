"""
Insta485 index (main) view.

URLs include:
/
"""
import flask
import arrow
import insta485


@insta485.app.route('/')
def show_index():
    """Display / route."""
    # Connect to database
    connection = insta485.model.get_db()

    # Query database
    logname = "awdeorio"
    cur = connection.execute(
        "SELECT DISTINCT posts.postid, posts.owner, "
        "posts.filename, posts.created "
        "FROM posts, following "
        "WHERE posts.owner == ? "
        "OR (following.username1 == ? AND following.username2 == posts.owner)"
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
    print(flask.session)

    return flask.render_template("index.html", **context)
