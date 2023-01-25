"""
Insta485 users view.

URLs include:
/posts/*
"""
import arrow
import flask
import insta485


@insta485.app.route('/posts/<postid>/', methods=['GET'])
def show_post(postid):
    """Show post page."""
    if 'username' not in flask.session:
        return flask.redirect(flask.url_for('login'))

    # Connect to database
    connection = insta485.model.get_db()
    logname = flask.session['username']

    post = connection.execute(
        "SELECT p.postid, p.filename, p.owner, p.created, "
        "p.filename, u.filename AS 'owner_img_url' "
        "FROM posts p "
        "JOIN users u on p.owner = u.username "
        "WHERE p.postid == ? ",
        (postid,)
    ).fetchall()[0]
    post['created'] = arrow.get(post['created']).humanize()

    comments = connection.execute(
        "SELECT c.owner, c.text, c.commentid "
        "FROM comments c "
        "WHERE c.postid == ?"
        "ORDER BY c.commentid ",
        (postid,)
    ).fetchall()

    likes = connection.execute(
        "SELECT COUNT(DISTINCT l.owner) AS 'number' "
        "FROM likes l "
        "WHERE l.postid == ?",
        (postid,)
    ).fetchall()[0]['number']

    people_liked = connection.execute(
        "SELECT l.owner "
        "FROM likes l "
        "WHERE l.postid == ?",
        (postid,)
    ).fetchall()
    people_liked = [people['owner'] for people in people_liked]

    context = {'logname': logname, 'post': post, 'comments': comments,
               'likes': likes, 'people_liked': people_liked}

    return flask.render_template("post.html", **context)
