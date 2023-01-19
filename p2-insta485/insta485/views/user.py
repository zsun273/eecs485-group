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
    pass


@insta485.app.route('/users/<username>/followers/', methods=['GET'])
def followers(username):
    """Show follower page."""
    pass


@insta485.app.route('/users/<username>/following/', methods=['GET'])
def following(username):
    """Show following page."""
    pass
