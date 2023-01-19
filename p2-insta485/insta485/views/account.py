"""
Insta485 accounts view.

URLs include:
/accounts/*
"""
import flask
import insta485


@insta485.app.route('/accounts/login/', methods=['GET', 'POST'])
def login():
    """POST-only route for handling login requests."""
    flask.request.form['username'] = 'awdeorio'
    print("DEBUG Login:", flask.request.form['username'])
    flask.session['username'] = flask.request.form['username']
    print(flask.session)
    return flask.redirect(flask.url_for('show_index'))


@insta485.app.route('/accounts/logout/', methods=['GET', 'POST'])
def logout():
    """POST-only route for handling logout requests."""
    print("DEBUG Logout:", flask.session['username'])
    flask.session.clear()
    print(flask.session)
    return flask.redirect(flask.url_for('show_index'))
