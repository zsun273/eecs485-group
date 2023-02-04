"""Insta485 model (database) API."""
import hashlib
import sqlite3
import uuid

import flask
from flask import request, session
import insta485
from insta485 import invalid_usage


def dict_factory(cursor, row):
    """Convert database row objects to a dictionary keyed on column name.

    This is useful for building dictionaries which are then used to render a
    template.  Note that this would be inefficient for large queries.
    """
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_db():
    """Open a new database connection.

    Flask docs:
    https://flask.palletsprojects.com/en/1.0.x/appcontext/#storing-data
    """
    if 'sqlite_db' not in flask.g:
        db_filename = insta485.app.config['DATABASE_FILENAME']
        flask.g.sqlite_db = sqlite3.connect(str(db_filename))
        flask.g.sqlite_db.row_factory = dict_factory

        # Foreign keys have to be enabled per-connection.  This is an sqlite3
        # backwards compatibility thing.
        flask.g.sqlite_db.execute("PRAGMA foreign_keys = ON")

    return flask.g.sqlite_db


def query_db(query, args=(), one=False):
    """Make SQL queries."""
    cur = get_db().execute(query, args)
    res = cur.fetchall()
    close_db(None)
    return (res[0] if res else None) if one else res


def update_db(query, args=()):
    """Update database records."""
    try:
        get_db().execute(query, args)
        close_db(None)
    except sqlite3.Error as err:
        print("UPDATE ERROR: ", err)


def encrypt_with_salt(password_1, salt_pass):
    """Encrypting password with known salt and algorithm."""
    algorithm = 'sha512'
    hash_obj = hashlib.new(algorithm)
    password_salted = salt_pass + password_1
    hash_obj.update(password_salted.encode('utf-8'))
    password_hash = hash_obj.hexdigest()
    return password_hash


def encrypt_new_password(password_1):
    """Encrypting password with default salt and algorithm."""
    algorithm = 'sha512'
    salt = uuid.uuid4().hex
    hash_obj = hashlib.new(algorithm)
    password_salted = salt + password_1
    hash_obj.update(password_salted.encode('utf-8'))
    password_hash = hash_obj.hexdigest()
    password_db_string = "$".join([algorithm, salt, password_hash])
    return password_db_string


def check_authorization():
    """Check authorizations."""
    if session:
        if 'username' not in session:
            raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
        username = session['username']
    elif request.authorization:
        username = request.authorization['username']
        password = request.authorization['password']
        exist = insta485.model.query_db('SELECT password '
                                        'FROM users WHERE username=?',
                                        (username,))
        if not exist:
            raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
        _, salt_pass, encrpt_password = exist[0]['password'].split('$')
        paswd_entered = insta485.model.encrypt_with_salt(password, salt_pass)
        if paswd_entered != encrpt_password:
            raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
    else:
        raise invalid_usage.InvalidUsage('Forbidden', status_code=403)
    return username


@insta485.app.teardown_appcontext
def close_db(error):
    """Close the database at the end of a request.

    Flask docs:
    https://flask.palletsprojects.com/en/1.0.x/appcontext/#storing-data
    """
    assert error or not error  # Needed to avoid superfluous style error
    sqlite_db = flask.g.pop('sqlite_db', None)
    if sqlite_db is not None:
        sqlite_db.commit()
        sqlite_db.close()
