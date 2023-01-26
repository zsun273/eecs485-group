"""
Insta485 accounts view.

URLs include:
/accounts/*
"""
import os
import uuid
import hashlib
from _pytest import pathlib
from flask import request, redirect, session, \
    url_for, abort, render_template
import insta485


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


@insta485.app.route('/accounts/', methods=['POST'])
def operation():
    """POST-only route for handling account operation requests."""
    if request.form.get('operation') == 'login':
        return ac_login()
    if request.form.get('operation') == 'create':
        return ac_create()
    if request.form.get('operation') == 'delete':
        return ac_delete()
    if request.form.get('operation') == 'edit_account':
        return ac_edit()
    if request.form.get('operation') == 'update_password':
        return ac_update()
    return redirect(url_for('show_index'))


def ac_login():
    """Handle login requests."""
    redir = request.args.get('target', default=url_for('show_index'), type=str)
    username = request.form.get('username')
    passwords = request.form.get('password')
    if not username or not passwords:
        abort(400)
    exist = insta485.model.query_db('SELECT password '
                                    'FROM users WHERE username=?', (username,))
    print(exist)
    if not exist:
        abort(403)
    _, salt_pass, encrpt_password = exist[0]['password'].split('$')
    paswd_entered = encrypt_with_salt(passwords, salt_pass)
    if paswd_entered != encrpt_password:
        abort(403)
    session['username'] = username
    return redirect(redir)


def ac_create():
    """Handle creating account requests."""
    redir = request.args.get('target', default=url_for('show_index'), type=str)
    username = request.form.get('username')
    passwords = request.form.get('password')
    fullname = request.form.get('fullname')
    email = request.form.get('email')

    if not username or not passwords or \
            not fullname or not email or (not request.files['file'].filename):
        abort(400)

    file = request.files['file'].filename

    result = insta485.model.query_db("""SELECT username
                       FROM users
                       WHERE  username=?""",
                                     (username,))

    if result:
        abort(409)
    else:
        stem = uuid.uuid4().hex
        suffix = pathlib.Path(file).suffix.lower()
        uuid_basename = f"{stem}{suffix}"

        insta485.model.update_db('INSERT INTO users(username,fullname,'
                                 'email,filename,password) '
                                 'VALUES (?,?,?,?,?)',
                                 (username,
                                  fullname,
                                  email,
                                  uuid_basename,
                                  encrypt_new_password(passwords),))
        # Save to disk
        path = insta485.app.config["UPLOAD_FOLDER"] / uuid_basename
        request.files['file'].save(path)

    session['logged_in'] = True
    session['username'] = username
    return redirect(redir)


def ac_delete():
    """Handle deleting account requests."""
    redir = request.args.get('target',
                             default=url_for('show_index'), type=str)
    username = session['username']
    if not username:
        abort(403)
    files_created = insta485.model.query_db('SElECT filename '
                                            'FROM posts WHERE owner = ?',
                                            (username,))
    for file in files_created:
        path = insta485.app.config["UPLOAD_FOLDER"] / file['filename']
        os.remove(path)
    avatar = insta485.model.query_db('SElECT filename '
                                     'FROM users WHERE username = ?',
                                     (username,), one=True)
    name = avatar['filename']
    path = os.path.join(
        insta485.app.config["UPLOAD_FOLDER"],
        name)
    os.remove(path)
    insta485.model.update_db("DELETE FROM users WHERE username=?", (username,))
    session.pop('username', None)
    return redirect(redir)


def ac_edit():
    """Handle account editing requests."""
    redir = request.args.get('target', default=url_for('show_index'), type=str)
    username = session['username']
    if not username:
        abort(403)

    if not request.form.get('fullname') \
            or not request.form.get('email'):
        abort(400)

    insta485.model.update_db("""UPDATE users
                SET fullname=?, email=?
                WHERE username=?""", (
        request.form["fullname"], request.form["email"],
        username,))

    if request.files['file'].filename:
        # delete old file
        cur = insta485.model.query_db(
            "SELECT filename FROM users WHERE username=?",
            (username,), one=True)
        file_name = cur["filename"]
        path = insta485.app.config["UPLOAD_FOLDER"] / file_name
        os.remove(path)

        # store new file
        new_file = request.files["file"]
        new_name = new_file.filename

        stem = uuid.uuid4().hex
        suffix = pathlib.Path(new_name).suffix
        uuid_basename = f"{stem}{suffix}"

        insta485.model.update_db("""UPDATE users
                SET filename=?
                WHERE username=?""",
                                 (uuid_basename, username,))

        path = insta485.app.config["UPLOAD_FOLDER"] / uuid_basename
        new_file.save(path)
    return redirect(redir)


def ac_update():
    """Handle account updating requests."""
    redir = request.args.get('target', default=url_for('show_index'), type=str)
    username = session['username']
    if not username:
        abort(403)

    cur = insta485.model.query_db(
        "SELECT password FROM users WHERE username=?",
        (username,), one=True)

    print(f"sql output:{cur}")
    passwords = request.form.get('password')
    new_password = request.form.get('new_password1')
    new_password_repeat = request.form.get('new_password2')

    if not passwords or not new_password or not new_password_repeat:
        abort(400)

    _, salt, encrypted = cur['password'].split('$')
    right_password = encrypt_with_salt(passwords, salt)

    if right_password != encrypted:
        abort(403)

    if new_password != new_password_repeat:
        abort(401)

    insta485.model.update_db("UPDATE users SET password=?",
                             (encrypt_new_password(new_password),))

    return redirect(redir)


@insta485.app.route('/accounts/login/', methods=['GET', 'POST'])
def login():
    """Handle login requests."""
    if 'username' not in session:
        return render_template("login.html", logname="login")
    return redirect(url_for('show_index'))


@insta485.app.route('/accounts/logout/', methods=['POST'])
def logout():
    """POST-only route for handling logout requests."""
    print("DEBUG Logout:", session['username'])
    session.clear()
    print(session)
    return redirect(url_for('login'))


@insta485.app.route('/accounts/delete/', methods=['GET', 'POST'])
def delete():
    """POST-only route for handling deleting account requests."""
    if 'username' not in session:
        return redirect(url_for('login'))

    json_obj = {"username": session["username"]}
    return render_template("delete.html", **json_obj)


@insta485.app.route('/accounts/create/', methods=['GET', 'POST'])
def create():
    """POST-only route for handling account create requests."""
    if 'username' not in session:
        return render_template("create.html")
    return redirect(url_for('edit'))


@insta485.app.route('/accounts/edit/', methods=['GET', 'POST'])
def edit():
    """POST-only route for handling logout requests."""
    if 'username' not in session:
        return redirect(url_for('login'))
    username = session['username']
    json_obj = {"logname": username}
    info = insta485.model.query_db(
        "SELECT fullname, filename AS user_img_url, email "
        "FROM users WHERE username=?",
        (username,), one=True)
    print(info)
    return render_template("edit.html", **json_obj, **info)


@insta485.app.route('/accounts/password/', methods=['GET', 'POST'])
def password():
    """POST-only route for password requests."""
    if 'username' not in session:
        return redirect(url_for('login'))
    username = session['username']
    json_obj = {"logname": username}
    return render_template("password.html", **json_obj)
