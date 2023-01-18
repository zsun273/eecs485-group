"""Insta485 development configuration."""


import pathlib


# Root of this application, useful if it doesn't occupy an entire domain
APPLICATION_ROOT = '/'


# Secret key for encrypting cookies
SECRET_KEY = b'.\x17\xf94#\x17\xc91\xc2\x00\xb6\xf94\x97\x87Y\xb1cVw\x95"\x100'
SESSION_COOKIE_NAME = 'login'


# File Upload to var/uploads/
INSTA485_ROOT = pathlib.Path(__file__).resolve().parent.parent
UPLOAD_FOLDER = INSTA485_ROOT/'var'/'uploads'
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif'])
MAX_CONTENT_LENGTH = 16 * 1024 * 1024


# Database file is var/insta485.sqlite3
DATABASE_FILENAME = INSTA485_ROOT/'var'/'insta485.sqlite3'



