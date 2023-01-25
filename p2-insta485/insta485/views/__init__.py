"""Views, one for each Insta485 page."""
from insta485.views.index import show_index
from insta485.views.index import handle_likes
from insta485.views.index import handle_comments
from insta485.views.index import uploads
from insta485.views.index import static_uploads
from insta485.views.account import *
from insta485.views.user import show_user
from insta485.views.user import following
from insta485.views.user import followers
from insta485.views.post import show_post
