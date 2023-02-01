"""Invalid usage class, handle exceptions."""
from flask import jsonify
import insta485


class InvalidUsage(Exception):
    """Invalid usage class, handle exceptions."""

    status_code = 400

    def __init__(self, message, status_code=None):
        """Implement constructor for this class."""
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code

    def to_dict(self):
        """Convert to dictionary format."""
        response = {'message': self.message, 'status_code': self.status_code}
        return response


@insta485.app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    """Handle error and return response."""
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response
