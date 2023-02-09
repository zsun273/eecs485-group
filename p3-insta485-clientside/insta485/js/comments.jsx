import React from "react";
import PropTypes from "prop-types";

export default function Comments({ comment, setComments }) {
  function handleUncomment() {
    // Call REST API to delete comment
    fetch(comment.url, { credentials: "same-origin", method: "DELETE" })
      .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        setComments((prevState) =>
          prevState.filter((item) => item.commentid !== comment.commentid)
        );
      })
      .catch((error) => console.log(error));
  }

  let content;
  content = null;
  if (comment.lognameOwnsThis) {
    content = (
      <button
        className="delete-comment-button"
        type="button"
        onClick={handleUncomment}
      >
        DELETE
      </button>
    );
  }

  return (
    <div>
      <a href={comment.ownerShowUrl}>{comment.owner}</a>
      <span className="comment-text">{comment.text}</span>
      {content}
    </div>
  );
}

Comments.propTypes = {
  comment: PropTypes.shape({
    commentid: PropTypes.number,
    lognameOwnsThis: PropTypes.bool,
    owner: PropTypes.string,
    ownerShowUrl: PropTypes.string,
    text: PropTypes.string,
    url: PropTypes.string,
  }).isRequired,
  setComments: PropTypes.func.isRequired,
};
