import React from "react";
import PropTypes from "prop-types";

export default function Like({ likes, load, handleLike, handleUnlike }) {
  let content;
  if (likes.numLikes === 1) {
    content = <div className="comment">{likes.numLikes} like</div>;
  } else {
    content = <div className="comment">{likes.numLikes} likes</div>;
  }

  let button;
  if (likes.lognameLikesThis) {
    button = (
      <button
        className="like-unlike-button"
        type="button"
        onClick={handleUnlike}
      >
        unlike
      </button>
    );
  } else {
    button = (
      <button className="like-unlike-button" type="button" onClick={handleLike}>
        like
      </button>
    );
  }

  if (!load) {
    return <div>{content}</div>;
  }

  return (
    <div>
      {button}
      {content}
    </div>
  );
}

Like.propTypes = {
  likes: PropTypes.shape({
    lognameLikesThis: PropTypes.bool,
    numLikes: PropTypes.number,
    url: PropTypes.string,
  }).isRequired,
  handleLike: PropTypes.func.isRequired,
  handleUnlike: PropTypes.func.isRequired,
  load: PropTypes.bool.isRequired,
};
