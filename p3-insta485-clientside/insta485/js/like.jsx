import React from "react";
import PropTypes from "prop-types";

export default function Like({ likes }) {
  let content;
  if (likes.numLikes === 1) {
    content = <p>{likes.numLikes} like</p>;
  } else {
    content = <p>{likes.numLikes} likes</p>;
  }
  return <div style={{ padding: 5 }}>{content}</div>;
}

Like.propTypes = {
  likes: PropTypes.shape({
    numLikes: PropTypes.number,
  }).isRequired,
};
