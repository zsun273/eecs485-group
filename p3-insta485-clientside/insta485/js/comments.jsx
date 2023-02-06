import React from "react";
import PropTypes from "prop-types";

export default function Comments({ comment }) {
  return (
    <div style={{ padding: 5 }}>
      <a href={comment.ownerShowUrl}>{comment.owner}</a> {comment.text}
    </div>
  );
}

Comments.propTypes = {
  comment: PropTypes.shape([]).isRequired,
};
