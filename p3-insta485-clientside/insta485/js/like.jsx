import React from "react";
import PropTypes from "prop-types";

export default function Like({ likes, setLikes, postid }) {
  function handleLike() {
    // Call REST API to post Like
    fetch(likes.url, { credentials: "same-origin", method: "POST" })
      .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        return response.json();
      })
      .then((data) => {
        setLikes((prevState) => ({
          lognameLikesThis: true,
          numLikes: prevState.numLikes + 1,
          url: data.url,
        }));
        console.log(data);
      })
      .catch((error) => console.log(error));
  }

  function handleUnlike() {
    // Call REST API to delete Like
    fetch(likes.url, { credentials: "same-origin", method: "DELETE" })
      .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        setLikes((prevState) => ({
          lognameLikesThis: false,
          numLikes: prevState.numLikes - 1,
          url: `/api/v1/likes/?postid=${postid}`,
        }));
      })
      .catch((error) => console.log(error));
  }

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
  setLikes: PropTypes.func.isRequired,
  postid: PropTypes.number.isRequired,
};
