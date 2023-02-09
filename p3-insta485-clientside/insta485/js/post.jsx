import React, { useState, useEffect } from "react";
import PropTypes from "prop-types";
import moment from "moment";
import Like from "./like";
import Comments from "./comments";
import Comment from "./comment";

// The parameter of this function is an object with a string called url inside it.
// url is a prop for the Post component.
export default function Post({ url, postid }) {
  /* Display image and post owner of a single post */

  const [imgUrl, setImgUrl] = useState("");
  const [ownerImgUrl, setOwnerImgUrl] = useState("");
  const [ownerShowUrl, setOwnerShowUrl] = useState("");
  const [postShowUrl, setPostShowUrl] = useState("");
  const [owner, setOwner] = useState("");
  const [created, setCreated] = useState("");
  const [likes, setLikes] = useState({});
  const [comments, setComments] = useState([]);
  const [commentsUrl, setCommentsUrl] = useState("");

  useEffect(() => {
    // Declare a boolean flag that we can use to cancel the API request.
    let ignoreStaleRequest = false;

    // Call REST API to get the post's information
    fetch(url, { credentials: "same-origin" })
      .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        return response.json();
      })
      .then((data) => {
        // If ignoreStaleRequest was set to true, we want to ignore the results of the
        // the request. Otherwise, update the state to trigger a new render.
        if (!ignoreStaleRequest) {
          setImgUrl(data.imgUrl);
          setOwnerImgUrl(data.ownerImgUrl);
          setOwner(data.owner);
          setCreated(data.created);
          if (data.likes.lognameLikesThis) {
            setLikes(data.likes);
          } else {
            setLikes({
              lognameLikesThis: data.likes.lognameLikesThis,
              numLikes: data.likes.numLikes,
              url: `/api/v1/likes/?postid=${postid}`,
            });
          }
          setComments(data.comments);
          setCommentsUrl(data.comments_url);
          setPostShowUrl(data.postShowUrl);
          setOwnerShowUrl(data.ownerShowUrl);
          console.log(data);
        }
      })
      .catch((error) => console.log(error));

    return () => {
      // This is a cleanup function that runs whenever the Post component
      // unmounts or re-renders. If a Post is about to unmount or re-render, we
      // should avoid updating state.
      ignoreStaleRequest = true;
    };
  }, [url, postid]); // dependency array, effect run after first render and
  // every time one of the array changes

  const renderedComments = comments.map((comment) => (
    <div className="comment" key={comment.commentid}>
      <Comments comment={comment} setComments={setComments} />
    </div>
  ));

  // Render post
  return (
    <div className="post">
      <div className="posthead">
        <a href={ownerShowUrl} className="profile">
          <img alt="Not Loaded" src={ownerImgUrl} />
        </a>
        <a href={ownerShowUrl}>{owner}</a>
        <a href={postShowUrl} className="time">
          {moment.parseZone(created).fromNow()}
        </a>
      </div>
      <div className="photo">
        <img alt="Not Loaded" src={imgUrl} />
      </div>
      <Like likes={likes} setLikes={setLikes} postid={postid} />
      {renderedComments}
      <Comment url={commentsUrl} setComments={setComments} />
    </div>
  );
}

Post.propTypes = {
  url: PropTypes.string.isRequired,
  postid: PropTypes.number.isRequired,
};
