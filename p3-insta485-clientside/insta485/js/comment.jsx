import React, { useState } from "react";
import PropTypes from "prop-types";

export default function Comment({ url, setComments, load }) {
  const [text, setText] = useState("");

  function handleType(e) {
    setText(e.target.value);
  }

  function handleComment(e) {
    e.preventDefault();
    // Call REST API to post comment
    fetch(url, {
      credentials: "same-origin",
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    })
      .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        return response.json();
      })
      .then((data) => {
        setComments((prevState) => [...prevState, data]);
        console.log(data);
        setText("");
      })
      .catch((error) => console.log(error));
  }

  if (!load) {
    return null;
  }

  return (
    <div>
      <form className="comment-form" onSubmit={handleComment}>
        <input type="text" value={text} onChange={handleType} />
      </form>
    </div>
  );
}

Comment.propTypes = {
  url: PropTypes.string.isRequired,
  setComments: PropTypes.func.isRequired,
  load: PropTypes.bool.isRequired,
};
