import React, { useState, useEffect } from "react";
import PropTypes from "prop-types";
import Post from "./post";

export default function Index({ url }) {
  /* Display index page. */

  const [results, setResults] = useState([]);

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
          setResults(data.results);
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
  }, [url]); // dependency array, effect run after first render and
  // every time one of the array changes

  const renderedPosts = results.map((result) => (
    <div key={result.postid}>
      {" "}
      <Post url={result.url} postid={result.postid} />{" "}
    </div>
  ));

  // Render post
  return <div> {renderedPosts} </div>;
}

Index.propTypes = {
  url: PropTypes.string.isRequired,
};
