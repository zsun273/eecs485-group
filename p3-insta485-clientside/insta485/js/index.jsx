import React, { useState, useEffect } from "react";
import PropTypes from "prop-types";
import InfiniteScroll from "react-infinite-scroll-component";
import Post from "./post";

export default function Index({ url }) {
  /* Display index page. */

  const [item, setItem] = useState([]);
  const [next, setNext] = useState(url);

  const fetchData = () => {
    // get the next 10 more posts:
    fetch(next, { credentials: "same-origin" })
      .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        return response.json();
      })
      .then((data) => {
        setNext(data.next);
        setItem([...item, ...data.results]);
      })
      .catch((error) => console.log(error));
  };

  useEffect(() => {
    // initial loading of the first 10:
    window.history.scrollRestoration = "manual";
    fetch(url, { credentials: "same-origin" })
      .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        return response.json();
      })
      .then((data) => {
        setNext(data.next);
        setItem(data.results);
      })
      .catch((error) => console.log(error));
  }, [url]);

  const rendered = item.map((results) => (
    <div key={results.postid}>
      {" "}
      <Post url={results.url} postid={results.postid} />{" "}
    </div>
  ));

  return (
    <div className="scroll">
      <InfiniteScroll
        dataLength={item.length}
        loader={<h4>Loading...</h4>}
        hasMore={next}
        next={fetchData}
      >
        {rendered}
      </InfiniteScroll>
    </div>
  );
}

Index.propTypes = {
  url: PropTypes.string.isRequired,
};
