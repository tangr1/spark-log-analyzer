import React, { PropTypes } from 'react';
import { Link, IndexLink } from 'react-router';

const App = (props) => {
  return (
    <div>
      <IndexLink to="/">首页</IndexLink>
      {' | '}
      <Link to="/table">表格</Link>
      {' | '}
      <Link to="/chart">图表</Link>
      {' | '}
      <Link to="/about">关于</Link>
      <br/>
      {props.children}
    </div>
  );
};

App.propTypes = {
  children: PropTypes.element
};

export default App;
