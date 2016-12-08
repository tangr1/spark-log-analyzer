import React, { PropTypes, Component } from 'react';
const baby = require('babyparse');
const request = require('superagent');
import '../styles/bootstrap.min.css';

class Table extends Component {

  componentDidMount() {
    const { file } = this.props;
    request
      .get(file)
      .end((err, res) => {
        this.setState({
          data: baby.parse(res.text).data
        });
      });
  }

  render() {
    if (this.state == undefined) {
      return(<div>aaa</div>);
    }
    const data = this.state.data;
    let tds = [];
    let thead = [];
    for (let i = 0; i < data[0].length; i++) {
      tds.push(
        <td key={i}>
          {data[0][i]}
        </td>
      );
    }
    thead.push(
      <tr key={0}>
        {tds}
      </tr>
    );
    let tbody = [];
    for (let i = 1; i < data.length - 1; i++) {
      tds = [];
      for (let j = 0; j < data[i].length; j++) {
        tds.push(
          <td key={j}>
            {data[i][j]}
          </td>
        );
      }
      tbody.push(
        <tr key={i + 1}>
          {tds}
        </tr>
      );
    }
    return(
      <div className="row">
        <div className="col-md-10 col-md-offset-1">
          <table className="table table-bordered table-striped centered">
            <thead>
            {thead}
            </thead>
            <tbody>
            {tbody}
            </tbody>
          </table>
        </div>
      </div>
    );
  }
}

Table.state = {
  data: []
};

Table.propTypes = {
  file: PropTypes.string.isRequired
};

export default Table;
