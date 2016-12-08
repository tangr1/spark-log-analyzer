import React, { PropTypes, Component } from 'react';
const echarts = require('echarts');
const baby = require('babyparse');
const request = require('superagent');

class LineChart extends Component {

  componentDidMount() {
    const { columns, inverted, labelRotate, labelInterval, file, title } = this.props;
    request
      .get(file)
      .end(function(err, res) {
        const parse = baby.parse(res.text);
        let chart = echarts.init(document.getElementById(title));
        let series = [];
        let x = [];
        let legend = [];
        if (inverted) {
          for (let i = 1; i < parse.data[0].length; i++) {
            x.push(parse.data[0][i]);
          }
          for (let i = 1; i < parse.data.length - 1; i++) {
            legend.push(parse.data[i][0]);
            let data = [];
            for (let j = 1; j < parse.data[0].length; j++) {
              data.push(parse.data[i][j]);
            }
            series.push({
              name: parse.data[i][0],
              type: 'line',
              data: data
            });
          }
        } else {
          for (let i = 1; i < parse.data.length - 1; i++) {
            x.push(parse.data[i][0]);
          }
          var cs = columns;
          if (cs.length == 0) {
            for (var k = 1; k <= x.length; k++) {
              cs.push(k);
            }
          }
          for (let i = 1; i < parse.data[0].length; i++) {
            if (cs.indexOf(i) == -1) {
              continue;
            }
            legend.push(parse.data[0][i]);
            let data = [];
            for (let j = 1; j < parse.data.length - 1; j++) {
              data.push(parse.data[j][i]);
            }
            series.push({
              name: parse.data[0][i],
              type: 'line',
              data: data
            });
          }
        }
        const chartOptions = {
          title : {
            text: title
          },
          tooltip: {
            trigger: 'axis'
          },
          legend: {
            data: legend
          },
          grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
          },
          xAxis: {
            type: 'category',
            boundaryGap: false,
            data: x,
            axisLabel: {
              rotate: labelRotate,
              interval: labelInterval
            }
          },
          yAxis: {
            type: 'value'
          },
          series: series
        };
        chart.setOption(chartOptions);
      });
  }

  render() {
    const { title } = this.props; // eslint-disable-line no-use-before-define
    return (
      <div id={title} style={{width: '80%', height: '600px'}}>
      </div>
    );
  }
}

LineChart.defaultProps = {
  labelRotate: 0,
  labelInterval: 0,
  inverted: false,
  columns: []
};

LineChart.propTypes = {
  file: PropTypes.string.isRequired,
  title: PropTypes.string.isRequired,
  labelRotate: PropTypes.number,
  labelInterval: PropTypes.number,
  inverted: PropTypes.bool,
  columns: PropTypes.array
};

export default LineChart;
