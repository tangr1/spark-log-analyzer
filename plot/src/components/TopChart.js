import React, { PropTypes, Component } from 'react';
const echarts = require('echarts');
const baby = require('babyparse');
const request = require('superagent');

class TopChart extends Component {

  componentDidMount() {
    const { labelUnit, labelBase, file, title } = this.props;
    request
      .get(file)
      .end(function(err, res) {
        const parse = baby.parse(res.text);
        let chart = echarts.init(document.getElementById(title));
        let series = [];
        let x = [];
        let legend = [];
        for (let i = 1; i < parse.data.length - 1; i++) {
          x.push(parse.data[i][0]);
        }
        for (let i = 1; i < parse.data[0].length; i++) {
          legend.push(parse.data[0][i]);
          let data = [];
          for (let j = 1; j < parse.data.length - 1; j++) {
            data.push(parse.data[j][i]);
          }
          series.push({
            name: parse.data[0][i],
            type: 'bar',
            barMaxWidth: 10,
            label: {
              normal: {
                show: true,
                position: 'right',
                formatter: function (a) {
                  return (Math.round(a.value / labelBase) + labelUnit);
                }
              }
            },
            data: data
          });
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
          yAxis: {
            type: 'category',
            data: x
          },
          xAxis: {
            type: 'value',
            boundaryGap: false
          },
          series: series
        };
        chart.setOption(chartOptions);
      });
  }

  render() {
    const { title } = this.props; // eslint-disable-line no-use-before-define
    return (
      <div id={title} style={{width: '100%', height: '600px'}}>
      </div>
    );
  }
}

TopChart.propTypes = {
  file: PropTypes.string.isRequired,
  title: PropTypes.string.isRequired,
  labelUnit: PropTypes.string,
  labelBase: PropTypes.number
};

TopChart.defaultProps = {
  labelUnit: '',
  labelBase: 1
};

export default TopChart;
