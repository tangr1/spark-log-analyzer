import React, { PropTypes, Component } from 'react';
const echarts = require('echarts');
const baby = require('babyparse');
const request = require('superagent');

class PieChart extends Component {

  componentDidMount() {
    const {percent, inverted, file, title, labelUnit, labelBase} = this.props;
    request
      .get(file)
      .end(function(err, res) {
        const parse = baby.parse(res.text);
        let chart = echarts.init(document.getElementById(title));
        let data = [];
        if (inverted) {
          for (let i = 0; i < parse.data.length; i++) {
            data.push({
              name: parse.data[i][0],
              value: parse.data[i][1]
            });
          }
        } else {
          for (let i = 0; i < parse.data[0].length; i++) {
            data.push({
              name: parse.data[0][i],
              value: parse.data[1][i]
            });
          }
        }
        const chartOptions = {
          title : {
            text: title
          },
          //color: ['#000000', '#c23531','#2f4554', '#61a0a8', '#d48265', '#91c7ae','#749f83',
          //  '#ca8622', '#bda29a','#6e7074', '#546570', '#c4ccd3'],
          series : [
            {
              type: 'pie',
              radius : percent + '%',
              label: {
                normal: {
                  show: true,
                  //formatter: "{b} ({c}, {d}%)",
                  formatter: function (params) {
                    var value = Math.round(params.value / labelBase) + labelUnit;
                    return params.name + " (" + value + ", " + params.percent + "%)";
                  },
                  textStyle: {
                    fontSize: 15
                  }
                }
              },
              data: data,
              itemStyle: {
                emphasis: {
                  shadowBlur: 10,
                  shadowOffsetX: 0,
                  shadowColor: 'rgba(0, 0, 0, 0.5)'
                }
              }
            }
          ]
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

PieChart.defaultProps = {
  percent: 80,
  inverted: false,
  labelUnit: '',
  labelBase: 1
};

PieChart.propTypes = {
  file: PropTypes.string.isRequired,
  title: PropTypes.string.isRequired,
  percent: PropTypes.number,
  inverted: PropTypes.bool,
  labelUnit: PropTypes.string,
  labelBase: PropTypes.number
};

export default PieChart;
