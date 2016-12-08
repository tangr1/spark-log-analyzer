import React, { PropTypes, Component } from 'react';
const echarts = require('echarts');
const baby = require('babyparse');
const request = require('superagent');

class FunnelChart extends Component {

  componentDidMount() {
    const {title} = this.props;
    request
      .get(this.props.file)
      .end(function(err, res) {
        const parse = baby.parse(res.text);
        let chart = echarts.init(document.getElementById(title));
        let data = [];
        for (let i = 0; i < parse.data[0].length; i++) {
          data.push({
            name: parse.data[0][i],
            value: parse.data[1][i]
          });
        }
        const chartOptions = {
          title : {
            text: title
          },
          series: [
            {
              type:'funnel',
              label: {
                normal: {
                  formatter: '{b}: {c}%',
                  textStyle: {
                    fontSize: 20
                  }
                }
              },
              data: data
            }
          ]
        };
        chart.setOption(chartOptions);
      });
  }

  render() {
    const { title } = this.props; // eslint-disable-line no-use-before-define
    return (
      <div id={title} style={{width: '60%', height: '600px'}}>
      </div>
    );
  }
}

FunnelChart.propTypes = {
  file: PropTypes.string.isRequired,
  title: PropTypes.string.isRequired
};

export default FunnelChart;
