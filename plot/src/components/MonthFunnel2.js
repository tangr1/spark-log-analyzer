import React, { PropTypes, Component } from 'react';
const echarts = require('echarts');
const baby = require('babyparse');
const request = require('superagent');

class MonthFunnel2 extends Component {

  componentDidMount() {
    const {name, title} = this.props;
    request
      .get(this.props.file)
      .end(function(err, res) {
        const parse = baby.parse(res.text);
        let chart = echarts.init(document.getElementById(name));
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
              name:'总量',
              type:'funnel',
              width: '35%',
              left: '5%',
              label: {
                normal: {
                  formatter: '{b}: {c}%',
                  textStyle: {
                    fontSize: 20
                  }
                }
              },
              data: [
                {value: 6.27, name: '下单'},
                {value: 57.15, name: '详情'},
                {value: 18.75, name: '注册'},
                {value: 100, name: '激活'}
              ]
            },
            {
              name:'沙特',
              type:'funnel',
              width: '35%',
              left: '55%',
              label: {
                normal: {
                  formatter: '{b}: {c}%',
                  textStyle: {
                    fontSize: 20
                  }
                }
              },
              data: [
                {value: 7.31, name: '沙特下单'},
                {value: 60.87, name: '沙特详情'},
                {value: 18.46, name: '沙特注册'},
                {value: 100, name: '沙特激活'}
              ]
            }
          ]
        };
        chart.setOption(chartOptions);
      });
  }

  render() {
    const { name } = this.props; // eslint-disable-line no-use-before-define
    return (
      <div id={name} style={{width: '90%', height: '600px'}}>
      </div>
    );
  }
}

MonthFunnel2.propTypes = {
  name: PropTypes.string.isRequired,
  file: PropTypes.string.isRequired,
  title: PropTypes.string.isRequired
};

export default MonthFunnel2;
