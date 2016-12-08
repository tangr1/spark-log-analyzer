import React, { PropTypes, Component } from 'react';
const echarts = require('echarts');
const baby = require('babyparse');
const request = require('superagent');

class MonthFunnel extends Component {

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
                {value: 4.61, name: '付款'},
                {value: 5.44, name: '下单'},
                {value: 58.26, name: '详情'},
                {value: 54.93, name: '激活'},
                {value: 12.96, name: '注册'},
                {value: 31.03, name: '优质'},
                {value: 100, name: '活跃'}
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
                {value: 6.31, name: '沙特付款'},
                {value: 6.33, name: '沙特下单'},
                {value: 63.43, name: '沙特详情'},
                {value: 54.18, name: '沙特激活'},
                {value: 12.52, name: '沙特注册'},
                {value: 36.94, name: '沙特优质'},
                {value: 100, name: '沙特活跃'}
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

MonthFunnel.propTypes = {
  name: PropTypes.string.isRequired,
  file: PropTypes.string.isRequired,
  title: PropTypes.string.isRequired
};

export default MonthFunnel;
