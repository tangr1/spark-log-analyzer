import React from 'react';
import {connect} from 'react-redux';
import TopChart from '../components/TopChart';
import PieChart from '../components/PieChart';
import BarChart from '../components/BarChart';
import LineChart from '../components/LineChart';
import FunnelChart from '../components/FunnelChart';
import MonthFunnel from '../components/MonthFunnel';

export const ChartPage = () => {
  /*
   <BarChart labelUnit="k" labelBase={1000} file="/data/zhiyu/first_orders.csv" title="首单占比"/>
   <LineChart columns={[3, 4, 5, 6]} labelRotate={30} file="/data/zhiyu/conversion_rate_saudi.csv" title="2016年5月沙特详情优质下单付款转化率"/>
   <LineChart columns={[1, 2]} labelRotate={30} file="/data/zhiyu/conversion_rate_saudi.csv" title="2016年5月沙特激活注册转化率"/>
   <LineChart columns={[3, 4, 5, 6]} labelRotate={30} file="/data/zhiyu/conversion_rate.csv" title="2016年5月详情优质下单付款转化率"/>
   <LineChart columns={[1, 2]} labelRotate={30} file="/data/zhiyu/conversion_rate.csv" title="2016年5月激活注册转化率"/>
   <MonthFunnel name="chart1" file="/data/zhiyu/refund_rate.csv" title="活跃用户月总量转化漏斗"/>
   <LineChart labelRotate={30} file="/data/zhiyu/refund_rate.csv" title="退款率"/>
   <BarChart labelUnit="k" labelBase={1000} file="/data/zhiyu/old_users.csv" title="老客占比"/>
   <BarChart file="/data/zhiyu/order_counts_saudi.csv" title="2016年1-5月沙特订单数"/>
   <FunnelChart file="/data/test/funnel.csv" title="转化漏斗"/>
   <LineChart inverted={true} file="/data/test/android_rr.csv" title="安卓留存"/>
   <LineChart labelRotate={30} file="/data/test/user_daily.csv" title="每月注册用户"/>
   <BarChart labelBase={10000} file="/data/test/login.csv" title="登录用户分布"/>
   <PieChart file="/data/test/order_user_dist.csv" title="订单分布"/>
   <TopChart labelBase={10000} labelUnit="万" file="/data/test/top20.csv" title="前20收入"/>
   <PieChart percent={40} inverted={true} labelBase={10000} labelUnit="万次" file="/data/newsdog/hindi_media_category.csv" title="印地语图片视频兴趣分布"/>
   <PieChart percent={40} inverted={true} labelBase={10000} labelUnit="万次" file="/data/newsdog/media_category.csv" title="英语图片视频兴趣分布"/>
   <PieChart inverted={true} labelBase={10000} labelUnit="万次" file="/data/newsdog/hindi_article_category.csv" title="印地语文章兴趣分布"/>
   <PieChart inverted={true} labelBase={10000} labelUnit="万次" file="/data/newsdog/article_category.csv" title="用户英语文章兴趣分布"/>
   <LineChart file="/data/newsdog/qudao_dnu.csv" title="各渠道激活用户数"/>
   <LineChart file="/data/newsdog/hindi_percent.csv" title="印地语阅读文章数占推荐比例"/>
   <LineChart file="/data/newsdog/percent.csv" title="英语阅读文章数占推荐比例"/>
   <LineChart inverted={true} file="/data/newsdog/hindi_drr.csv" title="印地语日留存"/>
   <LineChart file="/data/newsdog/dqu.csv" title="日优质用户数"/>
   <LineChart file="/data/newsdog/dau.csv" title="日活跃用户数"/>
   <LineChart file="/data/newsdog/hindi_dua.csv" title="日刷出印地语文章数"/>
   <LineChart file="/data/newsdog/hindi_dux.csv" title="日刷新印地语文章次数"/>
   <LineChart file="/data/newsdog/hindi_dur.csv" title="日阅读印地语文章数"/>
   <LineChart file="/data/newsdog/hindi_dud.csv" title="日阅读印地语文章时长"/>
   <LineChart file="/data/newsdog/hindi_tunnel.csv" title="印地语转化率"/>
   <BarChart labelBase={10000} labelUnit="万" file="/data/newsdog/wau.csv" title="周活跃用户数"/>
   <LineChart file="/data/newsdog/tunnel.csv" title="英语转化率"/>
   <LineChart file="/data/newsdog/dqu.csv" title="日优质英语用户数"/>
   <LineChart inverted={true} file="/data/newsdog/drr.csv" title="英语日留存"/>
   <LineChart file="/data/newsdog/dua.csv" title="日刷出英语文章数"/>
   <LineChart file="/data/newsdog/dux.csv" title="日刷新英语文章次数"/>
   <LineChart file="/data/newsdog/dur.csv" title="日阅读英语文章数"/>
   <LineChart file="/data/newsdog/dud.csv" title="日阅读英语文章时长"/>
   <LineChart file="/data/newsdog/qudao_rr.csv" title="各渠道30日留存"/>
   <PieChart file="/data/newsdog/qudao.csv" title="各渠道每日激活用户数分布"/>
   <LineChart file="/data/newsdog/dau.csv" title="日活跃英语用户数"/>
   <LineChart file="/data/newsdog/raw_dnu.csv" title="每日抓取文章数"/>
   <LineChart file="/data/newsdog/article_dnu.csv" title="每日采纳文章数"/>
   <LineChart file="/data/newsdog/hezuo_raw_dnu.csv" title="合作媒体每日抓取文章数"/>
   <LineChart file="/data/newsdog/hezuo_raw_ratio.csv" title="合作媒体每日抓取文章数比例"/>
   <LineChart file="/data/newsdog/hezuo_article_dnu.csv" title="合作媒体每日采纳文章数"/>
   <LineChart file="/data/newsdog/hezuo_article_ratio.csv" title="合作媒体每日采纳文章数比例"/>
   <LineChart file="/data/newsdog/xiajia_raw_dnu.csv" title="下架媒体每日抓取文章数"/>
   <LineChart file="/data/newsdog/xiajia_article_dnu.csv" title="下架媒体每日采纳文章数"/>
   <LineChart columns={[1, 2]} file="/data/newsdog/dnu.csv" title="每日激活英语用户数1"/>
   <LineChart columns={[3, 4]} file="/data/newsdog/dnu.csv" title="每日激活英语用户数2"/>
   <LineChart columns={[1, 2]} file="/data/newsdog/hindi_dnu.csv" title="每日激活印地语用户数1"/>
   <LineChart columns={[3, 4]} file="/data/newsdog/hindi_dnu.csv" title="每日激活印地语用户数2"/>
   <PieChart inverted={true} file="/data/newsdog/category.csv" title="英语类别分布"/>
   <PieChart inverted={true} file="/data/newsdog/hindi_category.csv" title="印地语类别分布"/>
   */
  return (
    <div>
      <PieChart percent={80} file="/data/zhiyu/pay.csv" title="2016年5月订单数按支付方式分布"/>
      <PieChart percent={80} file="/data/zhiyu/users.csv" title="注册用户按国家分布"/>
      <LineChart file="/data/zhiyu/active_users.csv" title="日活跃用户数"/>
      <BarChart labelUnit="k" labelBase={1000} file="/data/zhiyu/old_users.csv" title="老客占比"/>
    </div>
  );
};

export default connect()(ChartPage);
