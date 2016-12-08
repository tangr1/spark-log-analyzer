import React from 'react';
import {connect} from 'react-redux';
import Table from '../components/Table';

export const TablePage = () => {
  /*
   <Table column={[1,2,3]} file="/data/zhiyu/conversion_rate.csv"/>
   <Table file="/data/zhiyu/order_rate_saudi.csv"/>
   <Table file="/data/zhiyu/order_rate.csv"/>
   <Table file="/data/test/login.csv"/>
   */
  return (
    <div>
      <Table file="/data/newsdog/media_category.csv" title="英语图片视频兴趣分布"/>
      <Table file="/data/newsdog/hindi_media_category.csv" title="印地语图片视频兴趣分布"/>
    </div>
  );
};

export default connect()(TablePage);
