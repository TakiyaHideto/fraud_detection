#!/usr/bin/env bash

for i in {1..1}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

hadoop dfs -rmr /share_data/fraud_detection_seed/normal_seed/baidu_cp_data/log_date=${ONE_DAY_AGO}
hadoop distcp -update -skipcrccheck hftp://m1.data.yoyi:50070/user/ads/mid/user/day/userExtract/${ONE_DAY_AGO}/BaiduCP /share_data/fraud_detection_seed/normal_seed/baidu_cp_data/log_date=${ONE_DAY_AGO}
done

#hadoop dfs -rmr /share_data/fraud_detection_seed/normal_seed/baidu_cp_data/_*