#!/usr/bin/env bash

# operations on hadoop hive
output_ip=/data/dongjiahao/svnProject/fraudDetection/hive_result/ipInput
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
THIRTY_DAY_AGO=`date +%Y-%m-%d --date="-31 day"`
SEVEN_DAY_AGO=`date +%Y-%m-%d --date="-7 day"`
database=tmp

hive -e"
	use "$database";
	select ip,log_date,count(distinct user_agent),count(distinct browser),
		count(distinct os), count(distinct yoyi_cookie),
		count(distinct domain),count(distinct adzone_id),
		count(*)/count(distinct domain),count(*) as impression, sum(clk) as sumclk, sum(clk)/count(*) as ctr
	from algo_new_basedata
	where log_date<='$ONE_DAY_AGO' and log_date>='$THIRTY_DAY_AGO'
	group by ip,log_date
	having impression>1000
	order by log_date,ip
;" > $output_ip

python /data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/data_analysis_python/BlackList_ipWithInfo.py
hadoop dfs -rm /share_data/fraud_detection_seed/fraud_seed/robot_fraud_data
hadoop dfs -put /data/dongjiahao/svnProject/fraudDetection/hive_result/ipBL /share_data/fraud_detection_seed/fraud_seed/robot_fraud_data
