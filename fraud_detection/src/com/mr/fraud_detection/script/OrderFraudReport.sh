#!/usr/bin/env bash

output_file=/data/dongjiahao/svnProject/fraudDetection/hive_result/order_fraud_report_origin
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
database=tmp

#hive -e"
#	use "$database";
#	select order_id,domain,sum(clk) as sumclk,count(*) as sum_impre,sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where log_date='$ONE_DAY_AGO'
#	group by order_id,domain
#	order by order_id,sumclk desc
#;" > $output_file

python /data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/data_analysis_python/orderFraudReportExtractInfo.py

date=$ONE_DAY_AGO
output=/data/dongjiahao/svnProject/fraudDetection/hive_result/report_result_$ONE_DAY_AGO
order_fraud_report=/data/dongjiahao/svnProject/fraudDetection/hive_result/order_fraud_report_processed
info_table_file=/data/dongjiahao/svnProject/fraudDetection/hive_result/report_info_temp

touch $info_table_file
./html.sh $mobile_device_id_report $info_table_file "Mobile Device Info"

cat $info_table_file > $output

cat $output | /usr/local/bin/sendEmail -f monit@yoyi.com.cn -o message-charset=utf-8 -o message-content-type=html -o tls=yes -s smtp.partner.outlook.cn -xu monit@yoyi.com.cn -xp Alarm2018 -u "移动设备信息统计报表" -t hideto.dong@yoyi.com.cn,hongbiao.jiang@yoyi.com.cn -m

rm $info_table_file