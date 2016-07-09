#!/usr/bin/env bash

#for i in {0..24}
#do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-0 day"`
HOUR=`date -d -1hour +%H`
fraud_ip_all=/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_${ONE_DAY_AGO}

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.PredictFutureFrdIp
output_path=/mroutput/dongjiahao/PredictFutureFrdIp/log_date=$ONE_DAY_AGO/log_hour=${HOUR}
original_click_path=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}/log_hour=${HOUR}
original_impression_path=/user/ads/log/original_show/log_date=${ONE_DAY_AGO}/log_hour=${HOUR}
hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $original_click_path $original_impression_path
fraud_ip=/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_${ONE_DAY_AGO}
hadoop fs -text /mroutput/dongjiahao/PredictFutureFrdIp/log_date=$ONE_DAY_AGO/log_hour=${HOUR}/part* >> ${fraud_ip}
hadoop fs -rm /share_data/blocked_ip/fraud_ip_${ONE_DAY_AGO}
hadoop fs -put $fraud_ip /share_data/blocked_ip/

class_name=com.mr.fraud_detection.PredictFutureIpField
output_path=/mroutput/dongjiahao/PredictFutureIpFieldTest/log_date=$ONE_DAY_AGO/log_hour=${HOUR}
original_click_path=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}/log_hour=${HOUR}
original_impression_path=/user/ads/log/original_show/log_date=${ONE_DAY_AGO}/log_hour=${HOUR}
hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $original_click_path $original_impression_path
fraud_ipField=/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ipField_${ONE_DAY_AGO}
hadoop fs -text /mroutput/dongjiahao/PredictFutureIpFieldTest/log_date=$ONE_DAY_AGO/log_hour=${HOUR}/part* >> ${fraud_ipField}
hadoop fs -rm /share_data/blocked_ip/fraud_ipField_${ONE_DAY_AGO}
hadoop fs -put $fraud_ipField /share_data/blocked_ip/

cat ${fraud_ip} >> ${fraud_ip_all}
cat ${fraud_ipField} >> ${fraud_ip_all}
cat ${fraud_ip_all} > temp
cat temp | sort -k 1 | uniq > ${fraud_ip_all}
#cat temp | sort -k 1 | uniq | awk '{print $0"\001""'$date'"}' > ${fraud_ip_all}
#hadoop fs -mkdir /share_data/blocked_ip/log_date=${ONE_DAY_AGO}
#hadoop fs -rm /share_data/blocked_ip/log_date=${ONE_DAY_AGO}/${fraud_ip_all}
#hadoop fs -put ${fraud_ip_all} /share_data/blocked_ip/log_date=${ONE_DAY_AGO}/
rm temp

#cat ${fraud_ip} >> temp
#cat temp | sort -k 1 | uniq | awk '{print $0"\001"'${ONE_DAY_AGO}'}'> fraud_ip_with_date_$ONE_DAY_AGO
#hadoop fs -put fraud_ip_with_date_$ONE_DAY_AGO /share_data/blocked_ip/
#rm temp
#
#cat ${fraud_ipField} >> ${fraud_ip_all}
##done