#!/usr/bin/env bash

##!/usr/bin/env bash
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
#rm=/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ipField_${ONE_DAY_AGO}_test
#for i in {0..24}
#do
#HOUR=`date -d -${i}hour +%H`
#
#jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
#jar_name=hadoop_test-1.0-jar-with-dependencies.jar
#
##class_name=com.mr.fraud_detection.PredictFutureFrdIp
##
##output_path=/mroutput/dongjiahao/PredictFutureFrdIpTest/log_date=$ONE_DAY_AGO/log_hour=${HOUR}
##original_click_path=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}/log_hour=${HOUR}
##original_impression_path=/user/ads/log/original_show/log_date=${ONE_DAY_AGO}/log_hour=${HOUR}
##
##hadoop fs -rmr -skipTrash $output_path
##hadoop jar $jar_dir/$jar_name $class_name $output_path $original_click_path $original_impression_path
##
##fraud_ip=/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_${ONE_DAY_AGO}_test
##hadoop fs -text /mroutput/dongjiahao/PredictFutureFrdIpTest/log_date=$ONE_DAY_AGO/log_hour=${HOUR}/part* | sort -t $'\001' -k 3 -g -r | awk -F"\001" '{if($3>=30 && $4>0.15) print $1}' | uniq -u >> ${fraud_ip}
#
#################################################################################
#
#class_name=com.mr.fraud_detection.PredictFutureIpField
#
#output_path=/mroutput/dongjiahao/PredictFutureIpFieldTest/log_date=$ONE_DAY_AGO/log_hour=${HOUR}
#original_click_path=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}/log_hour=${HOUR}
#original_impression_path=/user/ads/log/original_show/log_date=${ONE_DAY_AGO}/log_hour=${HOUR}
#
#hadoop fs -rmr -skipTrash $output_path
#hadoop jar $jar_dir/$jar_name $class_name $output_path $original_click_path $original_impression_path
#
#fraud_ipField=/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ipField_${ONE_DAY_AGO}_test
#hadoop fs -text /mroutput/dongjiahao/PredictFutureIpFieldTest/log_date=$ONE_DAY_AGO/log_hour=${HOUR}/part* >> ${fraud_ipField}
#hadoop fs -rm /share_data/blocked_ip/fraud_ipField_${ONE_DAY_AGO}
#hadoop fs -put $fraud_ip /share_data/blocked_ip/
#done

#!/usr/bin/env bash
hadoop fs -mkdir /share_data/blocked_ip/ip_field
for i in {1..14}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
date="$ONE_DAY_AGO"

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.PredictFutureIpField

output_path=/mroutput/dongjiahao/PredictFutureIpFieldTest/log_date=$ONE_DAY_AGO
original_click_path=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}
original_impression_path=/user/ads/log/original_show/log_date=${ONE_DAY_AGO}

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $original_click_path $original_impression_path

fraud_ipField=/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ipField_${ONE_DAY_AGO}_test
hadoop fs -text /mroutput/dongjiahao/PredictFutureIpFieldTest/log_date=$ONE_DAY_AGO/part* > ${fraud_ipField}
cat ${fraud_ipField} > temp
cat temp | sort -k 1 | uniq | awk '{print $0"\001""'$date'"}'> fraud_ip_with_date_$ONE_DAY_AGO
hadoop fs -rm /share_data/blocked_ip/ip_field/fraud_ip_with_date_$ONE_DAY_AGO
hadoop fs -put fraud_ip_with_date_$ONE_DAY_AGO /share_data/blocked_ip/ip_field/
rm fraud_ip_with_date_$ONE_DAY_AGO
rm temp
done