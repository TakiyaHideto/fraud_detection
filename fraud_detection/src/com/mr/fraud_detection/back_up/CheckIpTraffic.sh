
#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-16 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.CheckIpTraffic

output_path=/mroutput/dongjiahao/CheckIpTraffic/log_date=$ONE_DAY_AGO
original_click_path=/user/ads/log/original_show/log_date=${ONE_DAY_AGO}

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $original_click_path
