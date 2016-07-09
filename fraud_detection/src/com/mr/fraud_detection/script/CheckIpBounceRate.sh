#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.CheckIpBounceRate

output_path=/mroutput/dongjiahao/CheckIpBounceRate/log_date=$ONE_DAY_AGO
new_basedata_path=/user/ads/mid/dmp/access

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $new_basedata_path $ONE_DAY_AGO
