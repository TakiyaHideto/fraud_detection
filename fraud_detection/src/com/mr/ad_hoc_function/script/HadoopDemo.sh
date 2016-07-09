#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-0 day"`
HOUR=`date -d -1hour +%H`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.ad_hoc_function.HadoopDemo

output_path=/mroutput/dongjiahao/HadoopDemo/log_date=$ONE_DAY_AGO
original_bid_log=/user/ads/log/original_bid/log_date=$ONE_DAY_AGO
specific_session_id=/mroutput/dongjiahao/file/session_ids.txt

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $original_bid_log $specific_session_id




