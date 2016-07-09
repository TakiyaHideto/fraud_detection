#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.CheckCookieImpClkVariance

output_path=/mroutput/dongjiahao/new_basedata/CheckCookieImpClkVariance/log_date=$ONE_DAY_AGO
bidLog=/user/ads/log/original_bid/log_date=$ONE_DAY_AGO

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $bidLog
