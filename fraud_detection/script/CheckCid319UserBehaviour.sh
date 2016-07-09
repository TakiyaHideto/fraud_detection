#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.CheckCid319UserBehaviour

output_path=/mroutput/dongjiahao/new_basedata/CheckCid319UserBehaviour/log_date=$ONE_DAY_AGO
cid319_data=/mroutput/dongjiahao/new_basedata/cid319Cookie/cid319
bidLog=/user/ads/log/original_bid/log_date=$ONE_DAY_AGO

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $bidLog $cid319_data