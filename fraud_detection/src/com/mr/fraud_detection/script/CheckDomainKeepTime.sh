#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.CheckDomainKeepTime

output_path=/mroutput/dongjiahao/CheckDomainKeepTime/log_date=$ONE_DAY_AGO
dmp_data=/user/ads/mid/dmp/access

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $dmp_data $ONE_DAY_AGO
