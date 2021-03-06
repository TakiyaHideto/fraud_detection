#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-3 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.data_analysis_java.CheckIpLocation

bid_input=/share_data/new_mobile_basedata/log_date=${ONE_DAY_AGO}
output_path=/mroutput/dongjiahao/new_basedata/data_test_mobile_$ONE_DAY_AGO

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $bid_input