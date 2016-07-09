#!/usr/bin/env bash

for i in {2..9}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.feature_engineering.OutputNewFormRecordsForBasedata

basedata_path=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
output_path=/share_data/fraud_detection_data/basedata/log_date=${ONE_DAY_AGO}

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $basedata_path
hadoop dfs -rmr -skipTrash ${output_path}/_logs
done