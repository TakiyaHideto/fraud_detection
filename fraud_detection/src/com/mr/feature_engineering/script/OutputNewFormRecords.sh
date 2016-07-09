#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
ONE_DAY_AGO=$1
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/home/algo/svn/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.feature_engineering.OutputNewFormRecords

fraud_basedata_path=/share_data/fraud_detection/data/fraud_collection
normal_basedata_path=/share_data/fraud_detection/data/normal_wasu_collection
normal_basedata_baidu_cp_path=/share_data/fraud_detection/data/normal_baidu_cp_collection
feature_map=$2

output_path=/share_data/fraud_detection/data/data_merged_xgboost/log_date=${ONE_DAY_AGO}

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $fraud_basedata_path $normal_basedata_path $normal_basedata_baidu_cp_path $feature_map $ONE_DAY_AGO
hadoop dfs -rmr -skipTrash ${output_path}/_logs