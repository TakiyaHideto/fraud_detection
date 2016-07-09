#!/usr/bin/env bash

START_DATE=$1
END_DATE=$2

jar_dir=/home/algo/svn/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.feature_engineering.FeatureEngineeringForFrd

fraud_basedata_path=/share_data/fraud_detection/data/fraud_collection
normal_basedata_path=/share_data/fraud_detection/data/normal_wasu_collection
normal_basedata_baidu_cp_path=/share_data/fraud_detection/data/normal_baidu_cp_collection
output_path=/mroutput/algo/FeatureEngineeringForFrd/log_date=${END_DATE}

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $fraud_basedata_path $normal_basedata_baidu_cp_path $normal_basedata_path $START_DATE $END_DATE
hadoop dfs -rmr -skipTrash ${output_path}/_logs


