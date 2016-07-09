#!/usr/bin/env bash

#for i in {1..7}
#do
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#echo ONE_DAY_AGO=$ONE_DAY_AGO
#
#jar_dir=/home/algo/svn/fraudDetection/trunk/target
#jar_name=hadoop_test-1.0-jar-with-dependencies.jar
#
#class_name=com.mr.merge_data.CollectNormalSeedDataFromBaiduCP
#
#output_path=/share_data/fraud_detection/data/normal_baidu_cp_collection/log_date=${ONE_DAY_AGO}
#new_basedata_path=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
#baidu_cp_data=/user/ads/mid/user/day/userExtract/${ONE_DAY_AGO}/BaiduCP
#
#hadoop dfs -rmr -skipTrash $output_path
#hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $baidu_cp_data $ONE_DAY_AGO
#hadoop dfs -rmr -skipTrash ${output_path}/_logs
#done

START_DATE=$1
END_DATE=$2

while [ $START_DATE != $END_DATE ]
do
    echo $START_DATE

    jar_dir=/home/algo/svn/fraudDetection/trunk/target
    jar_name=hadoop_test-1.0-jar-with-dependencies.jar

    class_name=com.mr.merge_data.CollectNormalSeedDataFromBaiduCP

    output_path=/share_data/fraud_detection/data/normal_baidu_cp_collection/log_date=${START_DATE}
    new_basedata_path=/share_data/new_basedata/log_date=${START_DATE}
    baidu_cp_data=/user/ads/mid/user/day/userExtract/${START_DATE}/BaiduCP

    hadoop dfs -rmr -skipTrash $output_path
    hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $baidu_cp_data $START_DATE
    hadoop dfs -rmr -skipTrash ${output_path}/_logs
    START_DATE=$(date -d "$START_DATE +1 days" "+%Y-%m-%d")
done