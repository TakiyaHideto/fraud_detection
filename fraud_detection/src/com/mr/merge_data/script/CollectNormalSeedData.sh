#!/usr/bin/env bash

#for i in {1..7}
#do
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#
#jar_dir=/home/algo/svn/fraudDetection/trunk/target
#jar_name=hadoop_test-1.0-jar-with-dependencies.jar
#
#class_name=com.mr.merge_data.CollectNormalSeedData
#
#output_path=/share_data/fraud_detection/data/normal_wasu_collection/log_date=${ONE_DAY_AGO}
#log_1=/mroutput/OTT_project/logs/cross_panel/112/log_date=${ONE_DAY_AGO}
#log_2=/mroutput/OTT_project/logs/cross_panel/191/log_date=${ONE_DAY_AGO}
#log_3=/mroutput/OTT_project/logs/cross_panel/226/log_date=${ONE_DAY_AGO}
#new_basedata_path=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
#
#hadoop dfs -rmr -skipTrash $output_path
#hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $log_1 $log_2 $log_3
#hadoop dfs -rmr -skipTrash ${output_path}/_logs
#done

START_DATE=$1
END_DATE=$2

while [ $START_DATE != $END_DATE ]
do
    echo $START_DATE

   jar_dir=/home/algo/svn/fraudDetection/trunk/target
    jar_name=hadoop_test-1.0-jar-with-dependencies.jar

    class_name=com.mr.merge_data.CollectNormalSeedData

    output_path=/share_data/fraud_detection/data/normal_wasu_collection/log_date=${START_DATE}
    log_1=/mroutput/OTT_project/logs/cross_panel/112/log_date=${START_DATE}
    log_2=/mroutput/OTT_project/logs/cross_panel/191/log_date=${START_DATE}
    log_3=/mroutput/OTT_project/logs/cross_panel/226/log_date=${START_DATE}
    new_basedata_path=/share_data/new_basedata/log_date=${START_DATE}

    hadoop dfs -rmr -skipTrash $output_path
    hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $log_1 $log_2 $log_3
    hadoop dfs -rmr -skipTrash ${output_path}/_logs
    START_DATE=$(date -d "$START_DATE +1 days" "+%Y-%m-%d")
done