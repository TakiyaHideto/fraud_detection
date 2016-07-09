#!/usr/bin/env bash

#for i in {1..7}
#do
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#SPECIFIC_DAY=`date +%Y-%m-%d --date="-1 day"`
#echo ONE_DAY_AGO=$ONE_DAY_AGO
#
#jar_dir=/home/algo/svn/fraudDetection/trunk/target
#jar_name=hadoop_test-1.0-jar-with-dependencies.jar
#
#class_name=com.mr.merge_data.AddFraudProbTagForBasedata
#
#output_path=/mroutput/algo/AddFraudProbTagForBasedata/log_date=${ONE_DAY_AGO}
#new_basedata_path=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
#featureIdMapPath=/mroutput/algo/file/feature_map
#featureWeightMapPath=/mroutput/algo/file/dump.txt
#
#hadoop dfs -rmr -skipTrash $output_path
#hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $featureIdMapPath $featureWeightMapPath
#hadoop dfs -rmr -skipTrash ${output_path}/_logs
#done

START_DATE=$1
END_DATE=$2

while [ $START_DATE != $END_DATE ]
do
    echo $START_DATE

    jar_dir=/home/algo/svn/fraudDetection/trunk/target
    jar_name=hadoop_test-1.0-jar-with-dependencies.jar

    class_name=com.mr.merge_data.AddFraudProbTagForBasedata

    output_path=/mroutput/algo/AddFraudProbTagForBasedata/log_date=${START_DATE}
    new_basedata_path=/share_data/new_basedata/log_date=${START_DATE}
    featureIdMapPath=/mroutput/algo/file/feature_map
    featureWeightMapPath=/mroutput/algo/file/dump.txt

    hadoop dfs -rmr -skipTrash $output_path
    hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $featureIdMapPath $featureWeightMapPath
    hadoop dfs -rmr -skipTrash ${output_path}/_logs
    START_DATE=$(date -d "$START_DATE +1 days" "+%Y-%m-%d")
done