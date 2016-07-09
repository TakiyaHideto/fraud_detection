#!/usr/bin/env bash

#for i in {1..7}
#do
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#
#echo ONE_DAY_AGO=$ONE_DAY_AGO
##
#jar_dir=/home/algo/svn/fraudDetection/trunk/target
#jar_name=hadoop_test-1.0-jar-with-dependencies.jar
#
#class_name=com.mr.merge_data.CollectFraudSeedData
#
#ipSetFilePath=/share_data/fraud_detection/seed/fraud_robot/log_date=${ONE_DAY_AGO}/robot_fraud_extracted_${ONE_DAY_AGO}
#adzoneIdSetFilePath=/share_data/fraud_detection_seed/fraud_seed/adzoneIdBL
#domainSetFilePath=/share_data/fraud_detection_seed/fraud_seed/domainBL
#yoyiCookieSetFilePath=/share_data/fraud_detection_seed/fraud_seed/yoyiCookieBL
#new_basedata_path=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
#output_path=/share_data/fraud_detection/data/fraud_collection/log_date=${ONE_DAY_AGO}
#
#hadoop dfs -rmr -skipTrash $output_path
#hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $ipSetFilePath $adzoneIdSetFilePath $domainSetFilePath $yoyiCookieSetFilePath $ONE_DAY_AGO
#hadoop dfs -rmr -skipTrash ${output_path}/_*
#done

START_DATE=$1
END_DATE=$2

while [ $START_DATE != $END_DATE ]
do
    echo $START_DATE

    jar_dir=/home/algo/svn/fraudDetection/trunk/target
    jar_name=hadoop_test-1.0-jar-with-dependencies.jar

    class_name=com.mr.merge_data.CollectFraudSeedData

    ipSetFilePath=/share_data/fraud_detection/seed/fraud_robot/log_date=${START_DATE}/robot_fraud_extracted_${START_DATE}
    adzoneIdSetFilePath=/share_data/fraud_detection_seed/fraud_seed/adzoneIdBL
    domainSetFilePath=/share_data/fraud_detection_seed/fraud_seed/domainBL
    yoyiCookieSetFilePath=/share_data/fraud_detection_seed/fraud_seed/yoyiCookieBL
    new_basedata_path=/share_data/new_basedata/log_date=${START_DATE}
    output_path=/share_data/fraud_detection/data/fraud_collection/log_date=${START_DATE}

    hadoop dfs -rmr -skipTrash $output_path
    hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $ipSetFilePath $adzoneIdSetFilePath $domainSetFilePath $yoyiCookieSetFilePath $START_DATE
    hadoop dfs -rmr -skipTrash ${output_path}/_*
    START_DATE=$(date -d "$START_DATE +1 days" "+%Y-%m-%d")
done