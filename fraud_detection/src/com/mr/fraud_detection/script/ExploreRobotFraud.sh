#!/usr/bin/env bash

#for i in {1..7}
#do
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#
#jar_dir=/home/algo/svn/fraudDetection/trunk/target
#jar_name=hadoop_test-1.0-jar-with-dependencies.jar
#
#class_name=com.mr.fraud_detection.ExploreRobotFraud
#
#output_path=/mroutput/algo/ExploreRobotFraud/log_date=$ONE_DAY_AGO
#new_basedata_path=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
#
#hadoop fs -rmr -skipTrash $output_path
#hadoop jar $jar_dir/$jar_name $class_name $output_path $new_basedata_path $ONE_DAY_AGO
#
#robot_fraud_dir=/home/algo/svn/fraudDetection/file/robot_fraud/log_date=$ONE_DAY_AGO
#mkdir $robot_fraud_dir
#robot_fraud_file=/home/algo/svn/fraudDetection/file/robot_fraud/log_date=${ONE_DAY_AGO}/robot_fraud_$ONE_DAY_AGO
#robot_fraud_file_extracted=/home/algo/svn/fraudDetection/file/robot_fraud/log_date=${ONE_DAY_AGO}/robot_fraud_extracted_$ONE_DAY_AGO
#hadoop fs -text /mroutput/algo/ExploreRobotFraud/log_date=$ONE_DAY_AGO/part* > $robot_fraud_file
#rm $robot_fraud_file_extracted
#
#python /home/algo/svn/fraudDetection/trunk/src/com/mr/data_analysis_python/BlackListIpWithInfoNew.py $robot_fraud_file $robot_fraud_file_extracted
#
#robot_fraud_dir_cluster=/share_data/fraud_detection/seed/fraud_robot/log_date=$ONE_DAY_AGO
#hadoop fs -rmr ${robot_fraud_dir_cluster}
#hadoop fs -mkdir $robot_fraud_dir_cluster
#hadoop fs -put $robot_fraud_file_extracted $robot_fraud_dir_cluster
#done

START_DATE=$1
END_DATE=$2

while [ $START_DATE != $END_DATE ]
do
    echo $START_DATE
    jar_dir=/home/algo/svn/fraudDetection/trunk/target
    jar_name=hadoop_test-1.0-jar-with-dependencies.jar

    class_name=com.mr.fraud_detection.ExploreRobotFraud

    output_path=/mroutput/algo/ExploreRobotFraud/log_date=$START_DATE
    new_basedata_path=/share_data/new_basedata/log_date=${START_DATE}

    hadoop fs -rmr -skipTrash $output_path
    hadoop jar $jar_dir/$jar_name $class_name $output_path $new_basedata_path $START_DATE

    robot_fraud_dir=/home/algo/svn/fraudDetection/file/robot_fraud/log_date=$START_DATE
    mkdir $robot_fraud_dir
    robot_fraud_file=/home/algo/svn/fraudDetection/file/robot_fraud/log_date=${START_DATE}/robot_fraud_$START_DATE
    robot_fraud_file_extracted=/home/algo/svn/fraudDetection/file/robot_fraud/log_date=${START_DATE}/robot_fraud_extracted_$START_DATE
    hadoop fs -text /mroutput/algo/ExploreRobotFraud/log_date=$START_DATE/part* > $robot_fraud_file
    rm $robot_fraud_file_extracted

    python /home/algo/svn/fraudDetection/trunk/src/com/mr/data_analysis_python/BlackListIpWithInfoNew.py $robot_fraud_file $robot_fraud_file_extracted

    robot_fraud_dir_cluster=/share_data/fraud_detection/seed/fraud_robot/log_date=$START_DATE
    hadoop fs -rmr ${robot_fraud_dir_cluster}
    hadoop fs -mkdir $robot_fraud_dir_cluster
    hadoop fs -put $robot_fraud_file_extracted $robot_fraud_dir_cluster
    START_DATE=$(date -d "$START_DATE +1 days" "+%Y-%m-%d")
done

