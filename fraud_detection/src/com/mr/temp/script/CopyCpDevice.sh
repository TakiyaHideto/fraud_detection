#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
#HOUR=`date -d -1hour +%H`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.temp.CopyCpDevice

output_path=/share_data/cross_platform/log_date=$ONE_DAY_AGO
original_cp_device=/mroutput/dongjiahao/YoyiCpPart5CollectAndPredict

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $original_cp_device




