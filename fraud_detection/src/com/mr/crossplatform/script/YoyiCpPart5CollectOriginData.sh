#!/usr/bin/env bash

#for i in {14..30}
#do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.crossplatform.YoyiCpPart5CollectOriginData_v2

output_path=/mroutput/dongjiahao/YoyiCpPart5CollectOriginData/log_date=$ONE_DAY_AGO
basedata=/mroutput/dongjiahao/YoyiCpPart4ExtractData/log_date=$ONE_DAY_AGO
device_couple_device=/mroutput/dongjiahao/YoyiCpPart3ExcludeCp/log_date=$ONE_DAY_AGO/part-r-00000
dump=/mroutput/dongjiahao/file/dump.txt
feature_map=/mroutput/dongjiahao/file/feature_map

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata $device_couple_device $ONE_DAY_AGO
#done

