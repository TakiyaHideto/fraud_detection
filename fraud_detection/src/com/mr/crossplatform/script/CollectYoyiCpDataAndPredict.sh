#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
#FOUR_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.crossplatform.CollectYoyiCpDataAndPredict

#output_path=/mroutput/dongjiahao/CollectYoyiCpDataAndPredict/log_date=$ONE_DAY_AGO
#basedata_accu=/user/ads/mid/user/day/userAccumulation
#basedata=/user/ads/mid/user/day/userExtract
#basedata=/mroutput/dongjiahao/CollectCpOriginDataForPredicting/log_date=$ONE_DAY_AGO
#device_couple_device=/mroutput/dongjiahao/CrossPlatformBasicJoiningMultiDay/log_date=$ONE_DAY_AGO/part-r-00000

output_path=/mroutput/dongjiahao/YoyiCpPart5CollectOriginData/log_date=$ONE_DAY_AGO
basedata=/mroutput/dongjiahao/YoyiCpPart4ExtractData/log_date=$ONE_DAY_AGO
device_couple_device=/mroutput/dongjiahao/YoyiCpPart3ExcludeCp/log_date=$ONE_DAY_AGO/part-r-00000
dump=/mroutput/dongjiahao/file/dump.txt
feature_map=/mroutput/dongjiahao/file/feature_map

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata $device_couple_device $dump $feature_map $ONE_DAY_AGO

output_path=/mroutput/dongjiahao/YoyiCpPart5CollectOriginData/log_date=$ONE_DAY_AGO
basedata=/mroutput/dongjiahao/YoyiCpPart4ExtractData/log_date=$ONE_DAY_AGO
device_couple_device=/mroutput/dongjiahao/YoyiCpPart3ExcludeCp/log_date=$ONE_DAY_AGO/part-r-00000
