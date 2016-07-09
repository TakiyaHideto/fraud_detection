#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.crossplatform.ExtractIpIntervalData

output_path=/mroutput/dongjiahao/ExtractIpIntervalData/log_date=$ONE_DAY_AGO
basedata=/user/ads/mid/user/day/userExtract
basedata=/user/ads/mid/user/day/userAccumulation/${ONE_DAY_AGO}/month/IP
ipEduPath=/mroutput/dongjiahao/file/ipEdu

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata $ipEduPath $ONE_DAY_AGO

