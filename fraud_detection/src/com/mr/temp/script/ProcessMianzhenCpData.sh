#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
#HOUR=`date -d -1hour +%H`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.temp.ProcessMianzhenCpData

output_path=/mroutput/dongjiahao/ProcessMianzhenCpData/log_date=$ONE_DAY_AGO
yoyiIpFile=/user/ads/mid/user/day/userExtract
miaozhenIpFile=/mroutput/dongjiahao/file/temp
yoyiAppFile=/user/ads/mid/user/day/userExtract

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $yoyiIpFile $miaozhenIpFile $yoyiAppFile $ONE_DAY_AGO




