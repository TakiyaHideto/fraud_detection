#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.temp.TagCookieNum

output_path=/mroutput/dongjiahao/TagCookieNum/log_date=$ONE_DAY_AGO
retargeting_data=/mroutput/ads/AT_data_merged/log_date=$ONE_DAY_AGO

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $retargeting_data




