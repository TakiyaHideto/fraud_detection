#!/usr/bin/env bash

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.assessibility.AttachThirdPartyCookieTagCHANGRONG

output_path=/mroutput/dongjiahao/new_basedata/CHANGRONG_tag_data/
CHANGRONG_tag_data_path=/mroutput/dongjiahao/new_basedata/CHANGRONG_deviceId

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $CHANGRONG_tag_data_path