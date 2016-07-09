#!/usr/bin/env bash

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.assessibility.AttachThirdPartyCookieTagPGI


output_path=/mroutput/dongjiahao/new_basedata/PGI_tag_data/
PGI_tag_data_path=/mroutput/dongjiahao/new_basedata/PGI_cookies


hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $PGI_tag_data_path