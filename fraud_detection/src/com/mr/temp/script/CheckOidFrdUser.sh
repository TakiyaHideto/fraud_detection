#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-0 day"`
#HOUR=`date -d -1hour +%H`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.temp.CheckOidFrdUser

output_path=/mroutput/dongjiahao/CheckOidFrdUser/log_date=$ONE_DAY_AGO
clk_log=/user/ads/log/original_click/log_date=$ONE_DAY_AGO
impression_log=/user/ads/log/original_show/log_date=$ONE_DAY_AGO
start_time=18
stop_time=21
cid=567
oid=17478


hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $clk_log $impression_log $start_time $stop_time $cid $oid




