#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-6 day"`
#HOUR=`date -d -1hour +%H`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.temp.CheckSpecificOrderDomainImpClk

output_path=/mroutput/dongjiahao/CheckSpecificOrderDomainImpClk/log_date=$ONE_DAY_AGO
clk_log=/user/ads/log/original_click/log_date=$ONE_DAY_AGO
impression_log=/user/ads/log/original_show/log_date=$ONE_DAY_AGO
bid_log=/user/ads/log/original_bid/log_date=$ONE_DAY_AGO

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $clk_log $impression_log $bid_log




