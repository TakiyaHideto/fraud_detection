#!/usr/bin/env bash

#昨天的日期
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.data_analysis_java.CheckIpCfieldDomain

bid_input=/user/ads/log/original_bid/log_date=${ONE_DAY_AGO}
domainPath1=/mroutput/dongjiahao/new_basedata/data_2015-12-01/part-r-00000
domainPath2=/mroutput/dongjiahao/new_basedata/data_2015-12-01/part-r-00001
domainPath3=/mroutput/dongjiahao/new_basedata/data_2015-12-01/part-r-00002
domainPath4=/mroutput/dongjiahao/new_basedata/data_2015-12-01/part-r-00003
output_path=/mroutput/dongjiahao/new_basedata/data_test_$ONE_DAY_AGO

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $bid_input $domainPath1 $domainPath2 $domainPath3 $domainPath4


