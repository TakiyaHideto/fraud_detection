#!/usr/bin/env bash

#for i in {30..80}
#do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
ONE_DAY_AGO=$1

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.crossplatform.YoyiCpPart1JoiningSingleDay

output_path=/mroutput/dongjiahao/YoyiCpPart1JoiningSingleDay/log_date=$ONE_DAY_AGO
bid_log=/user/ads/mid/user/day/userExtract/${ONE_DAY_AGO}/IP
/usr/bin/hadoop fs -rmr -skipTrash $output_path
/usr/bin/hadoop jar $jar_dir/$jar_name $class_name $output_path $bid_log $ONE_DAY_AGO
#done