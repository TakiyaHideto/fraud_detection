#!/usr/bin/env bash

#for i in {14..30}
#do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-4 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.ad_hoc_function.CollectCpDataForJiaoda

output_path=/mroutput/dongjiahao/CollectCpDataForJiaoda/log_date=$ONE_DAY_AGO
basedata=/user/ads/mid/user/day/userExtract
truePath=/mroutput/dongjiahao/cp_device/log_date=${ONE_DAY_AGO}/baidu_cp_jiaoda_${ONE_DAY_AGO}
falsePath=/mroutput/dongjiahao/cp_device/log_date=${ONE_DAY_AGO}/random_cp_jiaoda_${ONE_DAY_AGO}

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata $truePath $falsePath $ONE_DAY_AGO
#done

