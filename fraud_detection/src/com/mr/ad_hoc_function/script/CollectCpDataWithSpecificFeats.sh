#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.ad_hoc_function.CollectCpDataWithSpecificFeats

output_path=/mroutput/dongjiahao/CollectCpDataWithSpecificFeats/log_date=$ONE_DAY_AGO
#basedata_pc_path=/share_data/new_basedata
#basedata_mobile_path=/share_data/new_mobile_basedata
basedata_pc_path=/share_data/new_basedata
basedata_mobile_path=/share_data/new_mobile_basedata
trueData=/mroutput/dongjiahao/cp_device/log_date=${ONE_DAY_AGO}/baidu_cp_drawbridge_${ONE_DAY_AGO}
falseData=/mroutput/dongjiahao/cp_device/log_date=${ONE_DAY_AGO}/random_cp_drawbridge_${ONE_DAY_AGO}

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata_pc_path $basedata_mobile_path $trueData $falseData $ONE_DAY_AGO



