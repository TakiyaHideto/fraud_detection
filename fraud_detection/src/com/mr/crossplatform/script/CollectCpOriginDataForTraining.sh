#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

hadoop fs -text /mroutput/dongjiahao/GenerateTrueCpDevice/log_date=${ONE_DAY_AGO}/part* > true_cp_${ONE_DAY_AGO}
hadoop fs -text /mroutput/dongjiahao/GenerateImitatedCpDevice/log_date=${ONE_DAY_AGO}/part* > false_cp_${ONE_DAY_AGO}
hadoop fs -mkdir /share_data/cross_platform/log_date=${ONE_DAY_AGO}
hadoop fs -put true_cp_${ONE_DAY_AGO} /share_data/cross_platform/log_date=${ONE_DAY_AGO}/
hadoop fs -put false_cp_${ONE_DAY_AGO} /share_data/cross_platform/log_date=${ONE_DAY_AGO}/
rm true_cp_${ONE_DAY_AGO}
rm false_cp_${ONE_DAY_AGO}

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.crossplatform.CollectCpOriginData

output_path=/mroutput/dongjiahao/CollectCpOriginData/log_date=$ONE_DAY_AGO
basedata=/user/ads/mid/user/day/userExtract
truePath=/share_data/cross_platform/log_date=${ONE_DAY_AGO}/true_cp_${ONE_DAY_AGO}
falsePath=/share_data/cross_platform/log_date=${ONE_DAY_AGO}/false_cp_${ONE_DAY_AGO}

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata $truePath $falsePath $ONE_DAY_AGO
