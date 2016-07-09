#!/usr/bin/env bash


#for i in {1..1}
#do
i=$1
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
ONE_DAY_AGO=$1

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.merge_data.ExtractWasuInfo

output_path=/mroutput/dongjiahao/ExtractWasuInfo/log_date=$ONE_DAY_AGO
log_1=/mroutput/OTT_project/logs/cross_panel/112
log_2=/mroutput/OTT_project/logs/cross_panel/191
log_3=/mroutput/OTT_project/logs/cross_panel/226
current_date=$ONE_DAY_AGO

/usr/bin/hadoop fs -rm -r -skipTrash $output_path
/usr/bin/hadoop jar $jar_dir/$jar_name $class_name $output_path $log_1 $log_2 $log_3 $current_date
#done