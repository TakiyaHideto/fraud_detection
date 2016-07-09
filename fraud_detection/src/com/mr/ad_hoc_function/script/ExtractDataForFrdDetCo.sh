#!/usr/bin/env bash

for i in {14..15}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.ad_hoc_function.ExtractDataForFrdDetCo

output_path=/mroutput/dongjiahao/ExtractDataForFrdDetCo/log_date=$ONE_DAY_AGO
basedata_path=/share_data/new_basedata/log_date=$ONE_DAY_AGO
monitor_data=/user/ads/mid/dmp/access/$ONE_DAY_AGO

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata_path $monitor_data

data_dir_local=/data/dongjiahao/svn_project/fraudDetection/file/frd_det_co_project/log_date=$ONE_DAY_AGO
all_data_local=${data_dir_local}/all_data_$ONE_DAY_AGO
monitor_data_local=${data_dir_local}/monitor_data_$ONE_DAY_AGO
base_data_local=${data_dir_local}/base_data_$ONE_DAY_AGO
mkdir $data_dir_local
hadoop fs -text ${output_path}/part* > $all_data_local
cat $all_data_local | awk -F"\t" '{if($1=="basedata") print $2}' > $base_data_local
cat $all_data_local | awk -F"\t" '{if($1=="monitor") print $2}' > $monitor_data_local

done



