#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.crossplatform.GenerateImitatedCpDevice

output_path=/mroutput/dongjiahao/GenerateImitatedCpDevice/log_date=$ONE_DAY_AGO
baidu_cp=/user/ads/mid/user/day/userExtract

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $baidu_cp $ONE_DAY_AGO

cp_device_dir_cluster=/mroutput/dongjiahao/cp_device
hadoop fs -mkdir $cp_device_dir_cluster
cp_device_dir_cluster=/mroutput/dongjiahao/cp_device/log_date=$ONE_DAY_AGO
hadoop fs -mkdir $cp_device_dir_cluster
hadoop fs -rm ${cp_device_dir_cluster}/random*
cp_device_dir_local=/data/dongjiahao/svn_project/fraudDetection/file/cp_device/log_date=$ONE_DAY_AGO
mkdir $cp_device_dir_local
random_cp_file=${cp_device_dir_local}/random_cp_$ONE_DAY_AGO
hadoop fs -text ${output_path}/part* > $random_cp_file
hadoop fs -put $random_cp_file $cp_device_dir_cluster

#random_cp_drawbridge_file=${cp_device_dir_local}/random_cp_drawbridge_$ONE_DAY_AGO
#random_cp_jiaoda_file=${cp_device_dir_local}/random_cp_jiaoda_$ONE_DAY_AGO
#
#cat ${random_cp_file} | awk 'NR%100==1{print $0}' > $random_cp_drawbridge_file
#cat ${random_cp_file} | awk 'NR%5==1{print $0}' > $random_cp_jiaoda_file
#hadoop fs -put ${random_cp_drawbridge_file} $cp_device_dir_cluster
#hadoop fs -put ${random_cp_jiaoda_file} $cp_device_dir_cluster
