#!/usr/bin/env bash

for i in {4,11}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`

dir_local=/data/dongjiahao/svn_project/fraudDetection/file/frd_det_data/log_date=${ONE_DAY_AGO}
mkdir $dir_local

fraud_data_local=${dir_local}/fraud_data_robot_$ONE_DAY_AGO
normal_data_baidu_local=${dir_local}/normal_data_baidu_$ONE_DAY_AGO
normal_data_wasu_local=${dir_local}/normal_data_wasu_$ONE_DAY_AGO

hadoop fs -text /share_data/fraud_detection/data/normal_baidu_cp_collection/log_date=${ONE_DAY_AGO} > $normal_data_baidu_local
hadoop fs -text /share_data/fraud_detection/data/fraud_collection/log_date=${ONE_DAY_AGO} > $fraud_data_local
hadoop fs -text /share_data/fraud_detection/data/normal_wasu_collection/log_date=${ONE_DAY_AGO} > $normal_data_wasu_local

done