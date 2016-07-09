#!/usr/bin/env bash

hadoop fs -rmr /share_data/cooperation_project/jiaoda_frd_det/monitor_data

for i in {11..25}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
hadoop fs -mkdir /share_data/cooperation_project/jiaoda_frd_det/monitor_data
hadoop fs -rmr /share_data/cooperation_project/jiaoda_frd_det/monitor_data/log_date=${ONE_DAY_AGO}
hadoop fs -mkdir /share_data/cooperation_project/jiaoda_frd_det/monitor_data/log_date=${ONE_DAY_AGO}
hadoop fs -text /user/ads/mid/dmp/access/${ONE_DAY_AGO}/* > temp
cat temp | awk -F"\001" '{if(($2~/-$/ && length($2)==33) || ($2~/6$/ && length($2)==21)) print $1"\t"$2"\t"$18}' > monitor_data_$ONE_DAY_AGO
hadoop fs -put monitor_data_$ONE_DAY_AGO /share_data/cooperation_project/jiaoda_frd_det/monitor_data/log_date=${ONE_DAY_AGO}
rm temp
#rm monitor_data_$ONE_DAY_AGO
done