#!/usr/bin/env bash

hadoop dfs -rm /share_data/blacklist_filter/ipBL
hadoop dfs -put /data/dongjiahao/svnProject/fraudDetection/hive_result/ipBL /share_data/blacklist_filter/ipBL

for i in {23..23}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.merge_data.AddFilterBlackListForBasedataMR

ipSetFilePath=/share_data/blacklist_filter/ipBL
adzoneIdSetFilePath=/share_data/blacklist_filter/adzoneIdBL
domainSetFilePath=/share_data/blacklist_filter/domainBL
yoyiCookieSetFilePath=/share_data/blacklist_filter/yoyiCookieBL
new_basedata_path=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
output_path=/share_data/new_basedata_blacklist/log_date=${ONE_DAY_AGO}

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $new_basedata_path $ipSetFilePath $adzoneIdSetFilePath $domainSetFilePath $yoyiCookieSetFilePath
hadoop dfs -rmr -skipTrash ${output_path}/_logs
done