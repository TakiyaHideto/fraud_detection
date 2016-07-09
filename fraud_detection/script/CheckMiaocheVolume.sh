#!/usr/bin/env bash

#昨天的日期
ONE_DAY_AGO=`date +%Y-%m-%d --date="-24 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.data_analysis_java.CheckMiaocheVolume

basedata=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
blDomainPath=/mroutput/dongjiahao/databank/blDomain.txt
cookiePath=/mroutput/dongjiahao/databank/miaoche.txt
output_path=/mroutput/dongjiahao/new_basedata/data_test_$ONE_DAY_AGO

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $basedata $blDomainPath $cookiePath