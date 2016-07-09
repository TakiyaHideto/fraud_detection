#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.effect_analysis.EvaluateDomainOffline

#hadoop fs -put /data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/domain_rank_2016-05-08 /mroutput/dongjiahao/file/

output_path=/mroutput/dongjiahao/EvaluateDomainOffline/log_date=$ONE_DAY_AGO
new_basedata=/share_data/new_basedata/log_date=${ONE_DAY_AGO}
databank=/mroutput/dongjiahao/ExtractDomainValuationData/log_date=$ONE_DAY_AGO
domain_file=/mroutput/dongjiahao/file/domain_list
index=domain

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $new_basedata $databank $domain_file $index

hadoop fs -text /mroutput/dongjiahao/EvaluateDomainOffline/log_date=$ONE_DAY_AGO/part* > temp
keep_time=`cat temp | awk '{sum+=$2} END {print sum/NR}'`
deep=`cat temp | awk '{sum+=$3} END {print sum/NR}'`
second_bounce=`cat temp | awk '{sum+=$4} END {print sum/NR}'`
keeptime_deep_ratio=`cat temp |  awk '{if($4=="1")sum1+=$2;sum2+=$3 } END {print sum1/sum2}'`

echo $keep_time
echo $deep
echo $second_bounce
echo $keeptime_deep_ratio

rm temp
