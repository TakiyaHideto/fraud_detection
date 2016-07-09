#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.effect_analysis.ExtractDomainValuationData

output_path=/mroutput/dongjiahao/ExtractDomainValuationData/log_date=$ONE_DAY_AGO
databank_access=/user/ads/mid/dmp/access

/usr/bin/hadoop fs -rmr -skipTrash $output_path
/usr/bin/hadoop jar $jar_dir/$jar_name $class_name $output_path $databank_access $ONE_DAY_AGO

hadoop fs -text /mroutput/dongjiahao/ExtractDomainValuationData/log_date=$ONE_DAY_AGO/part* > temp
keep_time=`cat temp | awk '{sum+=$2} END {print sum/NR}'`
deep=`cat temp | awk '{sum+=$3} END {print sum/NR}'`
second_bounce=`cat temp | awk '{sum+=$4} END {print sum/NR}'`
keeptime_deep_ratio=`cat temp |  awk '{if($4=="1")sum1+=$2;sum2+=$3 } END {print sum1/sum2}'`

echo $keep_time
echo $deep
echo $second_bounce
echo $keeptime_deep_ratio

rm temp
