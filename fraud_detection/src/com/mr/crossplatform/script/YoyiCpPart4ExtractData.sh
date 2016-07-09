#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-7 day"`
ONE_DAY_AGO=$1

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.crossplatform.YoyiCpPart4ExtractData

output_path=/mroutput/dongjiahao/YoyiCpPart4ExtractData/log_date=$ONE_DAY_AGO
ip_file=/mroutput/dongjiahao_ads/AccumulateIp/log_date=${ONE_DAY_AGO}
domain_file=/mroutput/dongjiahao_ads/AccumulateDomain/log_date=${ONE_DAY_AGO}
crowd_tag_file=/mroutput/dongjiahao_ads/AccumulateCrowdTag/log_date=${ONE_DAY_AGO}
area_code_file=/mroutput/dongjiahao_ads/AccumulateAreaCode/log_date=${ONE_DAY_AGO}
app_type_file=/mroutput/dongjiahao_ads/AccumulateAppType/log_date=${ONE_DAY_AGO}
domain_type_file=/mroutput/dongjiahao_ads/AccumulateWebType/log_date=${ONE_DAY_AGO}
candidate_file=/mroutput/dongjiahao/YoyiCpPart3ExcludeCp/log_date=$ONE_DAY_AGO

/usr/bin/hadoop fs -rmr -skipTrash $output_path
/usr/bin/hadoop jar $jar_dir/$jar_name $class_name $output_path $ip_file $domain_file $crowd_tag_file $area_code_file $app_type_file $candidate_file $ONE_DAY_AGO
