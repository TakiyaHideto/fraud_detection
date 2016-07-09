#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
#ONE_DAY_AGO="2016-03-08"

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.CheckAdzoneClickVariance

output_path=/mroutput/dongjiahao/CheckAdzoneClickVariance/log_date=$ONE_DAY_AGO
new_basedata_path=/share_data/new_basedata

#hadoop fs -rmr -skipTrash $output_path
#hadoop jar $jar_dir/$jar_name $class_name $output_path $new_basedata_path $ONE_DAY_AGO

original_adzone_file=/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/original_adzone_file_${ONE_DAY_AGO}
hadoop fs -text ${output_path}/part* > ${original_adzone_file}

macro_ctr=`cat ${original_adzone_file} | awk '{impression+=$3;click+=$4} END {print click/impression}'`
mean_date_clk=`cat ${original_adzone_file} | awk '{sum+=$6} END {print sum/NR}'`
mean_user_clk=`cat ${original_adzone_file} | awk '{sum+=$9} END {print sum/NR}'`

echo "marcro ctr:'$macro_ctr'"
echo "mean date clk:'${mean_date_clk}'"
echo "mean user clk:'${mean_user_clk}'"

python /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/data_analysis_python/AdzoneRank.py $ONE_DAY_AGO $macro_ctr $mean_date_clk $mean_user_clk "adzone"

adzone_rank_file="/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/adzone_rank_${ONE_DAY_AGO}"
adzone_rank_file_for_domain="/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/adzone_rank_file_for_domain_${ONE_DAY_AGO}"
cat ${adzone_rank_file} | awk '{if($3>50) print $0}' | sort -k 12 -g -r | awk '{print NR"\t"$0}' > ${adzone_rank_file_for_domain}

python /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/data_analysis_python/AdzoneRank.py $ONE_DAY_AGO $macro_ctr $mean_date_clk $mean_user_clk "domain"

domain_rank_file="/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/domain_rank_${ONE_DAY_AGO}"
cat ${domain_rank_file} | sort -k 2 -g -r | awk '{print NR"\t"$0}' > temp
cat temp > ${domain_rank_file}
rm temp