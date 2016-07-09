#!/usr/bin/env bash

rm /data/dongjiahao/svn_project/fraudDetection/file/bounce_rate/result

for i in {1..7}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#ONE_DAY_AGO=2016-02-22

hadoop dfs -mkdir /share_data/dmp_monitor
hadoop dfs -mkdir /share_data/dmp_monitor/log_date=$ONE_DAY_AGO
source_dir=/user/ads/mid/dmp/access/$ONE_DAY_AGO
destination_dir=/share_data/dmp_monitor/log_date=$ONE_DAY_AGO
hadoop dfs -rmr $destination_dir
hadoop distcp -update -skipcrccheck hftp://m1.data.yoyi:50070/$source_dir $destination_dir
hadoop dfs -rmr /share_data/dmp_monitor/log_date=$ONE_DAY_AGO/_*

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.CheckFraudDetectionBounceRate

output_path=/mroutput/dongjiahao/CheckFraudDetectionBounceRate/log_date=$ONE_DAY_AGO
#basedata=/share_data/new_basedata/log_date=$ONE_DAY_AGO
basedata=/mroutput/dongjiahao/AddFraudProbTagForBasedata/log_date=${ONE_DAY_AGO}
monitorData=/share_data/dmp_monitor/log_date=$ONE_DAY_AGO
threshold_frd=0.7
threshold_nml=0.3

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata $monitorData $threshold_frd $threshold_nml

bounce_rate_dir=/data/dongjiahao/svn_project/fraudDetection/file/bounce_rate
mkdir $bounce_rate_dir

all_monitor_data=/data/dongjiahao/svn_project/fraudDetection/file/bounce_rate/bouncerate_$ONE_DAY_AGO
fraud_data=/data/dongjiahao/svn_project/fraudDetection/file/bounce_rate/fraud_$ONE_DAY_AGO
normal_data=/data/dongjiahao/svn_project/fraudDetection/file/bounce_rate/normal_$ONE_DAY_AGO
hadoop dfs -text  ${output_path}/part* > $all_monitor_data
cat ${all_monitor_data} | awk -F"\001" '{if($2=="1") print $3}' > $fraud_data
cat ${all_monitor_data} | awk -F"\001" '{if($2=="0") print $3}' > $normal_data
echo $ONE_DAY_AGO >> ${bounce_rate_dir}/result
python /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/data_analysis_python/calBounceRate.py $fraud_data $normal_data >> ${bounce_rate_dir}/result

done
