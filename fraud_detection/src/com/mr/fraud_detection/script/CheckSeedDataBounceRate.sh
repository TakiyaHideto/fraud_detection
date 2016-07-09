#!/usr/bin/env bash



#for i in {1..7}
#do
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#hadoop dfs -mkdir /share_data/dmp_monitor
#hadoop dfs -mkdir /share_data/dmp_monitor/log_date=$ONE_DAY_AGO
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#hadoop dfs -mkdir /share_data/dmp_monitor
#hadoop dfs -mkdir /share_data/dmp_monitor/log_date=$ONE_DAY_AGO
#source_dir=/user/ads/mid/dmp/access/$ONE_DAY_AGO
#destination_dir=/share_data/dmp_monitor/log_date=$ONE_DAY_AGO
#hadoop dfs -rmr $destination_dir
#hadoop distcp -update -skipcrccheck hftp://m1.data.yoyi:50070/$source_dir $destination_dir
#hadoop dfs -rmr /share_data/dmp_monitor/log_date=$ONE_DAY_AGO/_*
#done

rm /data/dongjiahao/svn_project/fraudDetection/file/bounce_rate/result

for i in {1..20}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.CheckSeedDataBounceRate

output_path=/mroutput/dongjiahao/CheckSeedDataBounceRate/log_date=$ONE_DAY_AGO
fraud_basedata_path=/share_data/fraud_detection/data/fraud_collection/log_date=$ONE_DAY_AGO
normal_basedata_path=/share_data/fraud_detection/data/normal_wasu_collection/log_date=$ONE_DAY_AGO
normal_basedata_baidu_cp_path=/share_data/fraud_detection/data/normal_baidu_cp_collection/log_date=$ONE_DAY_AGO
monitorData=/user/ads/mid/dmp/access/${ONE_DAY_AGO}

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $fraud_basedata_path $normal_basedata_path $normal_basedata_baidu_cp_path $monitorData $ONE_DAY_AGO

bounce_rate_dir=/data/dongjiahao/svn_project/fraudDetection/file/bounce_rate

all_monitor_data=/data/dongjiahao/svn_project/fraudDetection/file/bounce_rate/bouncerate_$ONE_DAY_AGO
fraud_data=/data/dongjiahao/svn_project/fraudDetection/file/bounce_rate/fraud_$ONE_DAY_AGO
normal_data=/data/dongjiahao/svn_project/fraudDetection/file/bounce_rate/normal_$ONE_DAY_AGO
hadoop dfs -text  ${output_path}/part* > $all_monitor_data
cat ${all_monitor_data} | awk -F"\001" '{if($2=="1") print $3}' > $fraud_data
cat ${all_monitor_data} | awk -F"\001" '{if($2=="0") print $3}' > $normal_data
echo $ONE_DAY_AGO >> ${bounce_rate_dir}/result
python /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/data_analysis_python/calBounceRate.py $fraud_data $normal_data >> ${bounce_rate_dir}/result
done

