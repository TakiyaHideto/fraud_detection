#!/usr/bin/env bash

#for i in {14..30}
#do
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#
#jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
#jar_name=hadoop_test-1.0-jar-with-dependencies.jar
#
#class_name=com.mr.ad_hoc_function.CollectFrdDecDataForJiaoda
#
#output_path=/mroutput/dongjiahao/CollectFrdDecDataForJiaoda/log_date=$ONE_DAY_AGO
#basedata_pc_path=/share_data/new_basedata/log_date=$ONE_DAY_AGO
#domain=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/domain_index
#host=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/host_index
#gender=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/gender_index
#tag=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/tag_index
#ip=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/ip_index
#
#hadoop fs -rmr -skipTrash $output_path
#hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata_pc_path $domain $host $gender $tag $ip $ONE_DAY_AGO
#done

for i in {14..30}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.ad_hoc_function.CollectFrdDecDataForJiaoda

output_path=/mroutput/dongjiahao/CollectFrdDecDataForJiaoda/log_date=$ONE_DAY_AGO
basedata_pc_path=/share_data/new_basedata/log_date=$ONE_DAY_AGO
#domain=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/domain_index
#host=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/host_index
#gender=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/gender_index
#tag=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/tag_index
#ip=/share_data/cooperation_project/jiaoda_frd_det/feat_id_index/ip_index

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata_pc_path
done