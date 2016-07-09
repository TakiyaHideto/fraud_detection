#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

old_cluster_data_dir=/share_data/fraud_detection_data/merge_data/log_date=${ONE_DAY_AGO}
hadoop dfs -mkdir /share_data/fraud_detection_data/merge_data/log_date=${ONE_DAY_AGO}
hadoop distcp -update -skipcrccheck hftp://m1.data.yoyi:50070/share_data/fraud_detection/data/data_merged_xgboost/log_date=${ONE_DAY_AGO} ${old_cluster_data_dir}/

frd_data_all=/data/dongjiahao/svn_project/xgboost/project/fraud_detection/frd_data_all
frd_data_sampled=/data/dongjiahao/svn_project/xgboost/project/fraud_detection/frd_data_sampled
hadoop dfs -text ${old_cluster_data_dir}/part* > $frd_data_all
neg_num=`cat $frd_data_all | awk -F" " '{if($1=="0") sum+=1} END {print sum}'`
pos_num=`cat $frd_data_all | awk -F" " '{if($1=="1") sum+=1} END {print sum}'`

python /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/data_analysis_python/sampleFraudData.py ${frd_data_all} ${frd_data_sampled} ${pos_num} ${neg_num}

feature_map=/data/dongjiahao/svn_project/xgboost/project/fraud_detection/feature_map
train_origin=/data/dongjiahao/svn_project/xgboost/project/fraud_detection/train
test_origin=/data/dongjiahao/svn_project/xgboost/project/fraud_detection/test
rm $feature_map
python /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/data_analysis_python/HandleOriginDataForXgboost.py $train_origin $feature_map
python /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/data_analysis_python/HandleOriginDataForXgboost.py $test_origin $feature_map

#../../xgboost/xgboost configuration.conf task=pred model_in=0010.model
#../../xgboost/xgboost configuration.conf task=dump model_in=0010.model name_dump=dump
#cat dump | awk '{print NR"\t"$0}' > temp