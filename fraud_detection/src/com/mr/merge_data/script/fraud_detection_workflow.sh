#!/usr/bin/env bash

#ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
START_DATE=`date +%Y-%m-%d --date="-8 day"`
END_DATE=`date +%Y-%m-%d --date="-2 day"`
data_merged_hdfs_dir=/share_data/fraud_detection/data/data_merged_xgboost/log_date=${END_DATE}
data_merged_local_dir=/home/algo/svn/fraudDetection/file/robot_fraud/log_date=${END_DATE}
data_training=/home/algo/svn/fraudDetection/file/robot_fraud/training
date_testing=/home/algo/svn/fraudDetection/file/robot_fraud/testing
file_hdfs_dir=/mroutput/algo/file

#sh /home/algo/svn/fraudDetection/trunk/src/com/mr/fraud_detection/script/ExploreRobotFraud.sh $START_DATE $END_DATE
#sh /home/algo/svn/fraudDetection/trunk/src/com/mr/merge_data/script/CollectFraudSeedData.sh $START_DATE $END_DATE
#sh /home/algo/svn/fraudDetection/trunk/src/com/mr/merge_data/script/CollectNormalSeedDataFromBaiduCP.sh $START_DATE $END_DATE
#sh /home/algo/svn/fraudDetection/trunk/src/com/mr/merge_data/script/CollectNormalSeedData.sh $START_DATE $END_DATE

#sh /home/algo/svn/fraudDetection/trunk/src/com/mr/feature_engineering/script/FeatureEngineeringForFrd.sh $START_DATE $END_DATE
hadoop fs -text /mroutput/algo/FeatureEngineeringForFrd/log_date=${END_DATE}/part* | awk '{print NR"\001"$0}' > ${data_merged_local_dir}/feature_map
hadoop fs -rm ${file_hdfs_dir}/feature_map
hadoop fs -put ${data_merged_local_dir}/feature_map ${file_hdfs_dir}/feature_map

sh /home/algo/svn/fraudDetection/trunk/src/com/mr/feature_engineering/script/OutputNewFormRecords.sh $END_DATE ${file_hdfs_dir}/feature_map

## 下载 OutputNewFormRecords.sh 产出的数据至本地
#mkdir ${data_merged_local_dir}
#hadoop fs -text ${data_merged_hdfs_dir}/part* > ${data_merged_local_dir}/data_merged
#fraud_count=`cat ${data_merged_local_dir}/data_merged | awk '{if($1==1) print $0}' | wc -l`
#normal_count=`cat ${data_merged_local_dir}/data_merged | awk '{if($1==0) print $0}' | wc -l`
#echo $fraud_count
#echo $normal_count
#
### 使用 http://yoyi.svn.yoyi.tv:8888/svn/Research/fraudDetection/trunk/src/com/mr/data_analysis_python/HandleOriginDataForXgboost.py 进行feature mapping
##feature_mapping_python_dir=/home/algo/svn/fraudDetection/trunk/src/com/mr/data_analysis_python
##python ${feature_mapping_python_dir}/HandleOriginDataForXgboost.py ${data_merged_local_dir}/data_merged ${data_merged_local_dir}/feature_map
#
## 使用xgboost训练模型，并dump出模型，目前使用LR模型
#cat ${data_merged_local_dir}/data_merged_XgFormat | awk '{if(NR%5==1) print $0}' > ${date_testing}
#cat ${data_merged_local_dir}/data_merged_XgFormat | awk '{if(NR%5!=1) print $0}' > ${data_training}
#xgboost_dir=/home/algo/xgboost
#conf_dir=/home/algo/svn/fraudDetection/conf
#model_name=frd_model.model
#${xgboost_dir}/xgboost ${conf_dir}/conf_frd.conf
#${xgboost_dir}/xgboost ${conf_dir}/conf_frd.conf task=dump model_in=${conf_dir}/$model_name name_dump=${conf_dir}/dump.txt
#
## 上传训练好的LR模型dump文件和feature_mapping文件至HDFS（最后一步AddFraudProbTagForBasedata.sh会使用）
#file_hdfs_dir=/mroutput/algo/file
#hadoop fs -rm ${file_hdfs_dir}/dump.txt
#hadoop fs -put ${conf_dir}/dump.txt ${file_hdfs_dir}/dump.txt
#
#sh /home/algo/svn/fraudDetection/trunk/src/com/mr/merge_data/script/AddFraudProbTagForBasedata.sh $START_DATE $END_DATE