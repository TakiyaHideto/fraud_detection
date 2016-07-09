#!/usr/bin/env bash
for i in {2..8}
do
ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.assessibility.FetchDiyuanxinAddTag

#output_path=/share_data/diyuanxin_tag_data/log_date=$ONE_DAY_AGO
output_path=/mroutput/dongjiahao/new_basedata/diyuanxin_data_temp=$ONE_DAY_AGO
diyuanxi_data_path=/share_data/diyuanxin_tag_data/log_date=$ONE_DAY_AGO

# hadoop dfs -mkdir /share_data/diyuanxin_data
# hadoop dfs -rmr -skipTrash output_path=/share_data/diyuanxin_data
hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $diyuanxi_data_path
done