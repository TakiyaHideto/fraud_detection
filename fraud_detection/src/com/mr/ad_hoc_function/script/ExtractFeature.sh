#!/usr/bin/env bash

#ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
#
#jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
#jar_name=hadoop_test-1.0-jar-with-dependencies.jar
#
#class_name=com.mr.ad_hoc_function.ExtractFeature
#
#output_path=/mroutput/dongjiahao/ExtractFeature/log_date=$ONE_DAY_AGO
#basedata_pc_path=/share_data/new_basedata
#
#hadoop fs -rmr -skipTrash $output_path
#hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata_pc_path $ONE_DAY_AGO
#
#sh /data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/downlaod_basedata.sh


#following belongs to mobile basedata
#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`

jar_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.ad_hoc_function.ExtractFeature

output_path=/mroutput/dongjiahao/ExtractFeatureMobile/log_date=$ONE_DAY_AGO
basedata_pc_path=/share_data/new_mobile_basedata

hadoop fs -rmr -skipTrash $output_path
hadoop jar $jar_dir/$jar_name $class_name $output_path $basedata_pc_path $ONE_DAY_AGO

sh /data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/downlaod_basedata.sh














