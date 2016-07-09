#!/usr/bin/env bash

jar_dir=/data/dongjiahao/svnProject/fraudDetection/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.assessibility.AttachThirdPartyCookieTag

#output_path=/share_data/diyuanxin_tag_data/log_date=$ONE_DAY_AGO
#tag=NIKE_LBJ_0113
#output_path=/mroutput/dongjiahao/new_basedata/nike_lbj_cookie
#remy_data=/mroutput/dongjiahao/new_basedata/nike/nike_lbj_yoyi_cookie.txt
#
#hadoop dfs -rmr -skipTrash $output_path
#hadoop jar ${jar_dir}/$jar_name $class_name $output_path $remy_data $tag
#
#tag=TIGHTS_0113
#output_path=/mroutput/dongjiahao/new_basedata/tights_yoyi_cookie
#remy_data=/mroutput/dongjiahao/new_basedata/nike/tights_yoyi_cookie.txt
#
#hadoop dfs -rmr -skipTrash $output_path
#hadoop jar ${jar_dir}/$jar_name $class_name $output_path $remy_data $tag

tag=MOBILEPHONE_0118
output_path=/mroutput/dongjiahao/new_basedata/mobile_tag
remy_data=/mroutput/dongjiahao/new_basedata/mobile
hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $remy_data $tag
