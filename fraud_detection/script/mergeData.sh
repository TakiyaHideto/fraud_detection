#!/usr/bin/env bash

#昨天的日期
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/basedata/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.merge_data.DataMR

bid_input=/user/ads/log/original_bid/log_date=${ONE_DAY_AGO}
win_input=/user/ads/log/original_win/log_date=${ONE_DAY_AGO}
show_input=/user/ads/log/original_show/log_date=${ONE_DAY_AGO}
click_input=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}
import_path=/mroutput/dongjiahao/new_basedata/dsp3_import
orderTanxPath=/user/dongjiahao/test
output_path=/mroutput/dongjiahao/new_basedata/data_$ONE_DAY_AGO


hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $bid_input $win_input $show_input $click_input $import_path $orderTanxPath


