ONE_DAY_AGO=`date +%Y-%m-%d --date="-18 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/basedata/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.merge_data.CheckReachVolumeMR

bid_input=/user/ads/log/original_bid/log_date=${ONE_DAY_AGO}
win_input=/user/ads/log/original_win/log_date=${ONE_DAY_AGO}
show_input=/user/ads/log/original_show/log_date=${ONE_DAY_AGO}
click_input=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}
reach_input=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}
output_path=/mroutput/dongjiahao/new_basedata/reach_volume_$ONE_DAY_AGO


hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $bid_input $reach_input