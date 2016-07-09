start_date=$1
period=$2

jar_dir=/data/rongyifei/svn/basedata/trunk/target
bid_input=/user/ads/log/original_bid/log_date=
click_input=/user/ads/log/original_click/log_date=
show_input=/user/ads/log/original_show/log_date=
win_input=/user/ads/log/original_win/log_date=
new_base=/mroutput/yuliang/autobidding/tempbase

hadoop dfs -rmr -skipTrash $new_base
hadoop jar ${jar_dir}/hadoop_test-1.0-jar-with-dependencies.jar com.mr.basedata.NewTypeInfoText $new_base $bid_input $click_input $show_input $win_input $start_date $period


