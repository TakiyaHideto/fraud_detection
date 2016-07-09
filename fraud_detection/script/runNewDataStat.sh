start_date=$1
period=$2
hour=$3
bid_bucket=$4
use_algo_traffic=$5

jar_dir=/data/rongyifei/svn/basedata/trunk/target
bid_input=/user/ads/log/original_bid/log_date=
click_input=/user/ads/log/original_click/log_date=
show_input=/user/ads/log/original_show/log_date=
win_input=/user/ads/log/original_win/log_date=
new_base=/mroutput/rongyifei/new_basedata/tempbase

jar_name=hadoop_test-1.0-jar-with-dependencies.jar
class_name=com.mr.newbasedata.NewDataStat

hadoop dfs -rmr -skipTrash $new_base
hadoop jar ${jar_dir}/$jar_name $class_name $new_base $bid_input $click_input $show_input $win_input $start_date $period $hour $bid_bucket $use_algo_traffic
