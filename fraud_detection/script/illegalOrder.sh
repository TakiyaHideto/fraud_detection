
#昨天的日期
ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/basedata/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

#class_name=com.mr.newbasedata.GetContent
#class_name=com.mr.newbasedata.Test
class_name=com.mr.dataanalytics.illegalOrder

#click_input=/user/ads/log/original_click/log_date=${ONE_DAY_AGO}
bid_input=/user/ads/log/original_bid/log_date=${ONE_DAY_AGO}
output_path=/mroutput/dongjiahao/new_basedata/data_example

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $bid_input $output_path


