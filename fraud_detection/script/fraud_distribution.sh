
#昨天的日期
ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/basedata/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.fraud_detection.DistributionMR

input_path=/mroutput/dongjiahao/new_basedata/data_$ONE_DAY_AGO
output_path=/mroutput/dongjiahao/new_basedata/tanx_hour_distribution_$ONE_DAY_AGO

hadoop dfs -rmr -skipTrash $output_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $input_path


