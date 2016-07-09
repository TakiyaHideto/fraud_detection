
#昨天的日期
# ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
# echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/data/dongjiahao/svnProject/basedata/trunk/target
jar_name=hadoop_test-1.0-jar-with-dependencies.jar

class_name=com.mr.frauddetection.RatioStatMR

origin_path=/mroutput/dongjiahao/new_basedata/fraud_detection_frequency_result_1025
input_path=/mroutput/dongjiahao/new_basedata/data_example_25
output_path=/mroutput/dongjiahao/new_basedata/fraud_detection_frequency_result_25

hadoop dfs -rmr -skipTrash $output_path
hadoop dfs -rmr -skipTrash $origin_path
hadoop jar ${jar_dir}/$jar_name $class_name $output_path $input_path


