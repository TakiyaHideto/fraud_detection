cd ../
mvn clean install -Dhadoop2 -Dhadoop.2.version=2.4.0 -DskipTests

date=2015-09-06
input=/mroutput/yuliang/autobidding/20150720downsample.txt
ctr_input1=/mroutput/ctr/${date}/ex_order_pid_ctr.txt
ctr_input2=/mroutput/ctr/${date}/ex_pid_ctr.txt
ctr_input3=/mroutput/ctr/${date}/width_height_ctr.txt
output1=/mroutput/yuliang/autobidding/tmp1
output2=/mroutput/yuliang/autobidding/tmp2
output3=/mroutput/yuliang/autobidding/auc


hadoop dfs -rmr -skipTrash $output1
hadoop jar target/hadoop_test-1.0.jar com.mr.JoinAutoBiddingFirst $input $ctr_input1 $output1

hadoop dfs -rmr -skipTrash $output2
hadoop jar target/hadoop_test-1.0.jar com.mr.JoinAutoBiddingSecond $output1 $ctr_input2 $output2

hadoop dfs -rmr -skipTrash $output3
hadoop jar target/hadoop_test-1.0.jar com.mr.JoinAutoBiddingThird $output2 $ctr_input3 $output3
