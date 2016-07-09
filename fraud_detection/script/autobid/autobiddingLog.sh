source /etc/profile
#今天
TODAY=`date +%Y-%m-%d`
#昨天的日期
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

jar_dir=/home/yuliang/ExtractFeatures/trunk
output=/mroutput/yuliang/autobidding/log/${ONE_DAY_AGO}

function old_log(){
cd ../
#mvn clean install -Dhadoop2 -Dhadoop.2.version=2.4.0 -DskipTests

log_show_input=/user/hive/warehouse/dsp.db/log_show/log_date=${ONE_DAY_AGO}
log_click_input=/user/hive/warehouse/dsp.db/log_click/log_date=${ONE_DAY_AGO}
material=/share_data/basedata/Import/category/material
advert=/share_data/basedata/Import/category/advert
output_old=/mroutput/yuliang/autobidding/log/${ONE_DAY_AGO}/old

hadoop dfs -rmr -skipTrash $output_old
hadoop jar ${jar_dir}/target/hadoop_test-1.0.jar com.mr.AutoBiddingLog $log_show_input $log_click_input $output_old $material $advert

}

function new_log(){
input_new=/
output_new=/mroutput/yuliang/autobidding/log/${ONE_DAY_AGO}/new
hadoop dfs -rmr -skipTrash $output_new
hadoop jar ${jar_dir}/target/hadoop_test-1.0.jar com.mr.AutoBiddingLogNewType $input_new $output_new

}





function creat(){
hive -e"
use tmp;
drop table if exists yuliang_autobid_log;
create external table yuliang_autobid_log(
    exchange_id string,
	ad_pos_id       string,
	order_id        string,
    pv      bigint,
    filter_pv       bigint,
    click   bigint,
    filter_click    bigint,
    width int,
    height int)
partitioned by (log_date string,
				log_type string)
row format delimited fields terminated by '\001';
"
}

old_log
if [ $? -ne 0 ];	then
	echo old_log error
	exit 1
else
	hadoop dfs -test -e $output_old
	if [ $? -ne 0 ];	then
		exit 1
	fi
fi

hive -e"
use tmp;
alter table yuliang_autobid_log drop partition (log_date='$ONE_DAY_AGO', log_type='old');   
alter table yuliang_autobid_log add partition (log_date='$ONE_DAY_AGO', log_type='old') location '$output_old';

"
if [ $? -ne 0 ];	then
	exit 1
fi

