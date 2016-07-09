#!/bin/bash
source /etc/profile
shell_dir=/home/yuliang/ExtractFeatures/trunk/shell_autobid
TODAY=`date +%Y-%m-%d`
COUNTER=0
max_repeat=5
EXE_STSTUS_1=1
EXE_STSTUS_2=1
while [ $COUNTER -lt $max_repeat ]
do

#${shell_dir}/test.sh
${shell_dir}/autobiddingLog.sh
if [ $? -eq 0 ]
then
	COUNTER=$max_repeat
	EXE_STSTUS_1=0
else
	COUNTER=`expr $COUNTER + 1`
fi
done

if [ $EXE_STSTUS_1 -eq 0 ]
then
	#${shell_dir}/test2.sh
	${shell_dir}/ctr.sh
	if [ $? -eq 0 ]
	then
		echo success exe test2
		EXE_STSTUS_2=0
	else
		echo failure exe test2
	fi
fi

echo $EXE_STSTUS_1 $EXE_STSTUS_2

function s_mail(){
	to_address=yuliang@yoyi.com.cn
	subject=$1
	content=$2
/usr/sbin/sendmail -t <<EOF
From: CTR DATA 
To: $to_address                           
Subject: $subject                       
---------------------------------              
$content
---------------------------------
EOF
}


function datacheck(){
	input_file_name=$1

	DAY_AGO_1=`date +%Y-%m-%d --date="-1 day"`
	DAY_AGO_2=`date +%Y-%m-%d --date="-2 day"`
	DAY_AGO_3=`date +%Y-%m-%d --date="-3 day"`
	DAY_AGO_4=`date +%Y-%m-%d --date="-4 day"`
	DAY_AGO_5=`date +%Y-%m-%d --date="-5 day"`
	DAY_AGO_6=`date +%Y-%m-%d --date="-6 day"`
	DAY_AGO_7=`date +%Y-%m-%d --date="-7 day"`

	eop_count_1=$(hadoop dfs -cat /mroutput/ctr/${DAY_AGO_1}/$input_file_name | wc -l)
	eop_count_2=$(hadoop dfs -cat /mroutput/ctr/${DAY_AGO_2}/$input_file_name | wc -l)
	eop_count_3=$(hadoop dfs -cat /mroutput/ctr/${DAY_AGO_3}/$input_file_name | wc -l)
	eop_count_4=$(hadoop dfs -cat /mroutput/ctr/${DAY_AGO_4}/$input_file_name | wc -l)
	eop_count_5=$(hadoop dfs -cat /mroutput/ctr/${DAY_AGO_5}/$input_file_name | wc -l)
	eop_count_6=$(hadoop dfs -cat /mroutput/ctr/${DAY_AGO_6}/$input_file_name | wc -l)
	eop_count_7=$(hadoop dfs -cat /mroutput/ctr/${DAY_AGO_7}/$input_file_name | wc -l)
	eop_count_0=$(hadoop dfs -cat /mroutput/ctr/${TODAY}/$input_file_name | wc -l)
	echo eop_count_0=${eop_count_0}
	echo eop_count_1=${eop_count_1}
	echo eop_count_2=${eop_count_2}
	echo eop_count_3=${eop_count_3}
	echo eop_count_4=${eop_count_4}
	echo eop_count_5=${eop_count_5}
	echo eop_count_6=${eop_count_6}
	echo eop_count_7=${eop_count_7}

	avg_eop_count=`expr \( ${eop_count_1} + ${eop_count_2} + ${eop_count_3} + ${eop_count_4} + ${eop_count_5} + ${eop_count_6} + ${eop_count_7} \) / 7 `
	echo avg_eop_count=${avg_eop_count}

	ratio=$(echo "scale=2;($eop_count_0 - $avg_eop_count)/$avg_eop_count" | bc)
	echo $ratio

	DATACHECK_STATUS=1
	if [ `echo "$ratio < 0.2 && $ratio > -0.2" | bc` -eq 1 ]; then 
		echo "ok"
		DATACHECK_STATUS=0
	else 
		echo "not ok"
		DATACHECK_STATUS=1
	fi 
}



if [ $EXE_STSTUS_1 -ne 0 ]; then
	s_mail "[WRONG][$TODAY]" "wrong in log" 
else
	if [ $EXE_STSTUS_2 -ne 0 ];    then
		s_mail "[WRONG][$TODAY]" "calculate wrong"
	else
		datacheck ex_order_pid_ctr.txt
		ST_1=$DATACHECK_STATUS
		echo $ST_1
		datacheck ex_pid_ctr.txt
		ST_2=$DATACHECK_STATUS
		echo $ST_2
		datacheck order_avgCtr.txt
		ST_3=$DATACHECK_STATUS
		echo $ST_3
		datacheck width_height_ctr.txt
		ST_4=$DATACHECK_STATUS
		echo $ST_4
		if [ ${ST_1} -ne 0 -o ${ST_2} -ne 0 -o ${ST_3} -ne 0 -o ${ST_4} -ne 0 ];	then
			s_mail "[WARNING][$TODAY]" "data count is unusual"
		else
			s_mail "[SUCCESS][$TODAY]" "success" 
		fi
	fi
fi















