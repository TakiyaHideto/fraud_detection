#!/usr/bin/env bash

output_ip=/data/dongjiahao/svnProject/fraudDetection/hive_result/ipInput
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
THIRTY_DAY_AGO=`date +%Y-%m-%d --date="-31 day"`
SEVEN_DAY_AGO=`date +%Y-%m-%d --date="-7 day"`
START_DATE="2016-05-01"
STOP_DATE="2016-05-10"
database=tmp
output_file1=/home/dongjiahao/tempResult1.txt
output_file2=/home/dongjiahao/tempResult2.txt

# 计算非黑名单domain的二跳率、停留时间、跳转深度
hive -e"
	use "$database";
	select b.user_id,b.keep_time,b.deep,b.is_start,b.is_end
	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
			from algo_new_basedata
			where log_date>='$START_DATE' and log_date<='$STOP_DATE'
			group by domain,log_date,hour
			having sum(clk)<5 or sum(clk)/count(*)<=0.008) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
		where algo_new_basedata.log_date>='$START_DATE' and algo_new_basedata.log_date<='$STOP_DATE') a
		join
		(select user_id, keep_time, deep, is_start, is_end
		from databank_access
		where log_date>='$START_DATE' and log_date<='$STOP_DATE') b on a.unique_cookie=b.user_id
;" > $output_file1

# 计算黑名单domain的二跳率、停留时间、跳转深度
hive -e"
	use "$database";
	select b.user_id,b.keep_time,b.deep,b.is_start,b.is_end
	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
			from algo_new_basedata
			where log_date>='$START_DATE' and log_date<='$STOP_DATE'
			group by domain,log_date,hour
			having sum(clk)>=5 and sum(clk)/count(*)>0.008) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
		where algo_new_basedata.log_date>='$START_DATE' and algo_new_basedata.log_date<='$STOP_DATE') a
		join
		(select user_id, keep_time, deep, is_start, is_end
		from databank_access
		where log_date>='$START_DATE' and log_date<='$STOP_DATE') b on a.unique_cookie=b.user_id
;" > $output_file2