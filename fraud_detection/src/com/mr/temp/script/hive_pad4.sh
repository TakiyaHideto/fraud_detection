#!/usr/bin/env bash

output_ip=/data/dongjiahao/svnProject/fraudDetection/hive_result/ipInput
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
THIRTY_DAY_AGO=`date +%Y-%m-%d --date="-31 day"`
SEVEN_DAY_AGO=`date +%Y-%m-%d --date="-7 day"`
database=tmp
output_file=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult.txt
output_file1=/home/dongjiahao/tempResult1.txt
output_file2=/home/dongjiahao/tempResult2.txt
output_file3=/home/dongjiahao/tempResult3.txt
output_file4=/home/dongjiahao/tempResult4.txt

#hive -e"
#	use "$database";
#	select domain, log_date,sum(clk), sum(clk)/count(distinct concat_ws('_',ip,user_agent)) as avg_clk
#	from algo_new_basedata
#	where log_date>='2016-04-01'
#	group by domain,log_date
#	having sum(clk)>5 and sum(clk)/count(distinct concat_ws('_',ip,user_agent))>0.5
#	sort by avg_clk;
#;" >$output_file4

## 抽取高clk的IP所访问的domain
#hive -e"
#	use "$database";
#	select domain, sum(clk), sum(clk)/count(*) as ctr
#	from (
#		select concat_ws('_',ip,user_agent) as user, log_date, sum(clk) as sumclk
#		from algo_new_basedata
#		where log_date>='2016-04-15'
#		group by concat_ws('_',ip,user_agent), log_date
#		having sum(clk)>20
#	) a
#	join algo_new_basedata on concat_ws('_',algo_new_basedata.ip,algo_new_basedata.user_agent)=a.user and algo_new_basedata.log_date=a.log_date
#	group by domain
#	sort by ctr desc
#" > $output_file4

## 抽取databank_access数据
#hive -e"
#	use "$database";
#	select domain, sum(clk), sum(clk)/count(*) as ctr
#
#" > $output_file4

## 特定domain在不同订单上每日流量分布情况
#hive -e"
#	use "$database";
#	select log_date, avg(bid_price), sum(reach)/sum(clk), count(distinct order_id), count(*), sum(inner_win), sum(inner_win)/count(*)
#	from algo_stat
#	where log_date>='2016-04-09' and log_date<='2016-04-17' and ctr_bucket='picc_reach_gbdt'
#	group by log_date
#"

#hive -e"
#	use "$database";
#	select adzone_id, count(distinct yoyi_cookie), count(distinct domain),count(*)/count(distinct yoyi_cookie), sum(imp) as sumimp, sum(clk) as sumclk, sum(clk)/sum(imp)
#	from algo_stat
#	where log_date>='$SEVEN_DAY_AGO' and bid_way='1'
#	group by adzone_id
#;" > $output_file2

#hive -e"
#	use "$database";
#	select domain,
#	sum(case when hour>=2 and hour<=6 and clk=1 then 1 else 0 end),
#	sum(clk),
#	sum(case when hour>=2 and hour<=6 and clk=1 then 1 else 0 end)/sum(clk),
#	sum(case when hour>=2 and hour<=6 then 1 else 0 end),
#	count(*),
#	sum(case when hour>=2 and hour<=6 then 1 else 0 end)/count(*),
#	sum(case when hour>=2 and hour<=6 and clk=1 then 1 else 0 end)/sum(case when hour>=2 and hour<=6 then 1 else 0 end),
#	sum(case when (hour<2 or hour>6) and clk=1 then 1 else 0 end)/sum(case when (hour<2 or hour>6) then 1 else 0 end),
#	(sum(case when (hour<2 or hour>6) and clk=1 then 1 else 0 end)/sum(case when (hour<2 or hour>6) then 1 else 0 end))/(sum(case when hour>=2 and hour<=6 and clk=1 then 1 else 0 end)/sum(case when hour>=2 and hour<=6 then 1 else 0 end)),
#	sum(clk)/count(*)
#	from algo_new_basedata
#	where log_date>='$SEVEN_DAY_AGO'
#	group by domain
#	having sum(clk)>0
#;" > $output_file3

hive -e"
	use "$database";
	select domain, count(*) as imp, sum(clk), sum(clk)/count(*)
	from algo_new_basedata
	where log_date>='2016-05-08' and order_id='32682'
	group by domain
	sort by imp;
"