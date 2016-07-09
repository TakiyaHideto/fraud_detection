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
output_file5=/home/dongjiahao/tempResult5.txt
output_file6=/home/dongjiahao/tempResult6.txt

## 检测PICC 04.09~04.17 domain流量分布情况
#hive -e"
#	use "$database";
#	select domain, log_date, count(*), sum(clk), sum(reach), sum(clk)/count(*) as ctr, sum(reach)/sum(clk) as reach_rate
#	from algo_stat
#	where log_date>='2016-04-09' and log_date<='2016-04-17' and (ctr_bucket='picc_reach_gbdt')
#	group by domain, log_date
#" > ${output_file4}

## 特定domain在不同订单上每日流量分布情况
#hive -e"
#	use "$database";
#	select log_date,avg(bid_price), sum(yoyi_cost)/sum(imp), count(distinct session_id), count(*), sum(inner_win), sum(imp), sum(clk), sum(reach), sum(inner_win)/count(*), sum(imp)/count(*), sum(clk)/sum(imp) as ctr, sum(reach)/sum(clk) as reach_rate
#	from algo_stat
#	where log_date>='2016-04-09' and log_date<='2016-04-17' and domain='pconline.com.cn' and ctr_bucket!='picc_reach_gbdt'
#	group by log_date
#"

## 特定order特定domain的到达和bid_price和yoyi_cost
#hive -e"
#	use "$database";
#	select domain, avg(bid_price), sum(yoyi_cost)/sum(imp), count(*), sum(inner_win), sum(imp), sum(clk) as sumclk, sum(reach), sum(inner_win)/count(*), sum(imp)/count(*), sum(clk)/sum(imp) as ctr, sum(reach)/sum(clk) as reach_rate
#	from algo_stat
#	where log_date>='2016-04-09' and log_date<='2016-04-11' and domain!='pconline.com.cn' and ctr_bucket='picc_reach_gbdt' and order_id='23131'
#	group by domain
#	having sum(clk)>0
#	sort by sumclk desc;
#" > $output_file2

## 特定domain在不同订单上每日流量分布情况
#hive -e"
#	use "$database";
#	select log_date, avg(bid_price), sum(yoyi_cost)/sum(imp), count(distinct order_id), count(*), sum(inner_win), sum(inner_win)/count(*)
#	from algo_stat
#	where log_date>='2016-04-09' and log_date<='2016-04-17' and domain='pconline.com.cn' and ctr_bucket!='picc_reach_gbdt'
#	group by log_date;
#"

## order对pconline竞价成功率和出价 on specific data
#hive -e"
#	use "$database";
#	select order_id, campaign_id, avg(bid_price) as price, variance(bid_price), sum(reach)/sum(clk), count(*), sum(inner_win) as innerwin, sum(inner_win)/count(*), sum(imp) as sumimp, sum(clk)
#	from algo_stat
#	where log_date='2016-04-16' and domain='pconline.com.cn'
#	group by order_id,campaign_id
#	order by innerwin desc;
#" > $output_file1

## 查看23131订单和其它订单的竞争情况
#hive -e"
#	use "$database";
#	select algo_stat.session_id, count(distinct algo_stat.order_id)
#	from (
#		select session_id, log_date
#		from algo_stat
#		where order_id='23131' and log_date='2016-04-16' and domain='pconline.com.cn'
#	) a join algo_stat on a.session_id=algo_stat.session_id and a.log_date=algo_stat.log_date
#	where algo_stat.log_date='2016-04-16' and algo_stat.domain='pconline.com.cn'
#	group by algo_stat.session_id
#	;
#" > $output_file6
#
## 查看23131订单和其它订单的竞争情况
#hive -e"
#	use "$database";
#	select algo_stat.session_id, count(distinct algo_stat.order_id)
#	from (
#		select session_id, log_date
#		from algo_stat
#		where order_id='23131' and log_date='2016-04-14' and domain='pconline.com.cn'
#	) a join algo_stat on a.session_id=algo_stat.session_id and a.log_date=algo_stat.log_date
#	where algo_stat.log_date='2016-04-14' and algo_stat.domain='pconline.com.cn'
#	group by algo_stat.session_id
#	;
#" > $output_file5

## 查看23131订单和其它订单的竞争情况
#hive -e"
#	use "$database";
#	select algo_stat.session_id, algo_stat.order_id, algo_stat.bid_price, algo_stat.inner_win
#	from (
#		select session_id, log_date
#		from algo_stat
#		where order_id='23131' and log_date='2016-04-14' and domain='pconline.com.cn' and inner_win=0
#	) a join algo_stat on a.session_id=algo_stat.session_id and a.log_date=algo_stat.log_date
#	where algo_stat.log_date='2016-04-14' and algo_stat.domain='pconline.com.cn'
#	order by algo_stat.session_id
#	;
#" > $output_file5

## 查看23131订单和其它订单的竞争情况
#hive -e"
#	use "$database";
#	select algo_stat.session_id, algo_stat.order_id, algo_stat.bid_price, algo_stat.inner_win
#	from (
#		select session_id, log_date, max(bid_price) as max_price, count(distinct order_id) as dis_oid_num
#		from algo_stat
#		where log_date='2016-04-14' and domain='pconline.com.cn'
#		group by session_id, log_date
#	) a join algo_stat on a.session_id=algo_stat.session_id and a.log_date=algo_stat.log_date and a.max_price=algo_stat.bid_price
#	where algo_stat.log_date='2016-04-14' and algo_stat.domain='pconline.com.cn'
#	order by algo_stat.session_id
#	;
#" > $output_file5

## 查看23131订单和其它订单的竞争情况
#hive -e"
#	use "$database";
#	select algo_stat.order_id, algo_stat.campaign_id, sum(imp), avg(bid_price), variance(bid_price)
#	from (
#		select session_id, log_date
#		from algo_stat
#		where order_id='23131' and log_date='2016-04-14' and domain='pconline.com.cn'
#	) a join algo_stat on a.session_id=algo_stat.session_id and a.log_date=algo_stat.log_date
#	where algo_stat.log_date='2016-04-14' and algo_stat.domain='pconline.com.cn' and order_id='17478'
#	group by algo_stat.order_id, algo_stat.campaign_id
#	;
#" > $output_file5

## 查看23131订单和其它订单的竞争情况
#hive -e"
#	use "$database";
#	select bid_price
#	from (
#		select session_id, log_date
#		from algo_stat
#		where order_id='23131' and log_date='2016-04-14' and domain='pconline.com.cn'
#	) a join algo_stat on a.session_id=algo_stat.session_id and a.log_date=algo_stat.log_date
#	where algo_stat.log_date='2016-04-14' and algo_stat.domain='pconline.com.cn' and order_id='17478'
#	;
#" > $output_file5
#hive -e"
#	use "$database";
#	select bid_price
#	from (
#		select session_id, log_date
#		from algo_stat
#		where order_id='23131' and log_date='2016-04-14' and domain='pconline.com.cn'
#	) a join algo_stat on a.session_id=algo_stat.session_id and a.log_date=algo_stat.log_date
#	where algo_stat.log_date='2016-04-14' and algo_stat.domain='pconline.com.cn' and order_id='23131'
#	;
#" > $output_file6


#hive -e"
#	use "$database";
#	select domain, count(*), sum(clk) as sumclk, sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where log_date>='2016-04-01'
#	group by domain
#	sort by sumclk desc
#	;
#" > $output_file6

hive -e"
	use "$database";
	select yoyi_cookie, count(distinct ip), count(distinct user_agent),
	count(distinct ip)/count(distinct user_agent), count(distinct domain),
	 count(*), sum(clk) as sumclk, sum(clk)/count(*) as ctr
	from algo_new_basedata
	where log_date>='2016-05-01'
	group by yoyi_cookie
	sort by sumclk desc
	limit 2000
	;
" > $output_file6