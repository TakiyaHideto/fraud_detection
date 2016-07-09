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
#
#hive -e"
#	use "$database";
#	select algo_new_basedata.domain, sum(algo_new_basedata.clk), sum(algo_new_basedata.reach), sum(algo_new_basedata.reach)/sum(algo_new_basedata.clk) as reach_rate
#	from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#		from algo_new_basedata
#		where log_date>='2016-04-01'
#		group by domain,log_date,hour
#		having sumclk>5 and sum(clk)/count(*)>0.008
#	) a join algo_new_basedata on a.domain = algo_new_basedata.domain
#	where split(algo_new_basedata.action_monitor_id,'\002')[0]!='' and algo_new_basedata.log_date>='2016-04-01'
#	group by algo_new_basedata.domain
#	having sum(algo_new_basedata.clk)>0
#	sort by reach_rate desc;
#" > $output_file1

#hive -e"
#	use "$database";
#	select split(algo_data,'\002')[0] as ctr_bucket, domain, count(*), sum(clk), sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where log_date>='2016-04-15' and (split(algo_data,'\002')[0]='ID_lr_UA' or split(algo_data,'\002')[0]='FM')
#	group by split(algo_data,'\002')[0], domain
#	having sum(clk)>10
#	order by domain, ctr_bucket
#" > $output_file1

#hive -e"
#	use "$database";
#	select avg(b.keep_time)
#	from (select distinct yoyi_cookie as unique_cookie
#		from algo_new_basedata
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and split(algo_data,'\002')[0]='FFM') a
#		join
#		(select user_id, keep_time, deep
#		from databank_access
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and keep_time<180
#		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
#;"

### keeptime分布图
#hive -e"
#	use "$database";
#	select keep_time
#	from databank_access
#	where log_date>='2016-04-15'
#;" >$output_file3

## 计算刨去黑名单domain的二跳率
#hive -e"
#	use "$database";
#	select b.user_id,b.keep_time
#	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
#		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#			from algo_new_basedata
#			where log_date>='2016-04-01' and log_date<='2016-04-17'
#			group by domain,log_date,hour
#			having sum(clk)<=5 or sum(clk)/count(*)<0.008) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
#		where algo_new_basedata.log_date>='2016-04-15' and algo_new_basedata.log_date<='2016-04-18' and split(algo_new_basedata.algo_data,'\002')[0]='FFM') a
#		join
#		(select user_id, keep_time, deep
#		from databank_access
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and is_start=1
#		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
#;" > $output_file3

## 计算黑名单domain的二跳率
#hive -e"
#	use "$database";
#	select b.user_id,b.keep_time
#	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
#		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#			from algo_new_basedata
#			where log_date>='2016-04-01' and log_date<='2016-04-17'
#			group by domain,log_date,hour
#			having sum(clk)<5 or sum(clk)/count(*)<=0.008) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
#		where algo_new_basedata.log_date>='2016-04-01' and algo_new_basedata.log_date<='2016-04-17' and (algo_new_basedata.domain!='tudou.com' or algo_new_basedata.domain!='tudouui.com')) a
#		join
#		(select user_id, keep_time, deep
#		from databank_access
#		where log_date>='2016-04-01' and log_date<='2016-04-17' and is_start=1
#		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
#;" > $output_file3

## 计算黑名单domain的deep
#hive -e"
#	use "$database";
#	select b.user_id,b.avg_deep
#	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
#		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#			from algo_new_basedata
#			where log_date>='2016-04-01' and log_date<='2016-04-17'
#			group by domain,log_date,hour
#			having sum(clk)>=5 and sum(clk)/count(*)>0.008) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
#		where algo_new_basedata.log_date>='2016-04-01' and algo_new_basedata.log_date<='2016-04-17' and (algo_new_basedata.domain!='tudou.com' or algo_new_basedata.domain!='tudouui.com')) a
#		join
#		(select user_id, avg(deep) as avg_deep
#		from databank_access
#		where log_date>='2016-04-01' and log_date<='2016-04-17' and is_end=1
#		group by user_id) b on a.unique_cookie=b.user_id
#;" > $output_file3

## 计算黑名单domain的deep
#hive -e"
#	use "$database";
#	select b.user_id,b.avg_deep
#	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
#		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#			from algo_new_basedata
#			where log_date>='2016-04-01' and log_date<='2016-04-17'
#			group by domain,log_date,hour
#			having sum(clk)>=5 and sum(clk)/count(*)>0.008) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
#		where algo_new_basedata.log_date>='2016-04-01' and algo_new_basedata.log_date<='2016-04-17' and (algo_new_basedata.domain!='tudou.com' or algo_new_basedata.domain!='tudouui.com')) a
#		join
#		(select user_id, avg(deep) as avg_deep
#		from databank_access
#		where log_date>='2016-04-01' and log_date<='2016-04-17' and is_end=1
#		group by user_id) b on a.unique_cookie=b.user_id
#;" > $output_file3

## 计算特定domain的keep_time deep
#hive -e"
#	use "$database";
#	select b.user_id,b.avg_deep
#	from (select distinct yoyi_cookie as unique_cookie
#		from algo_new_basedata
#		where log_date>='2016-04-01' and log_date<='2016-04-17' and domain='pconline.com.cn') a
#		join
#		(select user_id, avg(deep) as avg_deep
#		from databank_access
#		where log_date>='2016-04-01' and log_date<='2016-04-17' and is_end=1
#		group by user_id) b on a.unique_cookie=b.user_id
#;" > $output_file3

## 计算某段时间内所有domain的keep_time deep
#hive -e"
#	use "$database";
#	select a.adzone_id ,avg(b.avg_deep), count(distinct a.unique_cookie), variance(avg_deep), count(*), count(distinct concat_ws('_',ip,user_agent))
#	from (
#		select yoyi_cookie as unique_cookie, domain,adzone_id, ip, user_agent, log_date
#		from algo_new_basedata
#		where log_date>='2016-04-01') a
#		join
#		(select user_id, avg(deep) as avg_deep, log_date
#		from databank_access
#		where log_date>='2016-04-01' and is_end=1
#		group by user_id, log_date) b
#		on a.unique_cookie=b.user_id and a.log_date=b.log_date
#	group by a.adzone_id
#;" > $output_file4


#hive -e"
#	use "$database";
#	select avg(b.deep)
#	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
#		from (
#			select algo_new_basedata.domain, algo_new_basedata.log_date, algo_new_basedata.hour, sum(algo_new_basedata.clk) as sumclk, sum(algo_new_basedata.clk)/count(*) as ctr
#			from algo_new_basedata
#			where algo_new_basedata.log_date>='2016-04-01' and algo_new_basedata.log_date<='2016-04-17'
#			group by algo_new_basedata.domain,algo_new_basedata.log_date,algo_new_basedata.hour
#		) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
#		where algo_new_basedata.log_date>='2016-04-15' and algo_new_basedata.log_date<='2016-04-18' and split(algo_new_basedata.algo_data,'\002')[0]='FFM') a
#		join
#		(select user_id, keep_time, deep
#		from databank_access
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and deep<10
#		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
#;"

#hive -e"
#	use "$database";
#	select count(distinct a.domain), sum(a.sumimp) , sum(a.sumclk), sum(a.sumclk)/sum(a.sumimp) as ctr_glo
#	from (select domain, log_date, hour, count(*) as sumimp, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#		from algo_new_basedata
#		where log_date>='2016-04-15' and log_date<='2016-04-17' and split(algo_data,'\002')[0]='ID_lr_UA'
#		group by domain,log_date,hour
#		having sumclk>5 and sum(clk)/count(*)>0.01) a
#		join
#		(select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#		from algo_new_basedata
#		where log_date>='2016-04-01' and log_date<='2016-04-17'
#		group by domain,log_date,hour
#		having sumclk>5 and sum(clk)/count(*)>0.008) b on a.domain=b.domain and a.hour=b.hour and a.log_date=b.log_date
#	where a.domain is not null and a.hour is not null
#;"

#hive -e"
#	use "$database";
#	select count(*)
#	from (select distinct yoyi_cookie as unique_cookie
#		from algo_new_basedata
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and split(algo_data,'\002')[0]='FM') a
#		join
#		(select user_id, avg(keep_time) as avg_keep_time, avg(deep) as avg_deep
#		from databank_access
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and keep_time<180
#		group by user_id) b on a.unique_cookie=b.user_id
#;"

#avg(b.avg_keep_time) as avg_time_glo, avg(avg_deep) as avg_deep_glo

#hive -e"
#	use "$database";
#	select adzone_id, count(*) as sumimp, sum(clk) as sumclk, sum(clk)/count(*)
#	from algo_new_basedata
#	where log_date>='$SEVEN_DAY_AGO' and bid_way='1'
#	group by adzone_id
#;" > $output_file3

hive -e"
	use "$database";
	select algo_new_basedata.adzone_id, count(*) as sumimp, sum(algo_new_basedata.clk) as sumclk, sum(algo_new_basedata.clk)/count(*)
	from (select domain, sum(clk)/count(*) as ctr
		from algo_new_basedata
		where log_date>='$SEVEN_DAY_AGO' and bid_way='1'
		group by domain
		having sum(clk)/count(*) > 0.02)a
		join algo_new_basedata on a.domain=algo_new_basedata.domain
	where algo_new_basedata.log_date>='$SEVEN_DAY_AGO' and algo_new_basedata.bid_way='1'
	group by algo_new_basedata.adzone_id
;" > $output_file4