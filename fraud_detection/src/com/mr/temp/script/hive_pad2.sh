#!/usr/bin/env bash

output_ip=/data/dongjiahao/svnProject/fraudDetection/hive_result/ipInput
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
THIRTY_DAY_AGO=`date +%Y-%m-%d --date="-31 day"`
SEVEN_DAY_AGO=`date +%Y-%m-%d --date="-7 day"`
database=tmp
output_dir=/home/dongjiahao
output_file1=/home/dongjiahao/tempResult1.txt
output_file2=/home/dongjiahao/tempResult2.txt
output_file3=/home/dongjiahao/tempResult3.txt
output_file4=/home/dongjiahao/tempResult4.txt

#hive -e"
#	use "$database";
#	select yoyi_cookie from algo_new_basedata
#        select yoyi_cookie
#        from algo_new_basedata
#        where log_date=='2016-04-06' and clk==1' and
#            order_id='21884' and campaign_id=='731' and
#            (hour=='3' or hour=='4') and (domain=='whichk.com' or domain=='iszyy.com' or domain=='4443.com')
#
#;" > $output_ip

#
#hive -e"
#	use "$database";
#	select sum(clk), count(*), sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where (domain!='iszzy.com' or domain!='whichk.com' or domain!='4443.com') and log_date='2016-04-06' and hour='3' and campaign_id='731' and order_id='21884'
#;" > $output_file2

#hive -e"
#	use "$database";
#	select domain, sum(clk), count(*), sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where log_date='2016-04-11' and hour>2 and hour<5
#	group by domain
#	having sum(clk)>10
#	order by ctr desc
#
#;" > $output_file2

# domain黑名单
hive -e"
	use "$database";
	select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
	from algo_new_basedata
	where log_date>='2016-02-23' and log_date<='2016-03-08'
	group by domain,log_date,hour
	having sumclk>=5 and sum(clk)/count(*)>=0.008
	order by ctr desc;
;" > $output_file3
#
## adzone黑名单
#hive -e"
#	use "$database";
#	select adzone_id, domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where log_date>='2016-04-01' and log_date<'2016-04-06' and (domain='tudou.com' or domain='tudouui.com')
#	group by adzone_id, domain,log_date,hour
#	having sumclk>8 and sum(clk)/count(*)>0.008
#	order by ctr desc;
#;" > $output_file1

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
#		where algo_new_basedata.log_date>='2016-04-15' and algo_new_basedata.log_date<='2016-04-18' and split(algo_new_basedata.algo_data,'\002')[0]='FM') a
#		join
#		(select user_id, keep_time, deep
#		from databank_access
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and is_start=1
#		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
#;" > $output_file2

# 计算黑名单domain的平均停留时间、
hive -e"
	use "$database";
	select avg(b.keep_time)
	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
			from algo_new_basedata
			where log_date>='2016-04-01' and log_date<='2016-04-17'
			group by domain,log_date,hour
			having sum(clk)>=8 or sum(clk)/count(*)>0.008) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
		where algo_new_basedata.log_date>='2016-02-23' and algo_new_basedata.log_date<='2016-03-08') a
		join
		(select user_id, keep_time, deep
		from databank_access
		where log_date>='2016-04-01' and log_date<='2016-04-17'
		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
;"

#hive -e"
#	use "$database";
#	select avg(b.keep_time)
#	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
#		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#			from algo_new_basedata
#			where log_date>='2016-04-01' and log_date<='2016-04-17'
#			group by domain,log_date,hour
#			) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
#		where algo_new_basedata.log_date>='2016-04-01' and algo_new_basedata.log_date<='2016-04-17') a
#		join
#		(select user_id, keep_time, deep
#		from databank_access
#		where log_date>='2016-04-01' and log_date<='2016-04-17'
#		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
#;"

## 计算黑名单domain的yoyi_cost
#hive -e"
#	use "$database";
#	select sum(algo_stat.imp), sum(algo_stat.yoyi_cost)/sum(algo_stat.imp)
#	from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#		from algo_new_basedata
#		where log_date>='2016-04-01' and log_date<='2016-04-17' and (domain!='tudou.com' or domain!='tudouui.com')
#		group by domain,log_date,hour
#		having sum(clk)<5 and sum(clk)/count(*)<=0.008) c
#	join algo_stat on algo_stat.domain=c.domain and algo_stat.log_date=c.log_date and algo_stat.hour=c.hour
#	where algo_stat.log_date>='2016-04-01' and algo_stat.log_date<='2016-04-17'
#
#;"

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
#		where algo_new_basedata.log_date>='2016-04-15' and algo_new_basedata.log_date<='2016-04-18' and split(algo_new_basedata.algo_data,'\002')[0]='FM') a
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
#		where log_date>='2016-04-15' and log_date<='2016-04-17' and split(algo_data,'\002')[0]='FM'
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
#	select count(distinct a.domain), sum(a.sumimp) , sum(a.sumclk), sum(a.sumclk)/sum(a.sumimp) as ctr_glo
#	from (select domain, log_date, hour, count(*) as sumimp, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#		from algo_new_basedata
#		where log_date>='2016-04-15' and log_date<='2016-04-17' and split(algo_data,'\002')[0]='FM'
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
#	select domain, split(algo_data,'\002')[0], count(*), sum(clk)
#	from algo_new_basedata
#	where log_date>='2016-04-15'
#	group by domain, split(algo_data,'\002')[0]
#;" > ${output_dir}/domain_ctr_bucket_imp_clk
