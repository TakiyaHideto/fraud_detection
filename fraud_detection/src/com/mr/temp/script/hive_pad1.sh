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
#	select a.ip as a_ip, b.domain as b_domain, b.hour as b_hour, count(*), sum(b.clk) as sumclk
#	from (select ip, sum(clk) as sumclk
#		from algo_new_basedata
#		where log_date='2016-04-12' and clk='1' and campaign_id='567' and (hour>15 and hour<24) and (domain='pconline.com.cn' or domain='lady8844.com' or domain='rczzxs.com' or domain='jiangsurx.com' or domain='tianjinrxw.com' or domain='qr25.com' or domain='yunnanrx.com')
#		group by ip
#		having sumclk>2) a join
#		(select ip,domain,hour,clk from algo_new_basedata where bid_date='2016-04-12') b on a.ip=b.ip
#	where a.ip is not null
#	group by a.ip, b.domain, b.hour
#	order by a_ip
#;" > $output_file4

#hive -e"
#	use "$database";
#	select b.ip as b_ip, b.hour as b_hour, count(*), sum(b.clk) as sumclk
#	from (select domain, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#		from algo_new_basedata
#		where log_date>='2016-04-14' and log_date<='2016-04-14' and order_id='17478' and campaign_id='567'
#		group by domain,log_date,hour
#		having sumclk>5 and sum(clk)/count(*)>0.01) a
#		join
#		(select ip,domain,hour,clk
#		from algo_new_basedata
#		where bid_date='2016-04-14' and order_id='17478' and campaign_id='567') b on a.domain=b.domain and a.hour=b.hour
#	where a.domain is not null and a.hour is not null
#	group by b.ip, b.domain, b.hour
#	order by sumclk desc
#;" > $output_file4

#hive -e"
#	use "$database";
#	select adzone_id, domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where log_date>='2016-04-01' and log_date<='2016-04-06'
#	group by adzone_id, domain,log_date,hour
#	having sumclk>10 and sum(clk)/count(*)>0.015
#	order by ctr desc;
#;" > $output_file2

## 计算平均跳转深度
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
#		where algo_new_basedata.log_date>='2016-04-15' and algo_new_basedata.log_date<='2016-04-18' and split(algo_new_basedata.algo_data,'\002')[0]='ID_lr_UA') a
#		join
#		(select user_id, keep_time, deep
#		from databank_access
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and deep<10
#		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
#;"

# 计算刨去黑名单domain的二跳率
hive -e"
	use "$database";
	select b.user_id,b.keep_time
	from (select distinct algo_new_basedata.yoyi_cookie as unique_cookie
		from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
			from algo_new_basedata
			where log_date>='2016-04-01' and log_date<='2016-04-17'
			group by domain,log_date,hour
			having sum(clk)<=5 or sum(clk)/count(*)<0.008) c join algo_new_basedata on algo_new_basedata.domain=c.domain and algo_new_basedata.log_date=c.log_date and algo_new_basedata.hour=c.hour
		where algo_new_basedata.log_date>='2016-04-15' and algo_new_basedata.log_date<='2016-04-18' and split(algo_new_basedata.algo_data,'\002')[0]='ID_lr_UA') a
		join
		(select user_id, keep_time, deep
		from databank_access
		where log_date>='2016-04-15' and log_date<='2016-04-18' and is_start=1
		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
;" > $output_file1

#hive -e"
#	use "$database";
#	select avg(b.keep_time)
#	from (select distinct yoyi_cookie as unique_cookie
#		from algo_new_basedata
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and split(algo_data,'\002')[0]='ID_lr_UA') a
#		join
#		(select user_id, keep_time, deep
#		from databank_access
#		where log_date>='2016-04-15' and log_date<='2016-04-18' and keep_time<180
#		group by user_id, keep_time, deep) b on a.unique_cookie=b.user_id
#;"

#hive -e"
#	use "$database";
#	select count(*)
#	from (select domain, log_date, hour, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#		from algo_new_basedata
#		where log_date>='2016-04-01' and log_date<='2016-04-06' and order_id='21118' and campaign_id='732'
#		group by domain,log_date,hour
#		having sumclk>5 and sum(clk)/count(*)>0.01) a
#		join
#		(select session_id,domain,log_date,hour
#		from algo_new_basedata
#		where log_date>='2016-04-01' and log_date<='2016-04-06' and order_id='21118' and campaign_id='732' ) b on a.domain=b.domain and a.hour=b.hour and a.log_date=b.log_date
#	where a.domain is not null and a.hour is not null
#;"
