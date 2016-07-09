#!/usr/bin/env bash
# operations on hadoop hive
output_file=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult.txt
output_file1=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult1.txt
output_file2=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult2.txt
output_file3=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult3.txt
output_file4=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult4.txt
output_ip=/data/dongjiahao/svnProject/fraudDetection/hive_result/ipInput
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
database=tmp
# database=dsp

#hive -e"
#	use "$database";
#	select ip,log_date,adx_id,
#		count(distinct adx_id),count(distinct user_agent), count(distinct browser),
#		count(distinct os), count(distinct yoyi_cookie),
#		count(distinct domain),count(distinct adzone_id),
#		count(*)/count(distinct domain),count(*) as impression, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where ip='180.140.90.142'
#	group by ip,log_date,adx_id
#	having impression>0
#	order by log_date,ip
#;" > $output_file

hive -e"
	use "$database";
	select sum(clk),count(distinct ip)
	from algo_new_basedata
;" > $output_file

#hive -e"
#	use "$database";
#	select order_id,campaign_id,
#		count(distinct adx_id),count(distinct user_agent), count(distinct browser),
#		count(distinct os), count(distinct yoyi_cookie),
#		count(distinct domain),count(distinct adzone_id),
#		count(*)/count(distinct domain),count(*) as impression, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where domain='pconline.com.cn'
#	group by order_id,campaign_id
#	having impression>0
#	order by order_id,campaign_id,ctr
#;" > $output_file

#hive -e"
#	use "$database";
#	select domain, log_date,hour, sum(clk) as clicksum, sum(clk)/24 as clickavg, count(distinct ip)
#	from algo_new_basedata
#	where clk=1 and log_date>='2015-12-27' and log_date<='2016-01-29' and campaign_id='413'
#	group by domain, log_date,hour
#	order by domain, log_date,hour
#;" > $output_file3

#hive -e"
#	use "$database";
#	select user_agent,ip,log_date,hour,sum(clk) as clicksum, sum(clk)/count(*)
#	from algo_new_basedata
#	where clk=1 and domain='iszyy.com'
#	group by user_agent,ip,log_date,hour
#	order by ip,user_agent,clicksum, user_agent, log_date,hour
#;" > $output_file3

#hive -e"
#	use "$database";
#	select algo_new_basedata.yoyi_cookie,algo_new_basedata.domain
#	from (
#		select yoyi_cookie, sum(clk) as clicksum
#		from algo_new_basedata
#		where domain='iszyy.com'
#		group by yoyi_cookie
#		having sum(clk)>1
#		order by clicksum desc) a left outer join algo_new_basedata on a.yoyi_cookie=algo_new_basedata.yoyi_cookie
#		where algo_new_basedata.yoyi_cookie is not null
#		order by algo_new_basedata.yoyi_cookie
#;" > $output_file3

#select login.uid from login left outer join regusers on login.uid=regusers.uid where regusers.uid is not null

#hive -e"
#	use "$database";
#	select yoyi_cookie, sum(clk) as clicksum, sum(clk)/count(*)
#	from algo_new_basedata
#	where campaign_id='507'
#	group by yoyi_cookie
#	order by clicksum
#;" > $output_file3

#hive -e"
#	use "$database";
#	select ip,log_date,count(distinct domain),count(distinct adzone_id),count(*)/count(distinct domain),count(*) as impression, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where log_date>'2016-01-10'
#	group by ip,log_date
#	having impression>200
#	order by log_date,ip
#;" > $output_ip

#
#hive -e"
#	use "$database";
#	select ip, log_date,count(distinct domain),count(distinct adzone_id), count(distinct yoyi_cookie), count(*) as impression, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	group by ip, log_date
#	having impression>600
#	order by log_date, ip, impression desc
#;" > $output_ip