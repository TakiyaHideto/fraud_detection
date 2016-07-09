#!/usr/bin/env bash
output_file=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult.txt
output_file1=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult1.txt
output_file2=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult2.txt
output_file3=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult3.txt
output_file4=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult4.txt
output_file_special=/data/dongjiahao/svnProject/fraudDetection/hive_result/impre_clk_ctr.txt
output_file_impre_cookie=/data/dongjiahao/svnProject/fraudDetection/hive_result/impre_cookie.txt
output_file_clk_cookie=/data/dongjiahao/svnProject/fraudDetection/hive_result/clk_cookie.txt
log_date=2015-11-22
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
database=tmp

hive -e"
	use "$database";
	select algo_new_basedata.yoyi_cookie, algo_new_basedata.domain

	from (
		select yoyi_cookie
		from algo_new_basedata
		where domain='jsyks.com'
		) a
		join algo_new_basedata on a.yoyi_cookie=algo_new_basedata.yoyi_cookie

	where algo_new_basedata.yoyi_cookie is not null
	group by algo_new_basedata.yoyi_cookie,algo_new_basedata.domain
	order by yoyi_cookie,domain;
" > $output_file2

#hive -e"
#	use "$database";
#	select algo_new_basedata.yoyi_cookie, algo_new_basedata.domain, algo_new_basedata.hour,
#		count(distinct algo_new_basedata.log_date),sum(clk),count(*)
#
#	from (
#		select yoyi_cookie
#		from algo_new_basedata
#		where ip='180.140.90.142' and log_date='2016-02-16' and order_id='17099'
#		) a
#		join algo_new_basedata on a.yoyi_cookie=algo_new_basedata.yoyi_cookie
#
#	where algo_new_basedata.yoyi_cookie is not null
#	group by algo_new_basedata.yoyi_cookie,algo_new_basedata.domain,algo_new_basedata.hour
#	order by yoyi_cookie,hour;
#" > $output_file2

#hive -e"
#
#	select a.yoyi_cookie, b.domain, count(distinct b.log_date) as stat_sum
#	from
#	(
#		select yoyi_cookie
#		from algo_new_basedata
#		where ip='180.140.90.142' and log_date='2016-02-16' and order_id='17099'
#	) a
#		join
#	(
#		select yoyi_cookie, domain, log_date
#		from algo_new_basedata
#		where log_date>='2016-02-03' and log_date<='2016-02-16'
#	) b
#		on a.yoyi_cookie=b.yoyi_cookie
#	where a.yoyi_cookie is not null
#	group by a.yoyi_cookie,b.domain
#	sort by stat_sum desc
#	limit 100
#;" > $output_file2

#hive -e"
#	use "$database";
#	select algo_new_basedata.yoyi_cookie,algo_new_basedata.domain
#
# 	from (
#		select yoyi_cookie, sum(clk) as clicksum
#		from algo_new_basedata
#		where domain='iszyy.com'
#		group by yoyi_cookie
#		having sum(clk)>1
#		order by clicksum desc) a left outer join algo_new_basedata on a.yoyi_cookie=algo_new_basedata.yoyi_cookie
#
# 		where algo_new_basedata.yoyi_cookie is not null
# 		order by algo_new_basedata.yoyi_cookie
#;" > $output_file3
