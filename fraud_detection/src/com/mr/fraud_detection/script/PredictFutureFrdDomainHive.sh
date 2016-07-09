#!/usr/bin/env bash

database=algo
ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
START_DATE=`date +%Y-%m-%d --date="-14 day"`
output_file_domain="/home/algo/svn/fraudDetection/file/fraud_clk_domain_blocked/fraud_domain_${ONE_DAY_AGO}"
output_file_adzone="/home/algo/svn/fraudDetection/file/fraud_clk_domain_blocked/fraud_adz_${ONE_DAY_AGO}"
#
#hive -e"
#	use "$database";
#    select domain, log_date, hour, count(*) as sumimp, sum(clk) as sumclk, sum(clk)/count(*) as ctr
#	from algo_new_basedata
#	where log_date>='$ONE_DAY_AGO'
#	group by domain,log_date,hour
#	having sumclk>=5 and sum(clk)/count(*)>0.008
#;" > $output_file_domain
#
#cat ${output_file_domain} | awk '{print $1}' | sort -k 1 | uniq -c | awk '{if($1>2)print $2}' > temp
#cat temp > ${output_file_domain}
#rm temp

hive -e"
	use "$database";
    select algo_new_basedata.adzone_id, algo_new_basedata.log_date, algo_new_basedata.hour, count(*), sum(algo_new_basedata.clk) as sumclk, sum(algo_new_basedata.clk)/count(*) as ctr
	from (select domain, log_date, hour, count(*) as sumimp
			from algo_new_basedata
			where log_date='$ONE_DAY_AGO'
			group by domain, log_date, hour
			having count(*)>10000) a join algo_new_basedata on algo_new_basedata.domain=a.domain
	where algo_new_basedata.log_date>='$ONE_DAY_AGO'
	group by algo_new_basedata.adzone_id,algo_new_basedata.log_date,algo_new_basedata.hour
	having sumclk>=1 and sum(algo_new_basedata.clk)/count(*)>0.001
;" > $output_file_adzone