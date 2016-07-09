# 产生ip类别的黑名单
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

datebase=tmp
log_date=$ONE_DAY_AGO
output_file=/data/dongjiahao/svnProject/fraudDetection/hive_result/tempResult.txt
##########
hive -e"
	use "$datebase";
	select ip, log_date, count(distinct yoyi_cookie), count(distinct domain), count(*) as impression, sum(clk) as sumclk, sum(clk)/count(*) as ctr 
	from tmp.algo_new_basedata
    where log_date='$log_date'
    group by ip, log_date
    having sum(clk)>10 and sum(clk)/count(*)>0.1
    order by sumclk desc
    ;
" > $output_file