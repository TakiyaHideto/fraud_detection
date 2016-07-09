# extract data from hive

database=tmp
log_date=2015-11-05
# hive -e "
# 	use "$database";
#    	select session_id, yoyi_cost 
#    	from algo_new_basedata 
#    	where log_date='$log_date' ;
#    	" > /data/dongjiahao/svnProject/basedata/trunk/dataLog/hive_data.txt

hive -e "
	use "$database";
	select yoyi_cookie,sum(clk) as sumclk
	from algo_new_basedata 
	where log_date='$log_date'
	group by yoyi_cookie 
	having sum(clk)>0 
	order by sumclk desc;
	" > /data/dongjiahao/svnProject/basedata/trunk/dataLog/hive_data.txt