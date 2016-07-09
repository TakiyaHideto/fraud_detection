
hive -e"
	use tmp;
	select distinct adzone_id 
	from algo_new_basedata 
	where adx_id='1' and log_date='2015-11-14' 
	;
" > /data/dongjiahao/svnProject/basedata/trunk/hive_result/dongjiahaotemp