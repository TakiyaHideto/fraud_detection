# operations on hadoop hive
database=tmp
root_idr=$5
blType=$4
output_file=$root_idr/$4/BL_{$1}_{$2}_{$3}.txt
order_id=$1
adx_id=$2

### 2015-11-08 脚本
# hive -e "
# 	use "$database";
# 	;
# 	" > $output_file

hive -e "
	use "$database";
	select yoyi_cookie, domain, count(distinct log_date) as log_date_num, count(distinct adzone_id) as adzone_id_num, count(*) as impression, sum(clk) as click, sum(clk)/count(*) as ctr
	from algo_new_basedata 
	where order_id='$order_id' and adx_id='$adx_id'  
	group by yoyi_cookie, domain 
	order by "$blType" desc
	limit 1000 
	;
	" > $output_file
