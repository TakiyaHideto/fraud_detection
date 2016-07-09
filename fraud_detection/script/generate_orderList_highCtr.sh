mkdir $4/$3
blType=$3
output_file=$1
log_date=$2
database=tmp

hive -e "
	use "$database";
	select campaign_id, order_id, adx_id, count(*) as impression, sum(clk) as click, sum(clk)/count(*) as ctr
	from algo_new_basedata 
	where log_date='$log_date'  
	group by campaign_id, order_id, adx_id 
	order by "$blType" desc;
	" > $output_file$log_date.txt

echo "finish generating order list"