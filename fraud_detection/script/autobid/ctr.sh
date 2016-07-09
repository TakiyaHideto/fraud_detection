source /etc/profile
#向前推进几天的数据
CAL_DAYS=7
#今天
#TODAY=2015-07-20
TODAY=`date +%Y-%m-%d` 
echo $TODAY
#昨天的日期
#ONE_DAY_AGO=2015-07-19
ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
echo $ONE_DAY_AGO
#最早的统计数据的日期
#N_DAYS_AGO=2015-07-13
N_DAYS_AGO=`date +%Y-%m-%d --date="-${CAL_DAYS} day"`
echo $N_DAYS_AGO
dir=/home/yuliang/ExtractFeatures/trunk/shell_autobid

hive -e"
select 
	exchange_id, 
	order_id, 
	ad_pos_id, 
	sum(filter_click)/(sum(pv) + 100) ctr
from 
	tmp.yuliang_autobid_log
where 
	log_date between '${N_DAYS_AGO}' and '${ONE_DAY_AGO}'
group by
	exchange_id, 
    order_id, 
    ad_pos_id
having 
	sum(filter_click)/(sum(pv) + 100) > 0
	and sum(filter_click)/(sum(pv) + 100) < 1;
">${dir}/eop_tmp.txt

awk -F'\t' 'ARGIND==1{a[$1]=$2;}ARGIND==2{printf("%s\001%s\001%s\001%s\n", a[$1],$2,$3,$4)}' ${dir}/exchange_map.txt ${dir}/eop_tmp.txt > ${dir}/ex_order_pid_ctr.txt
rm ${dir}/eop_tmp.txt
hadoop dfs -rmr -skipTrash /mroutput/ctr/${TODAY}/ex_order_pid_ctr.txt
hadoop dfs -put ${dir}/ex_order_pid_ctr.txt /mroutput/ctr/${TODAY}/ex_order_pid_ctr.txt

if [ $? -ne 0 ];    then
    exit 1
fi

hive -e"
select
	exchange_id,
    ad_pos_id,
    sum(filter_click)*1.0/(sum(pv) + 100) ctr
from 
    tmp.yuliang_autobid_log
where 
    log_date between '${N_DAYS_AGO}' and '${ONE_DAY_AGO}'
group by
    exchange_id,
    ad_pos_id
having
	sum(filter_click)*1.0/(sum(pv) + 100) > 0
	and sum(filter_click)*1.0/(sum(pv) + 100) < 1;
">${dir}/ep_tmp.txt
awk -F'\t' 'ARGIND==1{a[$1]=$2;}ARGIND==2{printf("%s\001%s\001%s\n", a[$1],$2,$3)}' ${dir}/exchange_map.txt ${dir}/ep_tmp.txt > ${dir}/ex_pid_ctr.txt
rm ${dir}/ep_tmp.txt
hadoop dfs -rmr -skipTrash /mroutput/ctr/${TODAY}/ex_pid_ctr.txt
hadoop dfs -put ${dir}/ex_pid_ctr.txt /mroutput/ctr/${TODAY}/ex_pid_ctr.txt

if [ $? -ne 0 ];    then
    exit 1
fi

hive -e"
select 
    width,
    height,
    sum(filter_click)*1.0/(sum(pv)+100) ctr
from 
    tmp.yuliang_autobid_log
where 
    log_date between '${N_DAYS_AGO}' and '${ONE_DAY_AGO}'
group by 
    width,
    height 
having
    sum(filter_click)*1.0/(sum(pv) + 100) >= 0
	and sum(filter_click)*1.0/(sum(pv) + 100) < 1;
">${dir}/wh_tmp.txt
awk -F'\t' '{printf("%s\001%s\001%s\n", $1,$2,$3)}' ${dir}/wh_tmp.txt > ${dir}/width_height_ctr.txt
rm ${dir}/wh_tmp.txt
hadoop dfs -rmr -skipTrash /mroutput/ctr/${TODAY}/width_height_ctr.txt
hadoop dfs -put ${dir}/width_height_ctr.txt /mroutput/ctr/${TODAY}/width_height_ctr.txt

if [ $? -ne 0 ];    then
    exit 1
fi

hive -e"
select
    order_id, 
    avg(ctr)
from
    tmp.yuliang_autobid_log s
left outer join
    (select 
        ad_pos_id, 
        exchange_id,
        sum(filter_click) / (sum(pv) + 100) ctr
    from tmp.yuliang_autobid_log
    where
        log_date between '${N_DAYS_AGO}'  and '${ONE_DAY_AGO}'
    group by
        ad_pos_id, 
        exchange_id) p
on 
    s.ad_pos_id = p.ad_pos_id
    and s.exchange_id =p.exchange_id

where ctr <= 0.02 
    and ctr >= 0.0001
group by 
    order_id;
">${dir}/ov_tmp.txt
awk -F'\t' '{printf("%s\001%s\n", $1,$2)}' ${dir}/ov_tmp.txt > ${dir}/order_avgCtr.txt
rm ${dir}/ov_tmp.txt
hadoop dfs -rmr -skipTrash /mroutput/ctr/${TODAY}/order_avgCtr.txt
hadoop dfs -put ${dir}/order_avgCtr.txt /mroutput/ctr/${TODAY}/order_avgCtr.txt

if [ $? -ne 0 ];    then
    exit 1
fi
