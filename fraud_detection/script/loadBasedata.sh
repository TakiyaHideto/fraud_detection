#!/usr/bin/env bash
start_date=$1
end_date=$2
user=$3
campaign=$4

jar_path=/data/$user/svn/basedata/trunk/target
package=hadoop_test-1.0-jar-with-dependencies.jar
tmp_dir=/share_data/tmp
import_dir=$tmp_dir/import

if [ "$campaign" == "All" ]; then  # charge the diff table
    table_name="tmp.algo_basedata"
    out_path=/share_data/basedata
else 
    table_name='tmp.algo_campaigndata'
    out_path=/share_data/campaign_data
fi

hive -e "
    use tmp;
    CREATE EXTERNAL TABLE if not exists algo_basedata(
        sign               string,
        yoyi_cost          int,
        clk                int,
        reach              int,
        action             int,
        action_monitor_id  string,
        account_id         string,
        campaign_id        string,
        order_id           string,
        industry_cate_id   string,
        adx_id             string,
        domain             string,
        host               string,
        url                string,
        ad_pos_id          string,
        content_id         string,
        yoyi_cate_id       string,
        bid_timestamp      bigint,
        bid_date           string,
        weekday            int,
        hour               int,
        minute             int,
        ad_id              string,
        width              int,
        height             int,
        filesize           int,
        os_id              string,
        browser_id         string,
        area_id            string,
        ip                 string,
        language           string,
        cookie_id          string,
        bd_sex             string,
        bd_interest        string,
        mz_interest        string)
        PARTITIONED BY (log_date string)
        row format delimited fields terminated by '\001'
        location '/share_data/basedata';"

hive -e "
    use tmp;
    CREATE EXTERNAL TABLE if not exists algo_campaigndata(
        sign               string,
        yoyi_cost          int,
        clk                int,
        reach              int,
        action             int,
        action_monitor_id  string,
        account_id         string,
        campaign_id        string,
        order_id           string,
        industry_cate_id   string,
        adx_id             string,
        domain             string,
        host               string,
        url                string,
        ad_pos_id          string,
        content_id         string,
        yoyi_cate_id       string,
        bid_timestamp      bigint,
        bid_date           string,
        weekday            int,
        hour               int,
        minute             int,
        ad_id              string,
        width              int,
        height             int,
        filesize           int,
        os_id              string,
        browser_id         string,
        area_id            string,
        ip                 string,
        language           string,
        cookie_id          string,
        bd_sex             string,
        bd_interest        string,
        mz_interest        string)
        PARTITIONED BY (campaign string, log_date string)
        row format delimited fields terminated by '\001'
        location '/share_data/campaign_data';"

hadoop dfs -rmr -skipTrash $import_dir

hadoop jar $jar_path/$package com.mr.basedata.ImportData
echo "run ImportData over."

hadoop dfs -chmod 777 $tmp_dir

while [ $start_date != $end_date ]
do  
    rand_num=$(date +%s_%N)
    first_phase_out_path=$tmp_dir/$rand_num/media_info
    second_phase_out_path=$tmp_dir/$rand_num/cookie_info

    echo start_date=$start_date
    
    hadoop jar $jar_path/$package com.mr.basedata.MediaInfoMR $start_date Yes Yes $campaign $first_phase_out_path
    hadoop jar $jar_path/$package com.mr.basedata.CookieInfoMR $first_phase_out_path $second_phase_out_path
    if [ "$campaign" == "All" ];then
        hadoop fs -test -e $out_path/log_date=${start_date}
        if [ $? -eq 0 ]; then
             hadoop dfs -rmr -skipTrash $out_path/log_date=${start_date}
        fi

        hive -e "
            LOAD DATA INPATH '$second_phase_out_path/part*'
            INTO TABLE $table_name PARTITION(log_date='$start_date');"

        hadoop dfs -rmr -skipTrash $tmp_dir/$rand_num
        hadoop dfs -chmod 777 $out_path/log_date=${start_date}
    else
        hadoop fs -test -e $out_path/campaign=${campaign}/log_date=${start_date}
        if [ $? -eq 0 ]; then
             hadoop dfs -rmr -skipTrash $out_path/campaign=${campaign}/log_date=${start_date}
        fi

        hive -e "
            LOAD DATA INPATH '$out_path/campaign=${campaign}/$start_date/$campaign-r*'
            INTO TABLE $table_name PARTITION(campaign='${campaign}', log_date='$start_date');"

        hadoop dfs -rmr -skipTrash $tmp_dir/$rand_num
        hadoop dfs -chmod 777 $out_path/campaign=${campaign}/log_date=${start_date}
    fi

    start_date=$(date -d "$start_date +1 days" "+%Y-%m-%d")
done
