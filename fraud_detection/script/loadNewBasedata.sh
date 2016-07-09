jar_path=/data/rongyifei/svn/basedata/trunk/target
package=hadoop_test-1.0-jar-with-dependencies.jar

start_date=$1
end_date=$2
campaign_id=all

jar_dir=/data/rongyifei/svn/basedata/trunk/target
bid_input=/user/ads/log/original_bid/log_date=
click_input=/user/ads/log/original_click/log_date=
show_input=/user/ads/log/original_show/log_date=
win_input=/user/ads/log/original_win/log_date=
action_input=//user/hive/warehouse/dsp.db/original_count/log_date=

jar_name=hadoop_test-1.0-jar-with-dependencies.jar
import_data_class_name=com.mr.newbasedata.ImportData
basic_info_class_name=com.mr.newbasedata.BasicInfoMR
action_info_class_name=com.mr.newbasedata.ActionInfoMR
cookie_info_class_name=com.mr.newbasedata.CookieInfoMR

tmp_dir=/share_data/tmp
import_dir=$tmp_dir/dsp3_import
database=tmp

if [ "$campaign_id" == "all" ]; then
    table_name="algo_new_basedata"
    out_path=/share_data/new_basedata
else
    table_name='algo_new_campaigndata'
    out_path=/share_data/new_campaigndata
fi

#function ee(){
hive -e "
    use "$database";
    CREATE EXTERNAL TABLE if not exists "$table_name"(
        session_id         string,
        yoyi_cost          int,
        clk                int,
        reach              int,
        action             int,
        action_monitor_id  string,
        bid_way            string,
        algo_data          string,
        account_id         string,
        campaign_id        string,
        camp_cate_id       string,
        camp_sub_cate_id   string,
        order_id           string,
        ad_id              string,
        width              int,
        height             int,
        filesize           int,
        extName            string,
        adx_id             string,
        site_cate_id       string,
        content_cate_id    string,
        yoyi_cate_id       string,
        domain             string,
        host               string,
        url                string,
        page_title         string,
        adzone_id          string,
        adzone_posistion   string,
        reserve_price      int,
        bid_timestamp      bigint,
        bid_date           string,
        weekday            string,
        hour               string,
        minute             string,
        browser            string,
        os                 string,
        language           string,
        area_id            string,
        ip                 string,
        cookie_id          string,
        gender             string,
        profile            string)
        PARTITIONED BY (log_date string)
        row format delimited fields terminated by '\001'
        location '"$out_path"';"
#}

hadoop dfs -rmr -skipTrash $import_dir
hadoop jar $jar_path/$package $import_data_class_name $import_dir
echo plus data imported...
hadoop dfs -chmod 777 $tmp_dir

while [ $start_date != $end_date ]
do
    rand_num=$(date +%s_%N)
    basic_info_out=$tmp_dir/$rand_num/basic_info
    action_info_out=$tmp_dir/$rand_num/action_info
    cookie_info_out=$tmp_dir/$rand_num/cookie_info

    hadoop jar ${jar_dir}/$jar_name $basic_info_class_name $basic_info_out $import_dir $bid_input $click_input $show_input $win_input $start_date $campaign_id 

    #hadoop jar ${jar_dir}/$jar_name $cookie_info_class_name $basic_info_out $cookie_info_out $
    
    #hadoop jar ${jar_dir}/$jar_name $action_info_class_name /share_data/tmp/1444979511_433059868/basic_info /share_data/tmp/1444979511_433059868/action_info $import_dir $start_date $action_input
    
    if [ "$campaign_id" == "all" ]; then
        hadoop fs -test -e $out_path/log_date=${start_date}
        if [ $? -eq 0 ]; then
             hadoop dfs -rmr -skipTrash $out_path/log_date=${start_date}
        fi

        hive -e "
            use "$database";
            LOAD DATA INPATH '$basic_info_out/part*'
            INTO TABLE $table_name PARTITION(log_date='$start_date');"
    else
        echo to be finished
    fi

    hadoop dfs -rmr -skipTrash $tmp_dir/$rand_num
    start_date=$(date -d "$start_date +1 days" "+%Y-%m-%d")
done
