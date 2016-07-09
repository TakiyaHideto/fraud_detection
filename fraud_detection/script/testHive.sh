out_path=/mroutput/dongjiahao/new_basedata
database=tmp
table_name="new_basedata_filter_test"

hive -e"
    DROP TABLE if exists $table_name;
"

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

datapath=/mroutput/dongjiahao/new_basedata/test_2015-11-23

hive -e "
    use "$database";
    LOAD DATA INPATH '$datapath/part*'
    INTO TABLE $table_name PARTITION(log_date='2015-11-23');"


