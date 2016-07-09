table_name=benz_clickdetection_data
out_path=/share_data/benz_click_detection
source_path=/mroutput/dongjiahao/new_basedata/benz_detectionClick/log_date=2015-11-19 
database=tmp

hadoop dfs -rmr -skipTrash $out_path
hadoop dfs -mkdir $out_path

hive -e"
	DROP TABLE if exists $table_name;
"

hive -e"
	use "$database"; 
	create EXTERNAL TABLE if not exists "$table_name"(
		yoyi_cookie			string,
		accessTimestamp		string, 
		actionTimestamp		string,
		registerTimestamp	string, 
		clossTimestamp		string,
		accessReferer		string, 
		actionReferer		string, 
		userAgent			string,
		browserLanguage		string,
		actionFlag			int,
		clickTime			string)
		PARTITIONED BY (log_date string)
		row format delimited fields terminated by '\001'
		location '"$out_path"';"

hive -e "
        use "$database";
        LOAD DATA INPATH '$source_path/part*' 
        INTO TABLE $table_name PARTITION(log_date='2015-11-19');"