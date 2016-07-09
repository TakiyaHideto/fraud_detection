table_name=diyuanxin_data
out_path=/share_data/diyuanxin_data/log_date=2015-11-20~2015-11-26
source_path=/share_data/diyuanxin_tag_data
database=tmp

hadoop dfs -rmr -skipTrash $out_path
hadoop dfs -mkdir $out_path

hive -e"
	DROP TABLE if exists $table_name;
"

hive -e"
	use "$database"; 
	create EXTERNAL TABLE if not exists "$table_name"(
		yoyi_cookie		string,
		segment_tag		string,
		log_date		string,
		hour			int,
		min				int)
		row format delimited fields terminated by '\001' 
		location '"$out_path"';"

hive -e "
        use "$database";
        LOAD DATA INPATH '$source_path/part*' 
        INTO TABLE $table_name;"