package com.mr.config;

public class Properties {
	/********************基础配置***********************/
	public static class Base{
		public static final String  BS_DEFT_NAME     = "fs.default.name";
		public static final String  BS_HDFS_NAME     = "hdfs://hm.stat.dsp:9000";
		public static final String  BS_JOB_TRACKER   = "mapred.job.tracker";
		public static final String  BS_HDFS_TRACKER  = "hm.stat.dsp:9001";
		public static final String  BS_QUEUE_NAME    = "mapred.queue.name";
		public static final String  BS_MAPER_NAME    = "root";
		public static final String  BS_OUTPUT_COMPRESS = "mapred.output.compress";
		public static final String  BS_OUTPUT_COMPRESSION = "mapred.output.compression.codec";
		public static final int     BS_REDUCER_NUM   = 20;
		public static final String	BS_SEPARATOR_TAB = "\t";
		public static final String	BS_SEPARATOR_STAR = "\\*";
		public static final String	BS_SEPARATOR_UNDERLINE = "_";
		public static final String	BS_SEPARATOR_SPACE = " ";
		public static final String  BS_SEPARATOR     = "\001";
		public static final String  BS_SUB_SEPARATOR     = "\002";
		public static final String  BS_MR_JOINER     = "\003";
		public static final String	CTRL_A = "\001";
		public static final String	CTRL_B = "\002";
		public static final String	CTRL_C = "\003";
		public static final String	CTRL_D = "\004";
		public static final int     BS_IMP_VALUE_COUNT   = 30;
		public static final int     BS_FIRST_PHASE_OUT_COL_COUNT = 32;
		public static final int     BS_COOKIE_INPUT_COL_COUNT = 6;
		public static final int     BS_CROWD_TAG_LEN = 3;
		public static final int     BS_CLK_VALUE_COUNT   = 2;
		public static final int     BS_REACH_VALUE_COUNT   = 5;
		public static final int     BS_ACCESS_VALUE_COUNT   = 2;
	}
	
	public static class Comm{
		public static final int PC_BANNER_TYPE = 1;
		public static final String PC_BID_LOG_TYPE = "PC_BID_LOG_TYPE";
		public static final String PC_WIN_LOG_TYPE = "PC_WIN_LOG_TYPE";
		public static final String PC_SHOW_LOG_TYPE = "PC_SHOW_LOG_TYPE";
		public static final String PC_CLICK_LOG_TYPE = "PC_CLICK_LOG_TYPE";
		public static final int PC_SHOW_CHEAT_DELAY_THRESHOLD = 15;
		public static final int PC_REACH_CHEAT_DELAY_THRESHOLD = 45;
		public static final int PC_CONV_DELAY_DAY_COUNT = 10;
		public static final int PC_CLICK_CHEAT_DELAY_THRESHOLD = 3600;
		public static final int PC_REPEATED_CLICK_CHEAT_DAY_NUM_THRESHOLD = 3;
	}

	/********************Mapper配置***********************/
	public static class Mapper{
		public static final String MP_BID_DIR      = "/user/hive/warehouse/dsp.db/original_bid/log_date=";
		public static final String MP_SUCCESS_DIR  = "/user/hive/warehouse/dsp.db/log_access/log_date=";
		public static final String MP_IMPRESS_DIR  = "/user/hive/warehouse/dsp.db/log_show/log_date=";
		public static final String MP_CLICK_DIR    = "/user/hive/warehouse/dsp.db/log_click/log_date=";
		public static final String MP_REACH_DIR    = "/user/hive/warehouse/dsp.db/log_reach/log_date=";
		
		public static final String MP_CAMPAGIN_PATH             = "/share_data/tmp/import/campagin/ordercampaign";
		public static final String MP_CAMPAGIN_CATEGORY_PATH    = "/share_data/tmp/import/campagin/campagincategory";
		public static final String MP_IG_CATEGORY_PATH          = "/share_data/tmp/import/category/igcategory";
		public static final String MP_MEDIA_ALL_CATEGORY_PATH   = "/share_data/tmp/import/category/mediaallcategory";
		public static final String MP_MEDIA_YOYI_CATEGORY_PATH  = "/share_data/tmp/import/category/mediayoyicategory";
		public static final String MP_ADVERT_PATH               = "/share_data/tmp/import/category/advert";
		public static final String MP_MATERIAL_PATH             = "/share_data/tmp/import/category/material";
		public static final String MP_COOKIE_INFO_PATH          = "/user/hive/warehouse/dsp.db/crowd_tag/crowdtag/log_date=";

		public static final String MP_ORDER_PATH_NEW            = "/orderTable";
		public static final String MP_CAMPAIGN_PATH_NEW         = "/campaignTable";
		public static final String MP_ADVERT_PATH_NEW           = "/advertTable";
		public static final String MP_MATERIAL_PATH_NEW         = "/materialTable";
		public static final String MP_MEDIA_ALL_CATEGORY_PATH_NEW = "/mediaTable";
		public static final String MP_CPC_ORDER_PATH_NEW        = "/cpcOrders";
		
		public static final int ORDER_TABLE_LEN_NEW             = 11;
		public static final int CAMPAIGN_TABLE_LEN_NEW          = 5;
		public static final int ADVERT_TABLE_LEN_NEW            = 5;
		public static final int MATERIAL_TABLE_LEN_NEW          = 6;
		public static final int MEDIA_CATE_TABLE_LEN_NEW        = 4;
		
		public static final String BID_WAY_CPM = "0";
		public static final String BID_WAY_ICPM = "1";
		public static final String BID_WAY_CPC = "2";
		public static final String BID_WAY_CPA = "3";
	}
	
	/********************Reducer配置***********************/
	public static class Reducer{
		
	}

	/********************输出条件配置***********************/
	public static class OutputCondition{
		public static final String isPcBanner = "1";
	}
}










