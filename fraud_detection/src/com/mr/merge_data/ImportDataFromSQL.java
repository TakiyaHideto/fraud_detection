package com.mr.merge_data;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.utils.Mysql;

public class ImportDataFromSQL {
	// new SQL database
	public static final String DSP3_MYSQL_SERVER = "dbr.read.rmb";
	public static final String DSP3_MYSQL_USER = "data";
	public static final String DSP3_MYSQL_PWD = "8etABaLT8Qgz3eIl";
	public static final String DSP3_MYSQL_DB_CORE = "dsp3_main";
	
	// old SOL database
	public static final String DSP2_MYSQL_SERVER = "10.0.7.14";
	public static final String DSP2_MYSQL_USER = "ipdb_read";
	public static final String DSP2_MYSQL_PWD = "ke0UuYop";
	public static final String DSP2_MYSQL_DB_CORE = "ip_database_v1";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs(); 
		String importPath = otherArgs[0];

		conf.set("fs.default.name", Properties.Base.BS_HDFS_NAME);
		conf.set("mapred.job.tracker", Properties.Base.BS_HDFS_TRACKER);
			
		String orderTablePath = importPath + Properties.Mapper.MP_ORDER_PATH_NEW;
		String campaignTablePath = importPath + Properties.Mapper.MP_CAMPAIGN_PATH_NEW;
		String mediaCateTablePath = importPath + Properties.Mapper.MP_MEDIA_ALL_CATEGORY_PATH_NEW;
		String adTablePath = importPath + Properties.Mapper.MP_ADVERT_PATH_NEW;
		String materialTablePath = importPath + Properties.Mapper.MP_MATERIAL_PATH_NEW;
		
		FileSystem fs = FileSystem.get(conf);
		
		Mysql dsp3mysql = new Mysql();
		dsp3mysql.connect(DSP3_MYSQL_SERVER, DSP3_MYSQL_USER, DSP3_MYSQL_PWD, DSP3_MYSQL_DB_CORE);
		
		// Mysql dsp2mysql = new Mysql();
		// dsp2mysql.connect(DSP2_MYSQL_SERVER, DSP2_MYSQL_USER, DSP2_MYSQL_PWD, DSP2_MYSQL_DB_CORE);
		
		// 加载订单信息
		FSDataOutputStream output = fs.create(new Path(orderTablePath),true);
		ArrayList<HashMap<String, String>> list = dsp3mysql.fetchAll("select id, campaignId, type, platType, aim, aimType, aimVal, scheduleData, bidWay, intelligentBid, speed from `order`");
		for(HashMap<String, String> map : list){
			// try{
				output.write(map.get("id").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("campaignId").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("type").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("platType").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("aim").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("aimType").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("aimVal").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				try{
					output.write(map.get("scheduleData").getBytes());
				} catch(java.lang.NullPointerException e){
					output.write("-".getBytes());
				}
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("bidWay").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("intelligentBid").getBytes());
				output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
				output.write(map.get("speed").getBytes());
				output.write("\n".getBytes());
			// } catch(java.lang.NullPointerException e){
			// 	System.err.println("*****");
			// }
		}
		output.close();
		list.clear();
		
		// 加载广告活动信息
		output = fs.create(new Path(campaignTablePath),true);
		list = dsp3mysql.fetchAll("select id, type, tradeId, tradeSubId, status from `campaign`");
		for(HashMap<String, String> map : list){
			output.write(map.get("id").getBytes());
			output.write(Properties.Base.BS_SEPARATOR_TAB.getBytes());
			output.write(map.get("type").getBytes());
			// output.write(Properties.Base.BS_SEPARATOR.getBytes());
			// output.write(map.get("tradeId").getBytes());
			// output.write(Properties.Base.BS_SEPARATOR.getBytes());
			// output.write(map.get("tradeSubId").getBytes());
			// output.write(Properties.Base.BS_SEPARATOR.getBytes());
			// output.write(map.get("status").getBytes());	
			output.write("\n".getBytes());
		}
		output.close();
		list.clear();
		
		// 加载媒体全部Exchange分类信息
		// output = fs.create(new Path(mediaCateTablePath),true);
		// list = dsp2mysql.fetchAll("select yoyiId,sourceId,sourceCateid,sourceCatename from site_category_all");
		// for(HashMap<String, String> map : list){
		// 	output.write(map.get("yoyiId").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("sourceId").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("sourceCateid").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("sourceCatename").getBytes());
		// 	output.write("\n".getBytes());
		// }
		// output.close();
		// list.clear();
		
		// 加载广告信息
		// output = fs.create(new Path(adTablePath),true);
		// list = dsp3mysql.fetchAll("select id, creativeId, orderId, campaignId, status from `advert`");
		// for(HashMap<String, String> map : list){
		// 	output.write(map.get("id").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("creativeId").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("orderId").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("campaignId").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("status").getBytes());
		// 	output.write("\n".getBytes());
		// }
		// output.close();
		// list.clear();
		
		// 加载素材信息
		// output = fs.create(new Path(materialTablePath),true);
		// list = dsp3mysql.fetchAll("select id, creativeId, width, height, fileSize, extName from `material`");
		// for(HashMap<String, String> map : list){
		// 	output.write(map.get("creativeId").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("id").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("width").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("height").getBytes());
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	try{
		// 		output.write(map.get("fileSize").getBytes());
		// 	} catch (java.lang.NullPointerException e) {
		// 		output.write("0".getBytes());
		// 	}
		// 	output.write(Properties.Base.BS_SEPARATOR.getBytes());
		// 	output.write(map.get("extName").getBytes());		
		// 	output.write("\n".getBytes());
		// }
		// output.close();
		// list.clear();
	}
}
