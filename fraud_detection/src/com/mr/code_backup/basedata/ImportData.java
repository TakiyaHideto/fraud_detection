package com.mr.code_backup.basedata;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.mr.config.Properties;
import com.mr.utils.Mysql;
import com.mr.utils.StringUtil;

public class ImportData {
	public static final String MYSQL_SERVER = "10.0.7.14";
	public static final String MYSQL_USER = "dsp_read_db01";
	public static final String MYSQL_PWD = "jl2k0ihons0oin230vhread";
	public static final String MYSQL_IP_READ_USER = "ipdb_read";
	public static final String MYSQL_IP_READ_PWD = "ke0UuYop";
	public static final String MYSQL_DB_CORE = "dsp_main2";
	public static final String MYSQL_DB_IP = "ip_database_v1";
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", Properties.Base.BS_HDFS_NAME);
		conf.set("mapred.job.tracker", Properties.Base.BS_HDFS_TRACKER);
		
		FileSystem fs = FileSystem.get(conf);
		
		Mysql mysql = new Mysql();
		mysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PWD, MYSQL_DB_CORE);
		
		Mysql mysql2 = new Mysql();
		mysql2.connect(MYSQL_SERVER, MYSQL_IP_READ_USER, MYSQL_IP_READ_PWD, MYSQL_DB_IP);
		
		
		// 加载订单和活动信息
		FSDataOutputStream output = fs.create(new Path(Properties.Mapper.MP_CAMPAGIN_PATH),true);
		ArrayList<HashMap<String, String>> list = mysql.fetchAll("select id,accountId,campaignId from order_base");
		for(HashMap<String, String> map : list){
			output.write(map.get("id").getBytes());
			output.write(Properties.Base.BS_SEPARATOR.getBytes());
			output.write(map.get("campaignId").getBytes());
			output.write(Properties.Base.BS_SEPARATOR.getBytes());
			output.write(map.get("accountId").getBytes());
			output.write("\n".getBytes());
		}
		output.close();
		
		// 加载活动和分类ID信息
		FSDataOutputStream out2 = fs.create(new Path(Properties.Mapper.MP_CAMPAGIN_CATEGORY_PATH),true);
		ArrayList<HashMap<String, String>> list2 = mysql.fetchAll("select id,categoryId1,categoryId2 from campaign");
		for(HashMap<String, String> map : list2){
			out2.write(map.get("id").getBytes());
			out2.write(Properties.Base.BS_SEPARATOR.getBytes());
			out2.write(map.get("categoryId1").getBytes());
			out2.write(Properties.Base.BS_SEPARATOR.getBytes());
			out2.write(map.get("categoryId2").getBytes());
			out2.write("\n".getBytes());
		}
		out2.close();
		
		// 加载活动所属的分类ID和Name信息
		FSDataOutputStream out3 = fs.create(new Path(Properties.Mapper.MP_IG_CATEGORY_PATH),true);
		ArrayList<HashMap<String, String>> list3 = mysql2.fetchAll("select id,parent,name from ig");
		for(HashMap<String, String> map : list3){
			out3.write(map.get("id").getBytes());
			out3.write(Properties.Base.BS_SEPARATOR.getBytes());
			out3.write(map.get("parent").getBytes());
			out3.write(Properties.Base.BS_SEPARATOR.getBytes());
			out3.write(map.get("name").getBytes());
			out3.write("\n".getBytes());
		}
		out3.close();
		
		// 加载媒体全部Exchange分类信息
		FSDataOutputStream out4 = fs.create(new Path(Properties.Mapper.MP_MEDIA_ALL_CATEGORY_PATH),true);
		ArrayList<HashMap<String, String>> list4 = mysql2.fetchAll("select yoyiId,sourceId,sourceCateid,sourceCatename from site_category_all");
		for(HashMap<String, String> map : list4){
			out4.write(map.get("yoyiId").getBytes());
			out4.write(Properties.Base.BS_SEPARATOR.getBytes());
			out4.write(map.get("sourceId").getBytes());
			out4.write(Properties.Base.BS_SEPARATOR.getBytes());
			out4.write(map.get("sourceCateid").getBytes());
			out4.write(Properties.Base.BS_SEPARATOR.getBytes());
			out4.write(map.get("sourceCatename").getBytes());
			out4.write("\n".getBytes());
		}
		out4.close();
		
		// 加载Yoyi媒体分类信息
		FSDataOutputStream out5 = fs.create(new Path(Properties.Mapper.MP_MEDIA_YOYI_CATEGORY_PATH),true);
		ArrayList<HashMap<String, String>> list5 = mysql2.fetchAll("select yoyiId,pId,name from site_category_yoyi");
		for(HashMap<String, String> map : list5){
			out5.write(map.get("yoyiId").getBytes());
			out5.write(Properties.Base.BS_SEPARATOR.getBytes());
			out5.write(map.get("pId").getBytes());
			out5.write(Properties.Base.BS_SEPARATOR.getBytes());
			out5.write(map.get("name").getBytes());
			out5.write("\n".getBytes());
		}
		out5.close();
		
//		FSDataOutputStream out9 = fs.create(new Path(Properties.Mapper.MP_STYLE_RTB_PATH),true);
//		ArrayList<HashMap<String, String>> list9 = mysql.fetchAll("select id,sourcesId,viewTypeId,positionId from style_rtb");
//		for(HashMap<String, String> map : list9){
//			out9.write(map.get("id").getBytes());
//			out9.write(Properties.Base.BS_SEPARATOR.getBytes());
//			out9.write(map.get("sourcesId").getBytes());
//			out9.write(Properties.Base.BS_SEPARATOR.getBytes());
//			out9.write(map.get("viewTypeId").getBytes());
//			out9.write(Properties.Base.BS_SEPARATOR.getBytes());
//			out9.write(map.get("positionId").getBytes());
//			out9.write("\n".getBytes());
//		}
//		out9.close();
		
		FSDataOutputStream out10 = fs.create(new Path(Properties.Mapper.MP_ADVERT_PATH),true);
		ArrayList<HashMap<String, String>> list10 = mysql.fetchAll("select id,styleId,materialId from advert");
		for(HashMap<String, String> map : list10){
			out10.write(map.get("id").getBytes());
			out10.write(Properties.Base.BS_SEPARATOR.getBytes());
			try{
				output.write(map.get("styleId").getBytes());
			} catch (java.lang.NullPointerException e) {
				output.write("".getBytes());
			}
			out10.write(Properties.Base.BS_SEPARATOR.getBytes());
			out10.write(map.get("materialId").getBytes());
			out10.write("\n".getBytes());
		}
		out10.close();
		
		FSDataOutputStream out11 = fs.create(new Path(Properties.Mapper.MP_MATERIAL_PATH),true);
	    ArrayList<HashMap<String, String>> list11 = mysql.fetchAll("select id,width,height,filesize from material");
	    String iFileSize = "";
	    for(HashMap<String, String> map : list11){
			out11.write(map.get("id").getBytes());
			out11.write(Properties.Base.BS_SEPARATOR.getBytes());
			out11.write(map.get("width").getBytes());
			out11.write(Properties.Base.BS_SEPARATOR.getBytes());
			out11.write(map.get("height").getBytes());
			out11.write(Properties.Base.BS_SEPARATOR.getBytes());
			iFileSize = StringUtil.isNull(String.valueOf(map.get("filesize"))) ? "0":map.get("filesize");
			out11.write(iFileSize.getBytes());
			out11.write("\n".getBytes());
		}
		out11.close();
	}
}
