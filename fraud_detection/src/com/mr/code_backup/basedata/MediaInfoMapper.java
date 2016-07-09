package com.mr.code_backup.basedata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

import com.mr.config.Properties;

public class MediaInfoMapper extends Mapper<LongWritable, Text, Text, Text> {

	Counter nomalCt = null;
	Counter parseExceptionCt = null;
	Counter dataExceptionCt = null;
	Counter dataLenExceptionCt = null;
	Counter impCt = null;
	
	/*----------------------------------------------------------------
	* 函数名:getIgCategory
	* 函数功能:获取全部行业的类目
	* 参数说明:
	* 返回值类型:void
	------------------------------------------------------------------*/
	protected void getIgCategory() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Properties.Mapper.MP_IG_CATEGORY_PATH))));
		String line;
		while((line = br.readLine())!=null){
			String [] arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length==3) {
				mMapIgCampaign.put(arrayValue[0], arrayValue[2]);
			}
		}
		br.close();
	}
	
	/*----------------------------------------------------------------
	* 函数名:getOrderInfo
	* 函数功能:活动订单和活动的对应信息
	* 参数说明:
	* 返回值类型:void
	------------------------------------------------------------------*/
	protected void getOrderInfo() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Properties.Mapper.MP_CAMPAGIN_PATH))));
		String line;
		while((line = br.readLine())!=null){
			String [] arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length==3) {
				mMapOrderCampaign.put(arrayValue[0], arrayValue[1]);
			}
		}
		br.close();
	}
	
	/*----------------------------------------------------------------
	* 函数名:getAdcountnfo
	* 函数功能:活动和广告主的对应信息
	* 参数说明:
	* 返回值类型:void
	------------------------------------------------------------------*/
	protected void getAdcountnfo() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Properties.Mapper.MP_CAMPAGIN_PATH))));
		String line;
		while((line = br.readLine())!=null){
			String [] arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length==3) {
				mMapCampaignAdcount.put(arrayValue[1], arrayValue[2]);
			}
		}
		br.close();
	}
	
	/*----------------------------------------------------------------
	* 函数名:getCampaginCategory
	* 函数功能:获取活动的分类信息
	* 参数说明:
	* 返回值类型:void
	------------------------------------------------------------------*/
	protected void getCampaginCategory() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Properties.Mapper.MP_CAMPAGIN_CATEGORY_PATH))));
		String line;
		while((line = br.readLine())!=null){
			String [] arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length==3) {
				mMapCampaignCategory.put(arrayValue[0], arrayValue[2]);
			}
		}
		br.close();
	}
	
	/*----------------------------------------------------------------
	* 函数名:getMediaAllCategory
	* 函数功能:获取媒体的类目
	* 参数说明:
	* 返回值类型:void
	------------------------------------------------------------------*/
	protected void getMediaAllCategory() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Properties.Mapper.MP_MEDIA_ALL_CATEGORY_PATH))));
		String line;
		while((line = br.readLine())!=null){
			String [] arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if ( arrayValue.length==4 ) {
				String strKey = arrayValue[1]+Properties.Base.BS_SEPARATOR+arrayValue[2];
				String strValue = arrayValue[0];
				mMapMediaAllCategory.put(strKey, strValue);
			}
		}
		br.close();
	}
	
	/*----------------------------------------------------------------
	* 函数名:getMediaYoyiCategory
	* 函数功能:获取媒体的类目
	* 参数说明:
	* 返回值类型:void
	------------------------------------------------------------------*/
	protected void getMediaYoyiCategory() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Properties.Mapper.MP_MEDIA_YOYI_CATEGORY_PATH))));
		String line;
		while((line = br.readLine())!=null){
			String [] arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length==3) {
				mMapMediaYoyiCategory.put(arrayValue[0], arrayValue[2]);
			}
		}
		br.close();
	}
	
	protected void getAdvertInfo() throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Properties.Mapper.MP_ADVERT_PATH))));
		String line = new String();
		String [] arrayValue;
		String strValue = "";
		while((line = br.readLine())!=null){
		    arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length==3) {
				strValue = arrayValue[1]+Properties.Base.BS_SEPARATOR+arrayValue[2];
				mMapAdvertInfo.put(arrayValue[0], strValue);
			}
		}
		br.close();
	}
	
	protected void getMaterialInfo() throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Properties.Mapper.MP_MATERIAL_PATH))));
		String line = new String();
		String [] arrayValue;
		String strValue = "";
		while((line = br.readLine())!=null){
		    arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length==4) {
				strValue = arrayValue[1] + Properties.Base.BS_SEPARATOR
						+  arrayValue[2] + Properties.Base.BS_SEPARATOR + arrayValue[3];
				mMapMaterialInfo.put(arrayValue[0], strValue);
			}
		}
		br.close();
	}
	
	protected void setup(Context context) throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		fs = FileSystem.get(conf);
		getIgCategory();
		getOrderInfo();
		getAdcountnfo();
		getCampaginCategory();
		getMediaAllCategory();
		getMediaYoyiCategory();
		getAdvertInfo();
		getMaterialInfo();
	} 
	
	protected void cleanup(Context context) throws IOException,InterruptedException{}
	
	/*----------------------------------------------------------------
	* 函数名:valide
	* 函数功能:判断行是否为有效行(字段必须大于3个基本字段)
	* 参数说明: 有效/true 无效/false
	* 返回值类型:boolean
	------------------------------------------------------------------*/
	public boolean valide(String strLine, List<String> listLines) {
		if (strLine == null || strLine.length() <= 0) {
			return false;
		}

		listLines.addAll(Arrays.asList(strLine.split(Properties.Base.BS_SEPARATOR, -1)));
		if (listLines.size() > 3) { // 至少含有 日志字段长度 日志版本 日志类型 字段
			return true;
		} else {
			return false;
		}
	}

	protected void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException{
		String strLine = value.toString();
		List<String> listLog = new  ArrayList<String>();

		dataExceptionCt = context.getCounter("MEDIA_MAP_COUNT", "dataExceptionCt");
		dataLenExceptionCt = context.getCounter("MEDIA_MAP_COUNT", "dataLenExceptionCt");
		parseExceptionCt = context.getCounter("MEDIA_MAP_COUNT", "parseExceptionCt");
		impCt = context.getCounter("MEDIA_MAP_COUNT", "impCt");
		if (!valide(strLine, listLog)){
			dataLenExceptionCt.increment(1);
			return ;
		}

		MediaInfoBusinessHandle bsHandle = new MediaInfoBusinessHandle(
				mMapCampaignCategory, mMapIgCampaign, mMapMediaAllCategory,
				mMapMediaYoyiCategory, mMapOrderCampaign, mMapCampaignAdcount,
				mMapCookieInfo,mMapAdvertInfo,mMapMaterialInfo);

		Map<String, String> mapKeyValues = new LinkedHashMap<String, String>();
		nomalCt = context.getCounter("MEDIA_MAP_COUNT", "nomalCt");

		boolean use_null  = context.getConfiguration().get("USE_NULL_FILTER").equals("Yes") ? true:false;
		boolean counter_cheat = context.getConfiguration().get("USE_CHEAT").equals("Yes") ? true:false;
		String strCampain = context.getConfiguration().get("USE_CAMPAINID");

		try {
			if ( bsHandle.mapHandle(listLog, mapKeyValues,strCampain,use_null,counter_cheat, impCt) 
				&& mapKeyValues.size() > 0 ) {
				Set<String> keys = mapKeyValues.keySet();
				for (String k : keys){
//					if(mapKeyValues.get(k).split(Properties.Base.BS_SEPARATOR, -1).length == Properties.Base.BS_IMP_VALUE_COUNT)
					nomalCt.increment(1);
					context.write(new Text(k), new Text(mapKeyValues.get(k)));
				}		
			}else{
				dataExceptionCt.increment(1);
			}
		} catch ( ParseException e ) {
			parseExceptionCt.increment(1);
			e.printStackTrace();
		}
	}
	
	private FileSystem fs = null;
	private Map<String,String> mMapIgCampaign = new HashMap<String,String>();
	private Map<String,String> mMapOrderCampaign = new HashMap<String,String>();
	private Map<String,String> mMapCampaignAdcount = new HashMap<String,String>();
	private Map<String,String> mMapCampaignCategory = new HashMap<String,String>();
	private Map<String,String> mMapMediaAllCategory = new HashMap<String,String>();
	private Map<String,String> mMapMediaYoyiCategory = new HashMap<String,String>();
	private Map<String,String> mMapCookieInfo = new HashMap<String,String>();
	private Map<String,String> mMapAdvertInfo = new HashMap<String,String>();
	private Map<String,String> mMapMaterialInfo = new HashMap<String,String>();
}
