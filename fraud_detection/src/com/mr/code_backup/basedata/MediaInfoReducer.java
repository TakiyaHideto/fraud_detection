package com.mr.code_backup.basedata;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.StringUtil;

public class MediaInfoReducer extends Reducer<Text, Text, NullWritable, Text> {
    
	private Text result;
	Counter impCt;
	
	public void setup(Context context) {
		result = new Text();
	}

	public void cleanup(Context context) throws IOException {}

	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		impCt = context.getCounter("MEDIA_REDUCE_COUNT", "impCt");
		
		String sign = key.toString().trim().split(Properties.Base.BS_SEPARATOR, 2)[1];
		
		String yoyiCost = "0";
		String clk = "0";
		String reach = "0";
		String action = "0";
		String actionMonitorId = "";
		
		String accountId = "";
		String campaignId = "";
		String orderId = key.toString().trim().split(Properties.Base.BS_SEPARATOR, 2)[0];
		String industryCateId = "";
		
		String adxId = "";
		String domain = "";
		String host = "";
		String url = "";
		String adPosId = "";
		String contentId = "";
		String yoyiCateId = "";
		
		String bidTimestamp = "";
		String date = "";
		String weekday = "";
		String hour = "";
		String minute = "";
		
		String adId = "";
		String width = "";
		String height = "";
		String filesize = "";
		
		String os = "";
		String browser = "";
		String areaId = "";
		String ip = "";
		String language = "";
		
		String cookieId = "";
		
		String showActionOne = "";
		String showActionTwo = "";
		String showActionThree = "";
		
		String reachActionInfo = "";
		String reachActionOne = "";
		String reachActionTwo = "";
		String reachActionThree = "";

    	boolean ifGotShow = false;
    	boolean videoAdx = false;
    	
		for(Text val :values) {
			String[] info = val.toString().split(Properties.Base.BS_SEPARATOR, -1);
			if (info.length == Properties.Base.BS_IMP_VALUE_COUNT && info[0].equals("PC_EXPOSURE_LOG_TYPE")){
				cookieId = info[1];
				url = info[2];
				domain = info[3];
				host = info[4];
				contentId = info[5];
				yoyiCateId = info[6];
				browser = info[7];
				os = info[8];
				areaId = info[9];
				language = info[10];
				ip = info[11];
				adPosId = info[12];
				adxId = CommUtil.adxIdMappingOldToNew(info[13]);
				adId = info[14];
				campaignId = info[15];
				String tmpOrderId = info[16];
				industryCateId = info[17];
				width = info[18];
				height = info[19];
				filesize = info[20];
				bidTimestamp = info[21];
				date = info[22];
				weekday = info[23];
				hour = info[24];
				minute = info[25];
				accountId = info[26];
				showActionOne = info[27];
				showActionTwo = info[28];
				showActionThree = info[29];
				
				ifGotShow = true;
				
				if (StringUtil.isNull(adxId) || (adxId.length() == 3 && adxId.charAt(0) == '2'))
					videoAdx = true;
				
				impCt.increment(1);
			} else if (info.length == Properties.Base.BS_CLK_VALUE_COUNT && info[0].equals("PC_CLICK_LOG_TYPE")){
				clk = info[1];
			} else if (info.length == Properties.Base.BS_REACH_VALUE_COUNT && info[0].equals("PC_REACH_LOG_TYPE")){
				reach = "1";
				reachActionInfo = info[1];
				reachActionOne = info[2];
				reachActionTwo = info[3];
				reachActionThree = info[4];
			} else if (info.length == Properties.Base.BS_ACCESS_VALUE_COUNT && info[0].equals("PC_ACCESS_LOG_TYPE")){
				yoyiCost = info[1];
			} else {
				continue;
			}
		}
		
		if (ifGotShow && !videoAdx){
			if (!(reachActionInfo.equals(""))){ 
				actionMonitorId = reachActionInfo;
				if((showActionOne.equals(reachActionOne) && reachActionOne.equals(reachActionInfo)) 
						|| (showActionTwo.equals(reachActionTwo) && reachActionTwo.equals(reachActionInfo))
					    || (showActionThree.equals(reachActionThree) && reachActionThree.equals(reachActionInfo))){
					action = "1";					
				}
			}
			
			String strResult = sign 
					+ Properties.Base.BS_SEPARATOR + yoyiCost
					+ Properties.Base.BS_SEPARATOR + clk
					+ Properties.Base.BS_SEPARATOR + reach
					+ Properties.Base.BS_SEPARATOR + action
					+ Properties.Base.BS_SEPARATOR + actionMonitorId
					+ Properties.Base.BS_SEPARATOR + accountId
					+ Properties.Base.BS_SEPARATOR + campaignId
					+ Properties.Base.BS_SEPARATOR + orderId
					+ Properties.Base.BS_SEPARATOR + industryCateId
					+ Properties.Base.BS_SEPARATOR + adxId
					+ Properties.Base.BS_SEPARATOR + domain
					+ Properties.Base.BS_SEPARATOR + host
					+ Properties.Base.BS_SEPARATOR + url
					+ Properties.Base.BS_SEPARATOR + adPosId
					+ Properties.Base.BS_SEPARATOR + contentId
					+ Properties.Base.BS_SEPARATOR + yoyiCateId
					+ Properties.Base.BS_SEPARATOR + bidTimestamp
					+ Properties.Base.BS_SEPARATOR + date
					+ Properties.Base.BS_SEPARATOR + weekday
					+ Properties.Base.BS_SEPARATOR + hour
					+ Properties.Base.BS_SEPARATOR + minute
					+ Properties.Base.BS_SEPARATOR + adId
					+ Properties.Base.BS_SEPARATOR + width
					+ Properties.Base.BS_SEPARATOR + height
					+ Properties.Base.BS_SEPARATOR + filesize
					+ Properties.Base.BS_SEPARATOR + os
					+ Properties.Base.BS_SEPARATOR + browser
					+ Properties.Base.BS_SEPARATOR + areaId
					+ Properties.Base.BS_SEPARATOR + ip
					+ Properties.Base.BS_SEPARATOR + language
					+ Properties.Base.BS_SEPARATOR + cookieId;

			result.set(strResult);
			context.write(NullWritable.get(), result);			
		}
	}
}
