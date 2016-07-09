package com.mr.code_backup.basedata;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.lang.String;
import java.util.*;
import java.net.URLDecoder;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Counter;

import com.mr.utils.*;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.protobuffer.OriginalClickLog;
import com.mr.protobuffer.OriginalShowLog;
import com.mr.protobuffer.OriginalWinLog;
import com.mr.utils.DateUtil;
import com.mr.utils.TextMessageCodec;

public class NewTypeInfoText {

	/*input original bid*/
	public static class BidLogPbTagMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();
		@Override
		protected void map(Object key, Text value, Context context)	
				throws IOException, InterruptedException{

			TextMessageCodec TMC = new TextMessageCodec();
			String line = value.toString().trim();
			String[] fields = line.split("\001");
//			if(line.equals("") || fields.length < 3) return;
			Counter c1 = context.getCounter("my_counters", "input_bid");
            c1.increment(1);
            OriginalBidLog.OriginalBid bidLog;
			try{
				bidLog = (OriginalBidLog.OriginalBid) TMC
					.parseFromString(line,
							OriginalBidLog.OriginalBid.newBuilder());		
			}
			catch (Exception e){
                e.printStackTrace();
                return;
            }
			
			Counter filteredBidInputCt = context.getCounter("my_counters", "filteredBidInputCt");
			filteredBidInputCt.increment(1);
			
			//OriginalBidLog.OriginalBid.Builder bidLog = OriginalBidLog.OriginalBid.newBuilder();			
			//bidLog.mergeFrom(key.getBytes(), 0, key.getLength());

			String splitTypeOne = "\001";
			String splitTypeTwo = "\002";
			
			String session_id = bidLog.getSessionId();
			
			boolean isPcBanner = false;
			boolean isAlgoTraffic = false;
			
			//ad position information
			HashMap<String, String> map = new HashMap<String, String>();
			for(OriginalBidLog.Adzone a : bidLog.getAdzoneList()){
				String adzone_id = a.getAdzoneId();
				String adzone_height = a.getAdzoneHeight();
				String[] tmp = adzone_height.split("\\*", -1);
                String width = tmp[0];
                String height = tmp[1];
				int adzone_type  = a.getAdzoneType();
				int adzone_ad_count = a.getAdzoneAdCount();
				int adzone_positon = a.getAdzonePosition();
				String map_value  = width + splitTypeOne +
									height + splitTypeOne +
									Integer.toString(adzone_type) + splitTypeOne +
									Integer.toString(adzone_ad_count) + splitTypeOne +
									Integer.toString(adzone_positon);
				map.put(adzone_id, map_value);
                if (adzone_type == 1){
                	isPcBanner = true;
                }
			}
			
			if (isPcBanner){
				Counter bannerCt = context.getCounter("my_counters", "bannerCt");
				bannerCt.increment(1);				
			}
			
			//sys information
			String version = bidLog.getVersion();
			String log_type = bidLog.getLogType();
			int platform = bidLog.getPlatform();
			String bucket_id = bidLog.getBucketId();
			String host_nodes = bidLog.getHostNodes();
			boolean is_pmp = bidLog.getIsPmp();
			String strSys = version + splitTypeOne +
							log_type + splitTypeOne + 
							Integer.toString(platform) + splitTypeOne + 
							bucket_id + splitTypeOne + 
							host_nodes + splitTypeOne + 
							is_pmp;



			//time information
			long timestamp = bidLog.getTimestamp();
			int hour = 0;
			int week = 0;
			String date = "";
			try{
				hour = DateUtil.getTimeOfHour(timestamp);
				date = DateUtil.getTimeOfDate(timestamp);
				week = DateUtil.getTimeOfWeek(timestamp);
			}catch (ParseException e){
			}
			String strTime = Integer.toString(hour) + splitTypeOne + 
								date + splitTypeOne + 
								Integer.toString(week); 

			// exchange information
			OriginalBidLog.Exchange exchange = bidLog.getExchange();
			String exchange_id = exchange.getAdxId();
			String exchange_gender = exchange.getExchangeGender();
			String adx_bid_id = exchange.getAdxBidId();
		
			String exchange_interest = "";
			List<String> interestList = exchange.getCrowdTagsList();
			int ii = 0;
			for(String k : interestList){
				if(ii == interestList.size()-1){
					exchange_interest += k;
				}
				else{
					exchange_interest += k + splitTypeTwo;
				}
				ii += 1;
			}		
			String bd_sex = "";
			String bd_interest = "";
			String mz_interest = "";
			if(exchange_id.equals("7")){
				bd_sex = exchange_gender;
				bd_interest = exchange_interest;
			}
			else if(exchange_id.equals("4")){
				mz_interest = exchange_interest;
			}

			String ex_content_category = "";
			int ic = 0;
			List<String> contentCategotyList = exchange.getUrlContentCategoryList();	
			for(String c : contentCategotyList){
				if(ic == contentCategotyList.size() - 1){
					ex_content_category += c;
				}
				else{
					ex_content_category += c + splitTypeTwo;	
				}
				ic += 1;
			}

			String ex_site_category = "";
			int ie = 0;
			List<String> siteCategotyList = exchange.getUrlSiteCategoryList();
            for(String c : siteCategotyList){
				if(ie == siteCategotyList.size() - 1){
					ex_site_category += c;
				}
				else{
               		ex_site_category += c + splitTypeTwo;
				}
				ie += 1;
            }
			
			String strExchange =  exchange_id + splitTypeOne +
									adx_bid_id + splitTypeOne + 
									exchange_gender + splitTypeOne + 
									bd_interest + splitTypeOne +
									mz_interest + splitTypeOne + 
									ex_content_category + splitTypeOne +
									ex_site_category;
            
			
			
			// algo extension
			Counter algoTrafficCt = context.getCounter("my_counters", "algoTrafficCt");
			Counter truthfulTrafficCt = context.getCounter("my_counters", "truthfulTrafficCt");
			Counter linearTrafficCt = context.getCounter("my_counters", "linearTrafficCt");
			OriginalBidLog.AlgoExtension algoExt = bidLog.getAlgoInfo();
			for(OriginalBidLog.OrderRtpData algoData : algoExt.getOrderRtpList()){
				String ctrBucket = algoData.getCtrBucket();
				String bidBucket = algoData.getBidderBucket();
				if (!StringUtil.isNull(ctrBucket) && !StringUtil.isNull(bidBucket)){
					isAlgoTraffic = true;
					if(bidBucket.equals("1111")){
						truthfulTrafficCt.increment(1);
					} else if (bidBucket.equals("2222")){
						linearTrafficCt.increment(1);
					}
				}
			}
			
			if (isAlgoTraffic){
				algoTrafficCt.increment(1);
			}

			//page information
			OriginalBidLog.Page page = bidLog.getPage();
			String refer_url = page.getPageReferUrl();
			String url = page.getPageUrl();
			String domain = UrlUtil.getUrlDomain(url);
			String host = UrlUtil.getUrlHost(url);
			String page_title = page.getPageTitle();
			boolean is_wireless = page.getIsWireless();
			boolean is_video = page.getIsVideo();
			OriginalBidLog.WirelessExtension wirelessExtension = page.getWirelessExtension();
			OriginalBidLog.VideoExtension videoExtension = page.getVideoExtension();

			if(!is_video){
				Counter c2 = context.getCounter("my_counters", "not_video_input_num");
                c2.increment(1);	
			} 
			if(!is_wireless){
				Counter notWireCt = context.getCounter("my_counters", "not_wireless_input_num");
				notWireCt.increment(1);	
			} 
			//if(is_video) return;

			String strWirelessWxtension = "";
			if(is_wireless){
				String device_id = wirelessExtension.getDeviceId();
				int device_platform = wirelessExtension.getDevicePlatform();
				int device_os = wirelessExtension.getDeviceOs();
				String device_os_version = wirelessExtension.getDeviceOsVersion();
				String device_brand = wirelessExtension.getDeviceBrand();
				String device_model = wirelessExtension.getDeviceModel();
				String device_location = wirelessExtension.getDeviceLocation();
				String device_resolution = wirelessExtension.getDeviceResolution();
				int network_type = wirelessExtension.getNetworkType();
				int network_carrier =wirelessExtension.getNetworkCarrier();
				boolean isapp = wirelessExtension.getIsapp();
				String app_id = wirelessExtension.getAppId();
				String app_name = wirelessExtension.getAppName();
				String app_category = "";
				int iac = 0;
				List<String> appCategoryList = wirelessExtension.getAppCategoryList();
				for(String a : appCategoryList){
					if(iac == appCategoryList.size() - 1){
						app_category += a;
					}
					else{
						app_category += a + splitTypeTwo;
					}
					iac += 1;
				}
				strWirelessWxtension = device_id + splitTypeOne + 
										Integer.toString(device_platform) + splitTypeOne + 
										Integer.toString(device_os) + splitTypeOne +
										device_os_version + splitTypeOne + 
										device_brand + splitTypeOne + 
										device_model + splitTypeOne + 
										device_location + splitTypeOne +
										device_resolution + splitTypeOne + 
										Integer.toString(network_type) + splitTypeOne + 
										Integer.toString(network_carrier) + splitTypeOne +
										Boolean.toString(isapp) + splitTypeOne + 
										app_id + splitTypeOne + 
										app_name + splitTypeOne + 
										app_category;
			}
			else{
				strWirelessWxtension = "" + splitTypeOne + 
                                        "" + splitTypeOne + 
                                        "" + splitTypeOne +
                                     	"" + splitTypeOne + 
                                        "" + splitTypeOne + 
                                        "" + splitTypeOne + 
                                        "" + splitTypeOne +
                                        "" + splitTypeOne + 
                                        "" + splitTypeOne + 
                                        "" + splitTypeOne +
                                        "" + splitTypeOne + 
                                        "" + splitTypeOne + 
                                        "" + splitTypeOne +
										"";
			}

			String strVideoExtension = "";
			if(is_video){
				String video_title = videoExtension.getVideoTitle();
				int video_duration = videoExtension.getVideoDuration();
				String video_keywords = "";
				List<String> videoKeywordsList = videoExtension.getVideoKeywordsList();
				int ivk = 0;
				for(String v : videoKeywordsList){
					if(ivk == videoKeywordsList.size() - 1){
						video_keywords += v;
					}
					else{
						video_keywords += v + splitTypeTwo;
					}
					ivk += 1;
				}
				int video_start_time = videoExtension.getVideoStartTime();
				int video_ad_type = videoExtension.getVideoAdType();
				int video_ad_min_duration = videoExtension.getVideoAdMinDuration();
				int video_ad_max_duration = videoExtension.getVideoAdMaxDuration();
				strVideoExtension = video_title + splitTypeOne + 
									Integer.toString(video_duration) + splitTypeOne +
									video_keywords + splitTypeOne + 
									Integer.toString(video_start_time) + splitTypeOne +
									Integer.toString(video_ad_type) + splitTypeOne +
									Integer.toString(video_ad_min_duration) + splitTypeOne + 
									Integer.toString(video_ad_max_duration);
			}
			else{
				strVideoExtension =  "" + splitTypeOne +
                                     "" + splitTypeOne +
                                     "" + splitTypeOne +
                                     "" + splitTypeOne +
                                     "" + splitTypeOne +
                                     "" + splitTypeOne +
                                     ""; 
			}

			String yoyi_content_category ="";
			List<String> yoyiContentCategoryList = page.getPageContentCategoryList();
			int iyc = 0;
			for(String v : yoyiContentCategoryList){
				if(iyc== yoyiContentCategoryList.size() - 1){
					yoyi_content_category += v;
				}
				else{
					yoyi_content_category += v + splitTypeTwo;
				}
				iyc += 1;
			}
			String yoyi_site_category ="";
			List<String> yoyiSiteCategoryList = page.getPageSiteCategoryList();
			int iys = 0;
            for(String v : yoyiSiteCategoryList){
				if(iys == yoyiSiteCategoryList.size() - 1){
					yoyi_site_category += v;
				}
				else{
               		yoyi_site_category += v + splitTypeTwo;
				}
				iys += 1;
            }
			
			String strPage = url + splitTypeOne +
                                host + splitTypeOne +
                                domain + splitTypeOne +
                                refer_url + splitTypeOne +
                                page_title + splitTypeOne +
                                Boolean.toString(is_wireless) + splitTypeOne +
                                Boolean.toString(is_video) + splitTypeOne + 
								yoyi_content_category + splitTypeOne + 
								yoyi_site_category + splitTypeOne + 
								strWirelessWxtension + splitTypeOne + 
								strVideoExtension;

			//user information
			OriginalBidLog.User user = bidLog.getUser();
            String area_id = user.getUserArea();
            String user_ip = user.getUserIp();
            String user_agent = user.getUserAgent();
			String ex_cookie = user.getUserExid();
			String yoyi_cookie = user.getUserYyid();
			int user_gender = user.getUserGender();
			int user_crowd_tag_from = user.getUserCrowdTagFrom();
			String user_crowd_tags = "";
			int iuc = 0;
			List<String> user_crowd_tags_list = user.getUserCrowdTagsList();		
			for(String v : user_crowd_tags_list){
				if(iuc == user_crowd_tags_list.size() - 1){
					user_crowd_tags += v;
				}		
				else{
					user_crowd_tags += v + splitTypeTwo;
				}
				iuc += 1;
			}
			String strUser = ex_cookie + splitTypeOne + 
								yoyi_cookie + splitTypeOne + 
								area_id + splitTypeOne + 
								user_ip + splitTypeOne + 
								user_agent + splitTypeOne + 
								Integer.toString(user_gender) + splitTypeOne + 
								Integer.toString(user_crowd_tag_from) + splitTypeOne + 
								user_crowd_tags;	
			

			//ad information
			for(OriginalBidLog.Ad v : bidLog.getAdsList()){
				String customer_id = v.getCustomerId();
				String campaign_id = v.getCampaignId();
				String order_id = v.getOrderId();
				String ad_id = v.getAdId();
				//String ad_id = "";
				int ad_bid_price = v.getAdBidPrice();
				String adzone_id = v.getAdzoneId();
				String adzone_content = map.get(adzone_id);
				String strAd = customer_id + splitTypeOne +
								campaign_id + splitTypeOne + 
								ad_id + splitTypeOne +
								Integer.toString(ad_bid_price) + splitTypeOne + 
								adzone_id + splitTypeOne +
								adzone_content;				
				

				String strKeyResult = session_id + splitTypeOne + order_id;	
				String strValueResult = "PC_BID_LOG_TYPE" + "\003" + 
									strTime + splitTypeOne +
									strSys + splitTypeOne +
									strExchange + splitTypeOne +
									strPage + splitTypeOne +
									strUser + splitTypeOne +
									strAd;
				
				if (isPcBanner && isAlgoTraffic){
					Counter c10 = context.getCounter("my_counters", "all_bid");
	            	c10.increment(1);
					//String strValueResult = exchange_id + "\003" + adzone_id;
					key_result.set(strKeyResult);						
					value_result.set(strValueResult);
					context.write(key_result, value_result);				
				}
			}
		}
	}


	/*input click*/
	public static class ClickLogPbTagMapper extends Mapper<Object, Text, Text, Text>{
        private Text key_result = new Text();
        private Text value_result = new Text();
        
		@Override    
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
			TextMessageCodec TMC = new TextMessageCodec();
			OriginalClickLog.ClickLogMessage clickLog = (OriginalClickLog.ClickLogMessage) TMC
					.parseFromString(value.toString(),
							OriginalClickLog.ClickLogMessage.newBuilder());

            //OriginalClickLog.ClickLogMessage.Builder clickLog = OriginalClickLog.ClickLogMessage.newBuilder();
            //clickLog.mergeFrom(key.getBytes(), 0, key.getLength());   
			String version = clickLog.getVersion();
			String data = clickLog.getData();

			String oid = "";
			String sid = "";
			Counter c1 = context.getCounter("my_counters", "all_click");
            c1.increment(1);
			HashMap<String, String> map = new HashMap<String, String>();
						
			if (version.equals("1.0")) {
                String[] fields = data.split("&");
				for(String tp : fields){
					String[] kv = tp.split("=", 2);
					if (kv.length == 2) {
						map.put(kv[0], kv[1]);
					} else {
						continue;
					}
				}
            }
            else if (version.equals("2.0")) {
                String[] fields = data.split("=");
                String extD = URLDecoder.decode(fields[1], "UTF-8");

                String[] ext = extD.split("&");
				for(String tp : ext){
                    String[] kv = tp.split("=", 2);
                    if (kv.length == 2) {
                        map.put(kv[0], kv[1]);
                    } else {
                        continue;
                    }
                }
            }
			sid = map.get("sid");
			oid = map.get("oid");

            key_result.set(sid + "\001" + oid);
            value_result.set("PC_CLICK_LOG_TYPE" + "\003" + data);
            context.write(key_result, value_result);
        }
    }

	/*input show*/
    public static class ShowLogPbTagMapper extends Mapper<Object, Text, Text, Text>{
        private Text key_result = new Text();
        private Text value_result = new Text();

		@Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			TextMessageCodec TMC = new TextMessageCodec();
            OriginalShowLog.ImpLogMessage showLog = (OriginalShowLog.ImpLogMessage) TMC
                    .parseFromString(value.toString(),
                            OriginalShowLog.ImpLogMessage.newBuilder());

            //OriginalShowLog.ImpLogMessage.Builder showLog = OriginalShowLog.ImpLogMessage.newBuilder();
            //showLog.mergeFrom(key.getBytes(), 0, key.getLength());

			String version = showLog.getVersion();
            String data = showLog.getData();     
			String oid = "";
            String sid = "";
			Counter allShowCt = context.getCounter("my_counters", "all_show");
			allShowCt.increment(1);
			HashMap<String, String> map = new HashMap<String, String>();
		
			if (version.equals("1.0")) {
                String[] fields = data.split("&" ,-1);
				for(String tp : fields){
					String[] kv = tp.split("=", 2);
					if (kv.length == 2) {
						map.put(kv[0], kv[1]);
					} else {
						continue;
					}
				}
            }
            else if (version.equals("2.0")) {
                String[] fields = data.split("=",2);
                String extD = URLDecoder.decode(fields[1], "UTF-8");

                String[] ext = extD.split("&", -1);
                for(String tp : ext){
                    String[] kv = tp.split("=", 2);
                    if (kv.length == 2) {
                        map.put(kv[0], kv[1]);
                    } else {
                        continue;
                    }
                }
            }            
			sid = map.get("sid");
			oid = map.get("oid");

            key_result.set(sid + "\001" + oid);
            value_result.set("PC_EXPOSURE_LOG_TYPE" + "\003" + data);
            context.write(key_result, value_result);
        }
    }


	//input win notice
    public static class WinLogPbTagMapper extends Mapper<Object, Text, Text, Text>{
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            TextMessageCodec TMC = new TextMessageCodec();
            OriginalWinLog.WinNoticeLogMessage winLog = (OriginalWinLog.WinNoticeLogMessage) TMC
                    .parseFromString(value.toString(),
                            OriginalWinLog.WinNoticeLogMessage.newBuilder());

            String version = winLog.getVersion();
            String data = winLog.getData();
            String oid = "";
            String sid = "";
            String cost = "0";
            Counter allWinCt = context.getCounter("my_counters", "all_win");
            Counter costCt = context.getCounter("my_counters", "win_cost");
            Counter gotCostCt = context.getCounter("my_counters", "gotCostCt");
            Counter excCostCt = context.getCounter("my_counters", "excCostCt");
            Counter zeroCostCt = context.getCounter("my_counters", "zeroCostCt");
            allWinCt.increment(1);
			HashMap<String, String> kvMap = new HashMap<String, String>();

            if (version.equals("1.0")) {
                String[] fields = data.split("&", -1);
				for(String tp : fields){
					String[] kv = tp.split("=", 2);
					if (kv.length == 2) {
						kvMap.put(kv[0], kv[1]);
						if (kv[0].equals("settle_price")){
							cost = kv[1];
							gotCostCt.increment(1);
						}
					} else {
						continue;
					}
				}
            }
            else if (version.equals("2.0")) {
                String[] fields = data.split("=",2);
				String extD = URLDecoder.decode(fields[1], "UTF-8");

                String[] ext = extD.split("&", -1);
				for(String tp : ext){
                    String[] kv = tp.split("=", 2);
                    if (kv.length == 2) {
                    	kvMap.put(kv[0], kv[1]);
						if (kv[0].equals("settle_price")){
							cost = kv[1];
							gotCostCt.increment(1);
						}
                    } else {
                        continue;
                    }
                }
            }
            
//            String price = new String("0");
//            if (kvMap.containsKey("settle_price")){
//            	String tmp = kvMap.get("﻿settle_price");
//                if (tmp.matches("\\d+") && tmp != null){

//                	price = tmp;
//                }
//            }
            if (checkNumber(cost)){
            	long tmp = Long.parseLong(cost);
            	if (tmp > 0)
            		costCt.increment(tmp);
            	else
            		zeroCostCt.increment(1);
            } else {
            	cost = "0";
            	excCostCt.increment(1);
            }
            
			sid = kvMap.get("sid");
			oid = kvMap.get("oid");

            key_result.set(sid + "\001" + oid);
            value_result.set("PC_WIN_LOG_TYPE" + "\003" + cost);
            context.write(key_result, value_result);
        }
    }

	public static boolean checkNumber(String strSource) {
		if (strSource.length() < 1) {
			return false;
		}
		return strSource.matches("\\d+");
	}

    public static class Reduce extends Reducer<Text,Text,NullWritable,Text>{
		private Text result = new Text();

		@Override
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{  
			int show_count = 0;
			int click_count = 0;
			int win_count = 0;
			int bid_count = 0;
			String bid_info = "";
			int i_count = 0;
			int is_cheat = 0;
			long cost = 0;

			for(Text t:values){
				String[] fds = t.toString().split("\003", -1);
				String type = fds[0];
				String data = fds[1];
		
				if(type.equals("PC_WIN_LOG_TYPE")){
					win_count = 1;
					cost = Long.parseLong(data);
				}
				else if(type.equals("PC_EXPOSURE_LOG_TYPE")){
                    show_count = 1;
                }
                else if(type.equals("PC_CLICK_LOG_TYPE")){
                    click_count = 1;
                    i_count += 1;
                }
				else if(type.equals("PC_BID_LOG_TYPE")){
					bid_count = 1;
					bid_info = data;
				} 
			}
		
			if(win_count > 0){
				Counter c3 = context.getCounter("reduce_count", "unique_win_num");
				c3.increment(1);
			}
			if(bid_count > 0){
				Counter c4 = context.getCounter("reduce_count", "unique_bid_num");
                c4.increment(1);
			}
            if(show_count > 0){
                Counter c8 = context.getCounter("reduce_count", "unique_show_num");
                c8.increment(1);
            }
			if(click_count > 0){
                Counter c9 = context.getCounter("reduce_count", "unique_click_num");
                c9.increment(1);
            }
			
			if(bid_count > 0 && show_count > 0){
                Counter c6 = context.getCounter("reduce_count", "show_in_bid_num");
                c6.increment(1);
            }
			if(bid_count > 0 && click_count > 0){
                Counter c7 = context.getCounter("reduce_count", "click_in_bid_num");
                c7.increment(1);
            }
			if(bid_count > 0 && win_count > 0){
				Counter c5 = context.getCounter("reduce_count", "win_in_bid_num");
            	c5.increment(1);
				Counter costCt = context.getCounter("reduce_count", "costCt");
                costCt.increment(cost);
                Counter rtbZeroCostCt = context.getCounter("reduce_count", "rtbZeroCostCt");
                if (cost == 0){
                	rtbZeroCostCt.increment(1);
                }

				String strResult = key.toString() + "\001"
                                    + show_count + "\001"
                                    + click_count + "\001"
                                    + is_cheat + "\001"
                                    + bid_info;
                result.set(strResult);
                context.write(NullWritable.get(), result);
			}

			/*if(show_count > 0 || click_count > 0){
				String strResult = key.toString() + "\001"
                                    + show_count + "\001"
                                    + click_count + "\001"
                                    + is_cheat + "\001"
                                    + bid_info;
                result.set(strResult);
                context.write(NullWritable.get(), result);

                if(i_count > 1){
                    for(int i = 1;i < i_count;i ++){
                        is_cheat = 4;
                        strResult = key.toString() + "\001"
                                    + show_count + "\001"
                                    + click_count + "\001"
                                    + is_cheat + "\001"
                                    + bid_info;
                        result.set(strResult);
                        context.write(NullWritable.get(), result);
                    }
                }
			}*/
        }	
	}
          
	static void addInputFileComm(Job job,FileSystem fs,String filePaths, Class classname) throws IOException {
        Path path = new Path(filePaths);
        if (fs.exists(path)) {
            FileStatus[] fileStatus = fs.listStatus(path);
            for (int i = 0; i < fileStatus.length; i ++) {
                FileStatus fileStatu = fileStatus[i];
                if (fileStatu.isDir()) {
                    addInputFileComm(job,fs,fileStatu.getPath().toString(), classname);
                } else {
					String strPath = fileStatu.getPath().toString();
//                    if (fileStatu.getLen() > 0 && !strPath.contains("log_type=2")) {
					if (fileStatu.getLen() > 0 && !strPath.contains("log_type=2") && !strPath.contains("pmp")) {
                        System.out.println(fileStatu.getPath());
                        MultipleInputs.addInputPath(job, fileStatu.getPath(), TextInputFormat.class, classname);
                    }
                }
            }
        }
    }


    /** 
     * @param args 
     */  
    public static void main(String[] args) throws Exception{  
        // TODO Auto-generated method stub  
        Configuration conf = new Configuration();  
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
        if(otherArgs.length != 7){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"NewTypeInfo");  
		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        job.setJarByClass(NewTypeInfoText.class);
        //job.setCombinerClass(Reduce.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
		//job.setOutputKeyClass(Text.class);
        //job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class); 
		job.setNumReduceTasks(50);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		
		FileSystem fs = FileSystem.get(conf);
		String filePathBid = otherArgs[1];
		String filePathclick = otherArgs[2];
		String filePathShow = otherArgs[3];
		String filePathWin = otherArgs[4];
		
		String currentDate = otherArgs[5];
		int period = Integer.parseInt(otherArgs[6]);  
		
		for (int i=0; i < period; i++) {
			System.out.println("Current date " + currentDate);
			addInputFileComm(job,fs,filePathBid + currentDate, BidLogPbTagMapper.class);
			addInputFileComm(job,fs,filePathclick + currentDate, ClickLogPbTagMapper.class);
			addInputFileComm(job,fs,filePathShow + currentDate, ShowLogPbTagMapper.class);
			addInputFileComm(job,fs,filePathWin + currentDate, WinLogPbTagMapper.class);
			currentDate = DateUtil.getSpecifiedDayAfter(currentDate);
		}
		
//		addInputFileComm(job,fs,filePathBid, BidLogPbTagMapper.class);
//		addInputFileComm(job,fs,filePathclick, ClickLogPbTagMapper.class);
//		addInputFileComm(job,fs,filePathShow, ShowLogPbTagMapper.class);
//		addInputFileComm(job,fs,filePathWin, WinLogPbTagMapper.class);

		//SequenceFileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
}

