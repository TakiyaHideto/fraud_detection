package com.mr.code_backup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ArrayIndexOutOfBoundsException;
import java.lang.StringIndexOutOfBoundsException;
import java.text.ParseException;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.protobuffer.OriginalClickLog;
import com.mr.protobuffer.OriginalShowLog;
import com.mr.protobuffer.OriginalWinLog;
import com.mr.protobuffer.OriginalBidLog.OriginalBid;
import com.mr.protobuffer.OriginalClickLog.ClickLogMessage;
import com.mr.protobuffer.OriginalShowLog.ImpLogMessage;
import com.mr.protobuffer.OriginalWinLog.WinNoticeLogMessage;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.AuxiliaryFunction;
import com.mr.utils.TextMessageCodec;
import com.mr.config.Properties;
import com.mr.utils.StringUtil;
import com.mr.utils.UrlUtil; 

public class DataHandler {

	private Map<String,String> campaignTable = new HashMap<String,String>();
	private Map<String,String> orderTable = new HashMap<String,String>();
	private Map<String,String> advertTable = new HashMap<String,String>();
	private Map<String,String> mediaCateTable = new HashMap<String,String>();
	private Map<String,String> materialTable = new HashMap<String,String>();
	private Set<String> orderList = new HashSet<String>();

	public DataHandler(Map<String, String> campaignTable,
			Map<String, String> orderTable, 
			Set<String> orderList) {
		this.campaignTable = campaignTable;
		this.orderTable = orderTable;
		this.orderList = orderList;
	}
	public DataHandler(){}

	// bid log infomation handler
	public LinkedList<String> processBidLog(OriginalBid bidLog, Context context) throws ParseException {
		String featureValueStringAssemble = "";
		LinkedList<String> valueInfoDictList = new LinkedList<String>();

		// Counter
		Counter orderVolumeCt = context.getCounter("DataHandler", "orderVolumeCt");
		Counter failBidlogCt = context.getCounter("DataHandler", "failBidlogCt");
		Counter failBidlogExtractCt = context.getCounter("DataHandler", "failBidlogExtractCt");
		Counter pcBannerCt = context.getCounter("DataHandler", "pcBannerCt");
		Counter taobaoAdzoneCt = context.getCounter("DataHandler", "taobaoAdzoneCt");
		Counter taobaoSessionCt = context.getCounter("DataHandler", "taobaoSessionCt");
		Counter nullSiteCateExceptionCt = context.getCounter("DataHandler", "nullSiteCateExceptionCt");
		Counter nullContentCateExceptionCt = context.getCounter("DataHandler", "nullContentCateExceptionCt");
		Counter testCt = context.getCounter("DataHandler", "testCt");
		Counter testFlag1FailLogExtractCt = context.getCounter("DataHandler", "testFlag1FailLogExtractCt");
		Counter testFlag2FailLogExtractCt = context.getCounter("DataHandler", "testFlag2FailLogExtractCt");
		Counter testFlag3FailLogExtractCt = context.getCounter("DataHandler", "testFlag3FailLogExtractCt");
		Counter testFlag4FailLogExtractCt = context.getCounter("DataHandler", "testFlag4FailLogExtractCt");
		Counter testFlag5FailLogExtractCt = context.getCounter("DataHandler", "testFlag5FailLogExtractCt");
		Counter testFlag6FailLogExtractCt = context.getCounter("DataHandler", "testFlag6FailLogExtractCt");

		if (bidLog.getAdsCount() != 0)
		{
			for (OriginalBidLog.Ad ad : bidLog.getAdsList())
			{
				orderVolumeCt.increment(1);

				// output condition
				Boolean isPcBanner = false;
				Boolean tanxConversionOrder = false;

				// key info (for joining feature values)
				String orderId = "";
				String sessionId = bidLog.getSessionId();

				// basic info
				String timestamp = ""; // long
				String logType = "";
				String platForm = ""; // int
				String logDate = "";
				String weekday = "";
				String hour = "";

				// exchange info
				String exchangeAdxId = "";
				String exchangeUrlSiteCategory = ""; // List<String>
				String exchangeUrlContentCategory = ""; //List<String>

				// user info
				String userIp = "";
				String userArea = "";
				String userAgent = "";


				// adzone info
				String adzoneType = ""; // int
				String adzoneVisibility = "";
				String adzoneId = "";
				String height = "";
				String weight = "";

				// page info
				String pageUrl = "";
				String pageReferUrl = "";
				String pageSiteCategory = "";
				String host = "";
				String domain = "";

				// ad info
				String adAdzoneId = "";

				// matching info

				// algoextension info

				// here extract necessary information for feature value variable
				// basic info
				orderId = ad.getOrderId();
				timestamp = Long.toString(bidLog.getTimestamp());
				logType = bidLog.getLogType();
				platForm = String.valueOf(bidLog.getPlatform());
				DateUtil dateTime = new DateUtil();
				weekday = String.valueOf(dateTime.getTimeOfWeek(Long.parseLong(timestamp)));
	            hour = String.valueOf(dateTime.getTimeOfHour(Long.parseLong(timestamp)));
	            logDate = dateTime.getTimeOfDate(Long.parseLong(timestamp));
	            exchangeAdxId = bidLog.getExchange().getAdxId();

				// ad info
				adAdzoneId = ad.getAdzoneId();

				// adzone info
				for (OriginalBidLog.Adzone adzoneObject: bidLog.getAdzoneList()){
					if (adzoneObject.getAdzoneId().equals(adAdzoneId)){
						adzoneId = adzoneObject.getAdzoneId();
						adzoneType = String.valueOf(adzoneObject.getAdzoneType());
						adzoneVisibility = adzoneObject.getAdzoneVisuability();
						height = adzoneObject.getAdzoneHeight().split(Properties.Base.BS_SEPARATOR_STAR)[0];
						weight = adzoneObject.getAdzoneHeight().split(Properties.Base.BS_SEPARATOR_STAR)[1];
						break;
					}
				}

				if (adzoneType.equals("1") && platForm.equals("1") && !adzoneId.equals("") && exchangeAdxId.equals("1")){
					isPcBanner = true;
					pcBannerCt.increment(1);
				}
				else
					continue;

				try{
					adzoneId = adzoneId.split(Properties.Base.BS_SEPARATOR_UNDERLINE)[3];
				} catch (Exception e){
					continue;
				}

				if (orderList.contains(adzoneId)){
					tanxConversionOrder = true;
				}
				else
					continue;	

				// exchange info
				List<String> exchangeUrlSiteCategoryList = new LinkedList<String>();
				List<String> exchangeUrlContentCategoryList = new LinkedList<String>();
				for (String uSC : bidLog.getExchange().getUrlSiteCategoryList()){
					exchangeUrlSiteCategory += uSC + Properties.Base.BS_SEPARATOR_UNDERLINE;
				}
				for (String uCC : bidLog.getExchange().getUrlContentCategoryList()){
					exchangeUrlContentCategory += uCC + Properties.Base.BS_SEPARATOR_UNDERLINE;
				}
				try {
					exchangeUrlSiteCategory = exchangeUrlSiteCategory.substring(0,exchangeUrlSiteCategory.length()-1);
				} catch(StringIndexOutOfBoundsException e){
					nullSiteCateExceptionCt.increment(1);
				}
				try {
					exchangeUrlContentCategory = exchangeUrlContentCategory.substring(0,exchangeUrlContentCategory.length()-1);
				} catch(StringIndexOutOfBoundsException e){
					nullContentCateExceptionCt.increment(1);
				}

				// user info
				userIp = bidLog.getUser().getUserIp();
				userArea = bidLog.getUser().getUserArea();
				userAgent = bidLog.getUser().getUserAgent();

				// page info
				pageUrl = bidLog.getPage().getPageUrl();
				pageReferUrl = bidLog.getPage().getPageReferUrl();
				UrlUtil urlInfor = new UrlUtil();
				domain = urlInfor.getUrlDomain(pageUrl);
				host = urlInfor.getUrlHost(pageUrl);

				// ad info

				// matching info

				// algoextension info

				// extract info from campaignSQL and orderSQL
				// String[] orderSqlInfo = orderTable.get(orderId).split(Properties.Base.BS_SEPARATOR_TAB);
				// String orderIdSql = orderSqlInfo[0];
				// String campaignId = orderSqlInfo[1];
				// String orderType = orderSqlInfo[2];
				// String platType = orderSqlInfo[3];
				// String aim = orderSqlInfo[4];
				// String aimType = orderSqlInfo[5];
				// String aimVal = orderSqlInfo[6];
				// String scheduleData = orderSqlInfo[7];
				// String bidWay = orderSqlInfo[8];
				// String intelligentBid = orderSqlInfo[9];
				// String speed = orderSqlInfo[10];
				// String[] campaignSqlInfo = campaignTable.get(campaignId).split(Properties.Base.BS_SEPARATOR_TAB);
				// String campaignType = campaignSqlInfo[1];
				// featureValueStringAssemble = timestamp + "\t" 
				// 							+orderIdSql + "\t"
				// 							+campaignId + "\t"
				// 							+orderType + "\t"
				// 							+platType + "\t"
				// 							+aim + "\t"
				// 							+aimType + "\t"
				// 							+aimVal + "\t"
				// 							+scheduleData + "\t"
				// 							+bidWay + "\t"
				// 							+intelligentBid + "\t"
				// 							+speed + "&";

				// assemble feature values
				String dataType = "bidLog";
				featureValueStringAssemble = dataType + 
											Properties.Base.BS_SEPARATOR_TAB + orderId +
											Properties.Base.BS_SEPARATOR_TAB + height +
											Properties.Base.BS_SEPARATOR_TAB + weight +
											Properties.Base.BS_SEPARATOR_TAB + exchangeAdxId +
											Properties.Base.BS_SEPARATOR_TAB + timestamp +
											Properties.Base.BS_SEPARATOR_TAB + logDate +  
											Properties.Base.BS_SEPARATOR_TAB + weekday +
											Properties.Base.BS_SEPARATOR_TAB + hour +
											Properties.Base.BS_SEPARATOR_TAB + pageUrl + 
											Properties.Base.BS_SEPARATOR_TAB + domain + 
											Properties.Base.BS_SEPARATOR_TAB + host +
											Properties.Base.BS_SEPARATOR_TAB + userIp + 
											Properties.Base.BS_SEPARATOR_TAB + userAgent + 
											Properties.Base.BS_SEPARATOR_TAB + userArea + 
											Properties.Base.BS_SEPARATOR_TAB + exchangeUrlSiteCategory + 
											Properties.Base.BS_SEPARATOR_TAB + exchangeUrlContentCategory +
											Properties.Base.BS_SEPARATOR_TAB + adzoneId;

				if (tanxConversionOrder && isPcBanner){
					valueInfoDictList.add(featureValueStringAssemble);
					testCt.increment(1);
				}
			}
			return valueInfoDictList;
		} 
		else{
			failBidlogCt.increment(1);
			for (OriginalBidLog.Adzone adzoneObject: bidLog.getAdzoneList()){
				failBidlogExtractCt.increment(1);
				Boolean isPcBanner = false;
				Boolean tanxConversionOrder = false;

				// key info (for joining feature values)
				String orderId = "";
				String sessionId = bidLog.getSessionId();

				// basic info
				String timestamp = ""; // long
				String logType = "";
				String platForm = ""; // int
				String logDate = "";
				String weekday = "";
				String hour = "";

				// exchange info
				String exchangeAdxId = "";
				String exchangeUrlSiteCategory = ""; // List<String>
				String exchangeUrlContentCategory = ""; //List<String>

				// user info
				String userIp = "";
				String userArea = "";
				String userAgent = "";


				// adzone info
				String adzoneType = ""; // int
				String adzoneId = "";
				String height = "";
				String weight = "";

				// page info
				String pageUrl = "";
				String host = "";
				String domain = "";

				// basic info
				testFlag1FailLogExtractCt.increment(1);
				timestamp = Long.toString(bidLog.getTimestamp());
				logType = bidLog.getLogType();
				platForm = String.valueOf(bidLog.getPlatform());
				DateUtil dateTime = new DateUtil();
				weekday = String.valueOf(dateTime.getTimeOfWeek(Long.parseLong(timestamp)));
	            hour = String.valueOf(dateTime.getTimeOfHour(Long.parseLong(timestamp)));
	            logDate = dateTime.getTimeOfDate(Long.parseLong(timestamp));
				exchangeAdxId = bidLog.getExchange().getAdxId();

	            // adzone info
				testFlag4FailLogExtractCt.increment(1);
				adzoneId = adzoneObject.getAdzoneId();
				adzoneType = String.valueOf(adzoneObject.getAdzoneType());
				height = adzoneObject.getAdzoneHeight().split(Properties.Base.BS_SEPARATOR_STAR)[0];
				weight = adzoneObject.getAdzoneHeight().split(Properties.Base.BS_SEPARATOR_STAR)[1];

				if (adzoneType.equals("1") && platForm.equals("1") && !adzoneId.equals("") && exchangeAdxId.equals("1")){
					isPcBanner = true;
					pcBannerCt.increment(1);
				}
				else
					continue;

				try{
					adzoneId = adzoneId.split(Properties.Base.BS_SEPARATOR_UNDERLINE)[3];
				} catch (Exception e){
					continue;
				}

				if (orderList.contains(adzoneId)){
					tanxConversionOrder = true;
				}
				else
					continue;	


				// exchange info
				testFlag2FailLogExtractCt.increment(1);
				List<String> exchangeUrlSiteCategoryList = new LinkedList<String>();
				List<String> exchangeUrlContentCategoryList = new LinkedList<String>();
				for (String uSC : bidLog.getExchange().getUrlSiteCategoryList()){
					exchangeUrlSiteCategory += uSC + Properties.Base.BS_SEPARATOR_UNDERLINE;
				}
				for (String uCC : bidLog.getExchange().getUrlContentCategoryList()){
					exchangeUrlContentCategory += uCC + Properties.Base.BS_SEPARATOR_UNDERLINE;
				}
				try {
					exchangeUrlSiteCategory = exchangeUrlSiteCategory.substring(0,exchangeUrlSiteCategory.length()-1);
				} catch(StringIndexOutOfBoundsException e){
					nullSiteCateExceptionCt.increment(1);
				}
				try {
					exchangeUrlContentCategory = exchangeUrlContentCategory.substring(0,exchangeUrlContentCategory.length()-1);
				} catch(StringIndexOutOfBoundsException e){
					nullContentCateExceptionCt.increment(1);
				}

				// user info
				testFlag3FailLogExtractCt.increment(1);
				userIp = bidLog.getUser().getUserIp();
				userArea = bidLog.getUser().getUserArea();
				userAgent = bidLog.getUser().getUserAgent();
					
				// page info
				testFlag5FailLogExtractCt.increment(1);
				pageUrl = bidLog.getPage().getPageUrl();
				UrlUtil urlInfor = new UrlUtil();
				domain = urlInfor.getUrlDomain(pageUrl);
				host = urlInfor.getUrlHost(pageUrl);

				testFlag6FailLogExtractCt.increment(1);

				// assemble feature values
				String dataType = "bidLog";
				featureValueStringAssemble = dataType + 
											Properties.Base.BS_SEPARATOR_TAB + orderId +
											Properties.Base.BS_SEPARATOR_TAB + height +
											Properties.Base.BS_SEPARATOR_TAB + weight +
											Properties.Base.BS_SEPARATOR_TAB + exchangeAdxId +
											Properties.Base.BS_SEPARATOR_TAB + timestamp +
											Properties.Base.BS_SEPARATOR_TAB + logDate +  
											Properties.Base.BS_SEPARATOR_TAB + weekday +
											Properties.Base.BS_SEPARATOR_TAB + hour +
											Properties.Base.BS_SEPARATOR_TAB + pageUrl + 
											Properties.Base.BS_SEPARATOR_TAB + domain + 
											Properties.Base.BS_SEPARATOR_TAB + host +
											Properties.Base.BS_SEPARATOR_TAB + userIp + 
											Properties.Base.BS_SEPARATOR_TAB + userAgent + 
											Properties.Base.BS_SEPARATOR_TAB + userArea + 
											Properties.Base.BS_SEPARATOR_TAB + exchangeUrlSiteCategory + 
											Properties.Base.BS_SEPARATOR_TAB + exchangeUrlContentCategory +
											Properties.Base.BS_SEPARATOR_TAB + adzoneId;

				if (tanxConversionOrder && isPcBanner){
					valueInfoDictList.add(featureValueStringAssemble);
					testCt.increment(1);
				}
			}
		}
		return valueInfoDictList;
	}


	// // win log infomation handler
	public LinkedList<String> processWinLog(WinNoticeLogMessage winLog, Context context) throws ParseException{

		String featureValueStringAssemble = "";
		LinkedList<String> valueInfoDictList = new LinkedList<String>();

		// Counter

		// key info (for joining feature values)
		String idGroup = winLog.getData();
        String sessionId = AuxiliaryFunction.extractSessionId(idGroup);
        String orderId = AuxiliaryFunction.extractOrderId(idGroup);

		// initialize feature value variables
		// basic info
		String version = "";
		String timestamp = ""; // long
		String id = "";
		String data = "";
		String userIp = "";
		String userAgent = "";
		String referUrl = "";

		// here extract necessary information for feature value variable
		// basic info
		version = winLog.getVersion();
		timestamp = Long.toString(winLog.getTimestamp());
		id = winLog.getId();
		data = winLog.getData();
		userIp = winLog.getUserIp();
		userAgent = winLog.getUserAgent();
		referUrl = winLog.getReferUrl();

		// assemble feature values
		String dataType = "winLog";
		featureValueStringAssemble = dataType + 
									Properties.Base.BS_SEPARATOR_TAB + timestamp;

		valueInfoDictList.add(featureValueStringAssemble);
		return valueInfoDictList;
	}

	// // impression log infomation handler
	public LinkedList<String> processShowLog(ImpLogMessage showLog, Context context) throws ParseException{

		String featureValueStringAssemble = "";
		LinkedList<String> valueInfoDictList = new LinkedList<String>();

		// Counter

		// key info (for joining feature values)
		String idGroup = showLog.getData();
        String sessionId = AuxiliaryFunction.extractSessionId(idGroup);
        String orderId = AuxiliaryFunction.extractOrderId(idGroup);

		// initialize feature value variables
		// basic info
		String version = "";
		String timestamp = ""; // long
		String id = "";
		String data = "";
		String userIp = "";
		String userAgent = "";
		String referUrl = "";

		// here extract necessary information for feature value variable
		// basic info
		version = showLog.getVersion();
		timestamp = Long.toString(showLog.getTimestamp());
		id = showLog.getId();
		data = showLog.getData();
		userIp = showLog.getUserIp();
		userAgent = showLog.getUserAgent();
		referUrl = showLog.getReferUrl();

		// assemble feature values
		String dataType = "showLog";
		featureValueStringAssemble = dataType + 
									Properties.Base.BS_SEPARATOR_TAB + timestamp;

		valueInfoDictList.add(featureValueStringAssemble);
		return valueInfoDictList;
	}

	// // click log infomation handler
	public LinkedList<String> processClickLog(ClickLogMessage clickLog, Context context) throws ParseException{

		String featureValueStringAssemble = "";
		LinkedList<String> valueInfoDictList = new LinkedList<String>();

		// Counter

		// key info (for joining feature values)
		String idGroup = clickLog.getData();
        String sessionId = AuxiliaryFunction.extractSessionId(idGroup);
        String orderId = AuxiliaryFunction.extractOrderId(idGroup);

		// initialize feature value variables
		// basic info
		String version = "";
		String timestamp = ""; // long
		String id = "";
		String data = "";
		String userIp = "";
		String userAgent = "";
		String referUrl = "";

		// here extract necessary information for feature value variable
		// basic info
		version = clickLog.getVersion();
		timestamp = Long.toString(clickLog.getTimestamp());
		id = clickLog.getId();
		data = clickLog.getData();
		userIp = clickLog.getUserIp();
		userAgent = clickLog.getUserAgent();
		referUrl = clickLog.getReferUrl();

		// assemble feature values
		String dataType = "clickLog";
		featureValueStringAssemble = dataType + 
									Properties.Base.BS_SEPARATOR_TAB + timestamp;

		valueInfoDictList.add(featureValueStringAssemble);
		return valueInfoDictList;
	}
}




















