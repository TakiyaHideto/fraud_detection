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

public class BenzDataHandler{
	public BenzDataHandler(){}

	public LinkedList<String> processBidLog(OriginalBid bidLog, Context context) throws ParseException {
		Counter orderVolumeCt = context.getCounter("BenzDataHandler","orderVolumeCt");
		Counter pcBannerCt = context.getCounter("BenzDataHandler","pcBannerCt");
		Counter nullSiteCateExceptionCt = context.getCounter("BenzDataHandler","nullSiteCateExceptionCt");
		Counter nullContentCateExceptionCt = context.getCounter("BenzDataHandler","nullContentCateExceptionCt");
		Counter testCt = context.getCounter("BenzDataHandler","testCt");
		String featureValueStringAssemble = "";
		LinkedList<String> valueInfoDictList = new LinkedList<String>();

		for (OriginalBidLog.Ad ad : bidLog.getAdsList())
		{
			orderVolumeCt.increment(1);

			// output condition
			Boolean isPcBanner = false;
			Boolean benzOrder3369 = false;
			Boolean benzOrder3370 = false;

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

			// here extract necessary information for feature value variable
			// basic info
			orderId = ad.getOrderId();
			if (orderId.equals("3369")){
				benzOrder3369 = true;
			}
			else if (orderId.equals("3370")){
				benzOrder3370 = true;
			}
			else
				continue;
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

			// assemble feature values
			featureValueStringAssemble = 
										Properties.Base.BS_SEPARATOR + orderId +
										Properties.Base.BS_SEPARATOR + height +
										Properties.Base.BS_SEPARATOR + weight +
										Properties.Base.BS_SEPARATOR + exchangeAdxId +
										Properties.Base.BS_SEPARATOR + timestamp +
										Properties.Base.BS_SEPARATOR + logDate +  
										Properties.Base.BS_SEPARATOR + weekday +
										Properties.Base.BS_SEPARATOR + hour +
										Properties.Base.BS_SEPARATOR + pageUrl + 
										Properties.Base.BS_SEPARATOR + domain + 
										Properties.Base.BS_SEPARATOR + host +
										Properties.Base.BS_SEPARATOR + userIp + 
										Properties.Base.BS_SEPARATOR + userAgent + 
										Properties.Base.BS_SEPARATOR + userArea + 
										Properties.Base.BS_SEPARATOR + exchangeUrlSiteCategory + 
										Properties.Base.BS_SEPARATOR + exchangeUrlContentCategory +
										Properties.Base.BS_SEPARATOR + adzoneId;

			if (isPcBanner){
				valueInfoDictList.add(featureValueStringAssemble);
				testCt.increment(1);
			}
		}
		return valueInfoDictList;
	}
}