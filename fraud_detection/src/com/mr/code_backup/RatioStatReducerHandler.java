package com.mr.code_backup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ArrayIndexOutOfBoundsException;
import java.text.ParseException;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Collections;
import java.util.Set;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
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
import com.mr.utils.TextMessageCodec;
import com.mr.config.Properties;
import com.mr.utils.StringUtil;
import com.mr.utils.UrlUtil;

public class RatioStatReducerHandler{

	public HashMap<String, Float> processRatioInfo(HashMap<String, HashSet<String>> adPageInfo, int impressionCounter) throws ParseException{
		
		HashMap<String, Float> ratioInfoMap = new HashMap<String, Float>();

			// 0:   sessionId
            // 1:   impression / click
            // 2:   timestamp Impression
            // 3:   timestamp Bid
            // 4:   url
            // 5:   userIP
            // 6:   userAgent
            // 7:   userArea
			// 8:	pageSiteCategory

		ratioInfoMap.put("Imp/IP", (float)impressionCounter/(float)adPageInfo.get("userIP").size());
		ratioInfoMap.put("Imp/UA", (float)impressionCounter/(float)adPageInfo.get("userAgent").size());
		ratioInfoMap.put("Imp/UserArea", (float)impressionCounter/(float)adPageInfo.get("userArea").size());
		ratioInfoMap.put("IP/UA", (float)adPageInfo.get("userIP").size()/(float)adPageInfo.get("userAgent").size());
		ratioInfoMap.put("IP/UserArea", (float)adPageInfo.get("userIP").size()/(float)adPageInfo.get("userArea").size());

		return ratioInfoMap;
	}

	public HashMap<String, Integer> collectTimestamp(LinkedList<String> impressionTimestamps) throws ParseException{
		
		HashMap<String, Integer> hourFrequencyMap = new HashMap<String, Integer>();

			// 0:   sessionId
            // 1:   impression / click
            // 2:   timestamp Impression
            // 3:   timestamp Bid
            // 4:   url
            // 5:   userIP
            // 6:   userAgent
            // 7:   userArea
			// 8:	pageSiteCategory

		DateUtil dateTime = new DateUtil();
		for (String impressionTimestamp: impressionTimestamps){
			int hour = dateTime.getTimeOfHour(Long.parseLong(impressionTimestamp));
			if (hourFrequencyMap.containsKey(String.valueOf(hour))){
				int frequency = hourFrequencyMap.get(String.valueOf(hour)) + 1;
				hourFrequencyMap.remove(String.valueOf(hour));
				hourFrequencyMap.put(String.valueOf(hour), frequency);
			} else{
				hourFrequencyMap.put(String.valueOf(hour), 1);
			}
		}
		return hourFrequencyMap;
	}

	public String assembleRatioData(HashMap<String, Float> ratioResultMap) throws ParseException{
		String assembleRatioResult = "";
		assembleRatioResult = "Imp/IP" + ":" + String.valueOf(ratioResultMap.get("Imp/IP")) + "\t"
							+"Imp/UA" + ":" + String.valueOf(ratioResultMap.get("Imp/UA")) + "\t"
							+"Imp/UserArea" + ":" + String.valueOf(ratioResultMap.get("Imp/UserArea")) + "\t"
							+"IP/UA" + ":" + String.valueOf(ratioResultMap.get("IP/UA")) + "\t"
							+"IP/UserArea" + ":" + String.valueOf(ratioResultMap.get("IP/UserArea")) + "\t";

		return assembleRatioResult; 
	}

	public String assembleImpressionFrequencyData(HashMap<String, Integer> frequencyInfoMap, LinkedList<String> clickSequence) throws ParseException{
		String assembleFrequencyResult = "";
		int impressionSum = 0;
		float impressionMean = 0;
		int impressionMax = 0;
		int impressionGap = 0;
		String peakHour = "";
		String clickHourSquence = "";
		int clickCounter = 0;
		double ctr = 0.0;
		Set<String> keys = frequencyInfoMap.keySet();
		Iterator<String> it = keys.iterator();
		while(it.hasNext()){
			String hour = it.next();
			int impressionNum = frequencyInfoMap.get(hour);
           	impressionSum += impressionNum;
           	if (impressionNum > impressionMax){
           		impressionMax = impressionNum;
           		peakHour = hour;
           	}
        }
        impressionMean = (float)impressionSum/24;
        impressionGap = impressionMax - (int)impressionMean;

        for(String clickHour: clickSequence){
        	clickHourSquence += clickHour + "-";
        	clickCounter ++;
        }
        ctr = (double)clickCounter/(double)impressionSum;
        assembleFrequencyResult = "ImpSum" + ":" + impressionSum + "\t"
        						+ "ImpMean" + ":" + impressionMean + "\t"
        						+ "ImpMax" + ":" + impressionMax + "\t"
        						+ "peakHour" + ":" + peakHour + "\t"
        						+ "ImpGap" + ":" + impressionGap + "\t"
        						+ "ctr" + ":" + ctr + "\t"
        						+ "clickSequence" + ":" + clickHourSquence;
        return assembleFrequencyResult;
	}
}




















