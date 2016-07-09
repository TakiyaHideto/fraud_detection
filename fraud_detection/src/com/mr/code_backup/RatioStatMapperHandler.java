package com.mr.code_backup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ArrayIndexOutOfBoundsException;
import java.text.ParseException;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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

public class RatioStatMapperHandler {
	
	private String dataInfo = new String();
	private MapWritable dataInfoMap = new MapWritable();

	public RatioStatMapperHandler(String dataInfo){
		this.dataInfo = dataInfo;
	}

	public String getDataInfo(){
	       return this.dataInfo;
	}

	public MapWritable processDataInfo(String adPageInfo) throws ParseException{
		
        String[] dataElements = adPageInfo.split("\t");
        // 0:	sessionId
        // 1:	impression / click
        // 2:	timestamp Impression
        // 3:	timestamp Bid
        // 4:	url
        // 5:	userIP
        // 6:	userAgent
        // 7:	userArea
        // 8:   pageSiteCategory

        // Handle info
        DateUtil dateTime = new DateUtil();
        int hour = dateTime.getTimeOfHour(Long.parseLong(dataElements[2]));

        dataInfoMap.put(new Text("impressionClick"), new Text(dataElements[1]));
        dataInfoMap.put(new Text("timestampImpression"), new Text(dataElements[2]));
        dataInfoMap.put(new Text("hour"), new Text(String.valueOf(hour)));
        dataInfoMap.put(new Text("userIP"), new Text(dataElements[5]));
        dataInfoMap.put(new Text("userAgent"), new Text(dataElements[6]));
        dataInfoMap.put(new Text("userArea"), new Text(dataElements[7]));
        // dataInfoMap.put(new Text("pageSiteCategory"), new Text(dataElements[8]));

        return dataInfoMap;
	}
}
















