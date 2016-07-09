package com.mr.code_backup;

import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
import java.lang.String;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class RatioStatReducer{
	public static class RatioStatFraudReducer extends Reducer<Text, MapWritable, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {

            HashMap<String, HashSet<String>> dataInfoMap = new HashMap<String, HashSet<String>>(); 
            HashMap<String, Float> ratioResultMap = new HashMap<String, Float>();
            HashMap<String, Integer> frequencyInfoMap = new HashMap<String, Integer>();
            String assembleResult = "";

            // Info variables
            HashSet<String> uniqueUserIP = new HashSet<String>();
            HashSet<String> uniqueUserAgent = new HashSet<String>();
            HashSet<String> uniqueUserArea = new HashSet<String>();
            LinkedList<String> impressionTimestamps = new LinkedList<String>();
            LinkedList<String> clickSequence = new LinkedList<String>();
            int impressionCounter =  0;
            int clickCounter = 0;

                // 0:   sessionId
                // 1:   impression / click
                // 2:   timestamp Impression
                // 3:   timestamp Bid
                // 4:   url
                // 5:   userIP
                // 6:   userAgent
                // 7:   userArea
                // 8:   pageSiteCategory

            for (MapWritable value: values){
            	impressionCounter ++;
                if (value.get(new Text("impressionClick")).toString().equals("click")){
                    clickCounter ++;
                    clickSequence.add(value.get(new Text("hour")).toString());
                }
                impressionTimestamps.add(value.get(new Text("timestampImpression")).toString());
                uniqueUserIP.add(value.get(new Text("userIP")).toString());
                uniqueUserAgent.add(value.get(new Text("userAgent")).toString());
                uniqueUserArea.add(value.get(new Text("userArea")).toString());
            }
            dataInfoMap.put("userIP", uniqueUserIP);
            dataInfoMap.put("userAgent", uniqueUserAgent);
            dataInfoMap.put("userArea", uniqueUserArea);

            try{
                RatioStatReducerHandler ratioStatReducerHandler = new RatioStatReducerHandler();
                ratioResultMap = ratioStatReducerHandler.processRatioInfo(dataInfoMap, impressionCounter);
                frequencyInfoMap = ratioStatReducerHandler.collectTimestamp(impressionTimestamps);
                assembleResult = ratioStatReducerHandler.assembleRatioData(ratioResultMap);
                // assembleResult = ratioStatReducerHandler.assembleImpressionFrequencyData(frequencyInfoMap, clickSequence);
            } catch (ParseException e){
                return;
            }      
            
            value_result.set(assembleResult);
            context.write(key, value_result);
            
        }
    }
}












