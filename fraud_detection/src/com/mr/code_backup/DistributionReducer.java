package com.mr.code_backup;

import java.io.IOException;
import java.text.ParseException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
import java.io.FileReader; 
import java.io.BufferedReader;
import java.net.URI;
import java.net.URLDecoder;
import java.lang.String;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs; 
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Counter;

import com.mr.utils.*;
import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.protobuffer.OriginalClickLog;
import com.mr.protobuffer.OriginalShowLog;
import com.mr.protobuffer.OriginalReachLog;
import com.mr.protobuffer.OriginalWinLog;
import com.mr.utils.TextMessageCodec;
import com.mr.utils.ProtobufMessageCodec;

public class DistributionReducer{
	public static class DistributionStatReducer extends Reducer<Text, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
            
            // initialize Counter
            Counter validOutputCt = context.getCounter("DistributionStatReducer", "validOutputCt");
            Counter adzoneCt = context.getCounter("DistributionStatReducer", "adzoneCt");
            Counter clickCt = context.getCounter("DistributionStatReducer", "clickCt"); // not used
            Counter showWinCt = context.getCounter("DistributionStatReducer", "showWinCt"); // not used
            Counter exchange13Ct = context.getCounter("DistributionStatReducer", "exchange13Ct"); // not used
            Counter exchange13BidInfoCt = context.getCounter("DistributionStatReducer", "exchange13BidInfoCt"); // not used
            Counter multipleBidCt = context.getCounter("DistributionStatReducer", "multipleBidCt"); // not used
            Counter multipleClickCt = context.getCounter("DistributionStatReducer", "multipleClickCt"); // not used
            Counter multipleShowCt = context.getCounter("DistributionStatReducer", "multipleShowCt"); // not used 
            Counter multipleWinCt = context.getCounter("DistributionStatReducer", "multipleWinCt"); // not used

            String valueTemp = "";

            HashMap<String, Integer> hourNumMap = new HashMap<String, Integer>();
            for(Text value: values){
                if(hourNumMap.containsKey(value.toString())){
                    int hourNum = hourNumMap.get(value.toString()) + 1;
                    hourNumMap.remove(value.toString());
                    hourNumMap.put(value.toString(), hourNum);
                }
                else {
                    int hourNum = 1;
                    hourNumMap.put(value.toString(), hourNum);
                }
            }

            Set<String> hourSet = hourNumMap.keySet();
            for(Iterator iter = hourSet.iterator(); iter.hasNext();){
                String hourkey = iter.next().toString();
                valueTemp += hourkey + ":" + String.valueOf(hourNumMap.get(hourkey)) + "\t";
            }
      
            value_result.set(valueTemp);
            context.write(key, value_result);
            validOutputCt.increment(1);            
        }       
    }
}











