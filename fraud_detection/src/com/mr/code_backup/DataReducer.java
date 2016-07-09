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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.protobuffer.OriginalClickLog;
import com.mr.protobuffer.OriginalShowLog;
import com.mr.protobuffer.OriginalReachLog;
import com.mr.protobuffer.OriginalWinLog;
import com.mr.utils.TextMessageCodec;
import com.mr.utils.ProtobufMessageCodec;
import com.mr.utils.AuxiliaryFunction;

public class DataReducer{
	public static class LogReducer extends Reducer<Text, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
            
            // initialize Counter
            Counter validInputCt = context.getCounter("LogReducer", "validInputCt");
            Counter orderCt = context.getCounter("LogReducer", "orderCt");
            Counter clickCt = context.getCounter("LogReducer", "clickCt");
            Counter showWinCt = context.getCounter("LogReducer", "showWinCt");
            Counter exchange13Ct = context.getCounter("LogReducer", "exchange13Ct"); // not used
            Counter exchange13BidInfoCt = context.getCounter("LogReducer", "exchange13BidInfoCt"); // not used
            Counter multipleBidCt = context.getCounter("LogReducer", "multipleBidCt");
            Counter multipleClickCt = context.getCounter("LogReducer", "multipleClickCt");
            Counter multipleShowCt = context.getCounter("LogReducer", "multipleShowCt");
            Counter multipleWinCt = context.getCounter("LogReducer", "multipleWinCt");

            int bidSignalCt = 0;
            int winSignalCt = 0;
            int showSignalCt = 0;
            int clickSignalCt = 0;

            Text temp = new Text();
            List<String> valuesList= new LinkedList<String>(); 
            String featureValues = "";
            String timestampShow = "-";
            String timestampClick = "-";
            Boolean hasOtherInfo = false;
            Boolean isPcBanner = false;
            Boolean matchInfo = false;
            Boolean hasClicked = false;
            Boolean hasWin = false;
            int counter = 0;
            
            for (Text value : values){
                String[] infoElements = value.toString().split(Properties.Base.BS_SEPARATOR_TAB);
                if (infoElements[0].equals("bidLog")){
                    featureValues = AuxiliaryFunction.listToString(infoElements,7);
                    // featureValues = featureValues.substring(7,featureValues.length()-1);
                    matchInfo = true;
                }
                else if (infoElements[0].equals("winLog")){
                    hasWin = true;
                }
                else if (infoElements[0].equals("showLog")) {
                    timestampShow = infoElements[1];
                }
                else if (infoElements[0].equals("clickLog")) {
                    timestampClick = infoElements[1];
                    hasClicked = true;
                }
                counter ++ ;
            }

            if (matchInfo){
                orderCt.increment(1);
                value_result.set(featureValues);
                context.write(key, value_result);
            }
        }
    }
}
