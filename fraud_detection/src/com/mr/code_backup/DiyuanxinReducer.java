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
import java.util.Iterator;

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

public class DiyuanxinReducer{
    public static class DiyuanxinDataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private MultipleOutputs multipleOutputs;
        
        protected void setup(Context context) throws IOException,InterruptedException{
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
            
            // initialize Counter
            Counter validOutputCt = context.getCounter("DiyuanxinReducer","validOutputCt");
            Counter matchCookieCt = context.getCounter("DiyuanxinReducer","matchCookieCt");
            Counter diyuanxinCookieCt = context.getCounter("DiyuanxinReducer","diyuanxinCookieCt");
            Counter bidLogCookieCt = context.getCounter("DiyuanxinReducer","bidLogCookieCt");

            Boolean isClickData = false;
            Boolean isDiyuanxinData = false;

            for (Text value: values){
                if (value.toString().contains("bidLog")){
                    isClickData = true;
                } 
                else{
                    if (value.toString().contains("detection")){
                        isDiyuanxinData = true;  
                        String valueInfo = value.toString();
                        valueInfo = valueInfo.substring(0,valueInfo.length()-(Properties.Base.BS_SEPARATOR + "detection").length());
                        valueInfo = valueInfo.split(Properties.Base.BS_SEPARATOR)[0];
                        value_result.set(valueInfo);
//                        context.write(NullWritable.get(),value_result);
                        validOutputCt.increment(1);
                    }
                }
            } 
            if (isDiyuanxinData)
                diyuanxinCookieCt.increment(1);
            if (isClickData)
                bidLogCookieCt.increment(1);
            if (!isClickData && isDiyuanxinData){
                matchCookieCt.increment(1);
                context.write(NullWritable.get(),value_result);
            }
        }
    }
}






