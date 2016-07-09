package com.mr.code_backup;

import java.net.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ArrayIndexOutOfBoundsException;
import java.text.ParseException;
import java.lang.Long;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
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
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.TextMessageCodec;

public class DistributionMapper{
	public static class DistributionStatMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> adzoneIdSet = new HashSet<String>();

        protected void setup(Context context) throws IOException,InterruptedException{
        }

        protected void cleanup(Context context) throws IOException, InterruptedException{ 
            // Iterator<String> it = adzoneIdSet.iterator();  
            // while (it.hasNext()) {  
            //     this.adzoneIdCt.increment(1);  
            // }  
        }

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			
            Counter testCt = context.getCounter("DistributionStatMapper", "testCt");
            Counter validOutputCt = context.getCounter("DistributionStatMapper", "validOutputCt");

            String line = value.toString().trim();
            String[] elements = line.split("\t");

            try{
            	// key info
                String adzoneId = elements[17];
                adzoneIdSet.add(adzoneId);

            	// value info
            	String hour = elements[8];

            	// mapper output
                key_result.set(adzoneId);
                value_result.set(hour);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch(Exception e){
                testCt.increment(1);
                e.printStackTrace();
                return;
            }
		}
	}
}
















