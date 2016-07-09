package com.mr.merge_data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.net.URL;
import java.lang.ArrayIndexOutOfBoundsException;
import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs; 
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.protobuffer.OriginalClickLog;
import com.mr.protobuffer.OriginalShowLog;
import com.mr.protobuffer.OriginalWinLog;
import com.mr.utils.CommUtil;
import com.mr.utils.AuxiliaryFunction;
import com.mr.utils.DateUtil;
import com.mr.utils.TextMessageCodec;
import com.mr.utils.StringUtil;

public class AddFilterBlackListForBasedataMR{

	public static class AddFilterBlackListForBasedataMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text(); 
        private HashSet<String> ipSet = new HashSet<String>();
        private HashSet<String> adzoneIdSet = new HashSet<String>();
        private HashSet<String> domainSet = new HashSet<String>();
        private HashSet<String> yoyiCookieSet = new HashSet<String>();
        private FileSystem fs = null;


        protected void setup(Context context) throws IOException,InterruptedException{
            Counter ipSetVolumeCt = context.getCounter("AddFilterBlackListForBasedataMapper", "ipSetVolumeCt");
            Counter adzoneIdSetVolumeCt = context.getCounter("AddFilterBlackListForBasedataMapper", "adzoneIdSetVolumeCt");
            Counter domainSetVolumeCt = context.getCounter("AddFilterBlackListForBasedataMapper","domainSetVolumeCt");
            Counter yoyiCookieSetVolumeCt = context.getCounter("AddFilterBlackListForBasedataMapper","yoyiCookieSetVolumeCt");
            Configuration conf = context.getConfiguration();
            fs = FileSystem.get(conf);
            String ipSetFilePath = (String)context.getConfiguration().get("ipSetFilePath");
            String adzoneIdSetFilePath = (String)context.getConfiguration().get("adzoneIdSetFilePath");
            String domainSetFilePath = (String)context.getConfiguration().get("domainSetFilePath");
            String yoyiCookieSetFilePath = (String)context.getConfiguration().get("yoyiCookieSetFilePath");
            readFilterFile(fs, ipSetFilePath, this.ipSet,ipSetVolumeCt);
//            readFilterFile(fs, adzoneIdSetFilePath, this.adzoneIdSet, adzoneIdSetVolumeCt);
            readFilterFile(fs, domainSetFilePath, this.domainSet, domainSetVolumeCt);
//            readFilterFile(fs, yoyiCookieSetFilePath, this.yoyiCookieSet, yoyiCookieSetVolumeCt);
        } 

        public static void readFilterFile(FileSystem fs, String path, HashSet<String> filterSet, Counter filterCt) 
        	throws FileNotFoundException, IOException{
        	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
			String line;
			while((line = br.readLine())!=null){
				filterCt.increment(1);
				filterSet.add(line.trim());
			}
			br.close();
	    }

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validInputCt = context.getCounter("AddFilterBlackListForBasedataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("AddFilterBlackListForBasedataMapper", "validOutputCt");
            Counter cheatVolumeCt = context.getCounter("AddFilterBlackListForBasedataMapper","cheatVolumeCt");
            Counter normalVolumeCt = context.getCounter("AddFilterBlackListForBasedataMapper","normalVolumeCt");
            Counter cheatClickCt = context.getCounter("AddFilterBlackListForBasedataMapper","cheatClickCt");
            Counter clickVolumeCt = context.getCounter("AddFilterBlackListForBasedataMapper","clickVolumeCt");
            Counter cheatWithoutDomainCt = context.getCounter("AddFilterBlackListForBasedataMapper","cheatWithoutDomainCt");
            // Counter tudouuiCt = context.getCounter("AddFilterBlackListForBasedataMapper","tudouuiCt");
            Counter brokenStringCt = context.getCounter("AddFilterBlackListForBasedataMapper","brokenStringCt");
            Counter brokenDataCt = context.getCounter("AddFilterBlackListForBasedataMapper", "brokenDataCt");
            Counter brokenColumeCt = context.getCounter("AddFilterBlackListForBasedataMapper","brokenColumeCt");
            Counter wrongColumeFraudCt = context.getCounter("AddFilterBlackListForBasedataMapper","wrongColumeFraudCt");
            Counter wrongColumeOriginCt = context.getCounter("AddFilterBlackListForBasedataMapper","wrongColumeOriginCt");
            Counter domainFilterImpCt = context.getCounter("AddFilterBlackListForBasedataMapper","domainFilterImpCt");
            Counter domainFilterClkCt = context.getCounter("AddFilterBlackListForBasedataMapper","domainFilterClkCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            String line = value.toString();
            String[] elementsInfo = line.split(Properties.Base.BS_SEPARATOR, -1);

            validInputCt.increment(1);

            String log_date = elementsInfo[31];
//            String ip = elementsInfo[40] + Properties.Base.BS_SEPARATOR + log_date;
            String ip = elementsInfo[40];
            String yoyiCookie = "";
            try {
                yoyiCookie = elementsInfo[42] + Properties.Base.BS_SEPARATOR + log_date;
            } catch (ArrayIndexOutOfBoundsException e) {
                brokenDataCt.increment(1);
            }
            
            String adzoneId = elementsInfo[27];
            String domain = elementsInfo[22];

            Boolean cheatData = false;

            String valueInfo = "";

            if (elementsInfo[2].equals("1"))
                    clickVolumeCt.increment(1);

            if (this.ipSet.contains(ip) || 
            	this.adzoneIdSet.contains(adzoneId) ||
            	this.yoyiCookieSet.contains(yoyiCookie) ||
                this.domainSet.contains(domain))
            {
                if (this.domainSet.contains(domain)&& !(this.ipSet.contains(ip) ||
                        this.adzoneIdSet.contains(adzoneId) ||
                        this.yoyiCookieSet.contains(yoyiCookie))){
                    if (elementsInfo[2].equals("1"))
                        domainFilterClkCt.increment(1);
                    domainFilterImpCt.increment(1);
                }

            	elementsInfo[7] = "1" + Properties.Base.BS_SUB_SEPARATOR + elementsInfo[7];
            	cheatVolumeCt.increment(1);
                if (elementsInfo[2].equals("1"))
                    cheatClickCt.increment(1);
                if (domain.equals(""))
                    cheatWithoutDomainCt.increment(1);
            } 
            else{
                normalVolumeCt.increment(1);
            	elementsInfo[7] = "0" + Properties.Base.BS_SUB_SEPARATOR + elementsInfo[7];
            }

            valueInfo = StringUtil.listToString(elementsInfo,Properties.Base.BS_SEPARATOR);
            if (valueInfo.length() - "1".length() - Properties.Base.BS_SUB_SEPARATOR.length() != line.length())
                brokenStringCt.increment(1);
            if (valueInfo.split(Properties.Base.BS_SEPARATOR,-1).length!=line.split(Properties.Base.BS_SEPARATOR,-1).length)
                brokenColumeCt.increment(1);
            if (valueInfo.split(Properties.Base.BS_SEPARATOR,-1).length!=45)
                wrongColumeFraudCt.increment(1);
            if (line.split(Properties.Base.BS_SEPARATOR,-1).length!=45)
                wrongColumeOriginCt.increment(1);

            // output key-value
            key_result.set(elementsInfo[0] + elementsInfo[12]);
            value_result.set(valueInfo);
            context.write(key_result, value_result);
            validOutputCt.increment(1);      
		}
	}

	public static class AddFilterBlackListForBasedataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
            
            // initialize Counter
            Counter validOutputCt = context.getCounter("AddFilterBlackListForBasedataReducer","validOutputCt");

            for (Text value: values){
            	context.write(NullWritable.get(),value); 
            	validOutputCt.increment(1);    	
            }       
        }
    }

    public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if(otherArgs.length != 6){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"AddFilterBlackListForBasedataMR"); 
		conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		FileSystem fs = FileSystem.get(conf);
		job.setJarByClass(AddFilterBlackListForBasedataMR.class);
//		job.setReducerClass(AddFilterBlackListForBasedataReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String baseDataFilePath = otherArgs[1];
		String ipSetFilePath = otherArgs[2];
		String adzoneIdSetFilePath = otherArgs[3];
		String domainSetFilePath = otherArgs[4];
		String yoyiCookieSetFilePath = otherArgs[5];
        job.getConfiguration().set("ipSetFilePath", ipSetFilePath);
        job.getConfiguration().set("adzoneIdSetFilePath", adzoneIdSetFilePath);
        job.getConfiguration().set("domainSetFilePath", domainSetFilePath);
        job.getConfiguration().set("yoyiCookieSetFilePath", yoyiCookieSetFilePath);
		job.getConfiguration().set("mapred.queue.name", "algo-dev");	
        
		CommUtil.addInputFileCommSpecialJob(job,fs,baseDataFilePath,TextInputFormat.class,AddFilterBlackListForBasedataMapper.class,"addFilter");
        
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

}
















