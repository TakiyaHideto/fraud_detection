package com.mr.code_backup.basedata;

import java.io.IOException;
import java.lang.String;
import java.util.*;
import java.net.URLDecoder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Counter;

import com.mr.utils.*;
import com.mr.protobuffer.OriginalReachLog;
import com.mr.utils.TextMessageCodec;

public class Reach {

    //input reach
    public static class ReachLogPbTagMapper extends Mapper<Object, Text, Text, Text>{
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            TextMessageCodec TMC = new TextMessageCodec();
            OriginalReachLog.ArrivalLogMessage reachLog = (OriginalReachLog.ArrivalLogMessage) TMC
                    .parseFromString(value.toString(),
                            OriginalReachLog.ArrivalLogMessage.newBuilder());

            String version = reachLog.getVersion();
            String data = reachLog.getData();     
            String sid = "";
            String euid =  "";
            Counter c1 = context.getCounter("my_counters", "all_reach");
            c1.increment(1);
            HashMap<String, String> map = new HashMap<String, String>();        

            if (version.equals("1.0")) {
                String[] fields = data.split("&" ,-1);
                for(String tp : fields){
                    String[] kv = tp.split("=", 2);
                    if (kv.length == 2) {
                        map.put(kv[0], kv[1]);
                    } else {
                        continue;
                    }
                }
            }
            else if (version.equals("2.0")) {
                String[] fields = data.split("=",2);
                String extD = URLDecoder.decode(fields[1]);

                String[] ext = extD.split("&", -1);
                for(String tp : ext){
                    String[] kv = tp.split("=", 2);
                    if (kv.length == 2) {
                        map.put(kv[0], kv[1]);
                    } else {
                        continue;
                    }
                }
            }            
            sid = map.get("sid");   
            euid = map.get("euid");

            key_result.set(sid + "\001" + euid);
            value_result.set("reach" + "\003" + "1");
            context.write(key_result, value_result);
        }
    }

    public static class Reduce extends Reducer<Text,Text,NullWritable,Text>{
        private Text result = new Text();

        @Override
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{  
            for(Text t:values){
            }
            context.write(NullWritable.get(), result);
        }   
    }     

    /** 
     * @param args 
     */  
    public static void main(String[] args) throws Exception{  
        // TODO Auto-generated method stub  
        Configuration conf = new Configuration();  
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
        if(otherArgs.length != 2){  
            System.err.println("<int> <out>");  
            System.exit(2);  
        }
        System.out.println("xxxxxxx");

 
        Job job = new Job(conf,"NewTypeInfo");  
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        job.setJarByClass(Reach.class);  
        //job.setCombinerClass(Reduce.class);  
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setReducerClass(Reduce.class);  
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        //job.setOutputKeyClass(Text.class);
        //job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);  
        job.setOutputValueClass(Text.class); 
        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        
        FileSystem fs = FileSystem.get(conf);
        String filePathWin = otherArgs[1];
        CommUtil.addInputFileComm(job,fs,filePathWin, TextInputFormat.class, ReachLogPbTagMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
}

