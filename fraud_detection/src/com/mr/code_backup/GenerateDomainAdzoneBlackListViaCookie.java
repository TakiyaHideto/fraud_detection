package com.mr.code_backup;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.utils.CommUtil;
import com.mr.utils.TextMessageCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;

/**
 * Created by TakiyaHideto on 15/12/18.
 */
public class GenerateDomainAdzoneBlackListViaCookie {

    public static class GenerateDomainAdzoneBlackListViaCookieMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> cookieSet = new HashSet<String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            fs = FileSystem.get(conf);
            String domainSetFilePath1 = (String)context.getConfiguration().get("ipSetPath");
            readFilterFile(fs,domainSetFilePath1,this.cookieSet);
        }

        public static void readFilterFile(FileSystem fs, String path, HashSet<String> filterSet)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                filterSet.add(line.trim());
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("GenerateDomainAdzoneBlackListViaCookieMapper","validInputCt");
            Counter validOutputCt = context.getCounter("GenerateDomainAdzoneBlackListViaCookieMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("GenerateDomainAdzoneBlackListViaCookieMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("GenerateDomainAdzoneBlackListViaCookieMapper", "otherExceptionCt");

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalBidLog.OriginalBid bidLog;
            try{
                bidLog = (OriginalBidLog.OriginalBid) TMC.parseFromString(value.toString(), OriginalBidLog.OriginalBid.newBuilder());
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

//            String domain = UrlUtil.getUrlDomain(bidLog.getPage().getPageUrl());
            String yoyiCookie = "";
            if (bidLog.getAdsCount() != 0){
                yoyiCookie = bidLog.getUser().getUserYyid();
            }
            else {
                yoyiCookie = bidLog.getUser().getUserExid();
            }
            List<OriginalBidLog.Adzone> adzoneList = bidLog.getAdzoneList();


            // output key-value
            if (this.cookieSet.contains(yoyiCookie)){
                for (OriginalBidLog.Adzone adzone: adzoneList) {
                    key_result.set(adzone.getAdzoneId());
                    value_result.set("1");
                    context.write(key_result, value_result);
                    validOutputCt.increment(1);
                }
            }
            else {
                for (OriginalBidLog.Adzone adzone: adzoneList) {
                    key_result.set(adzone.getAdzoneId());
                    value_result.set("2");
                    context.write(key_result, value_result);
                    validOutputCt.increment(1);
                }
            }
        }
    }

    public static class GenerateDomainAdzoneBlackListViaCookieReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter reachVolumeCt = context.getCounter("GenerateDomianBlackListViaIpReducer","reachVolumeCt");

            int cheatVolume = 0;
            int normalVolume = 0;

            for (Text value: values){
                if (value.toString().equals("1")){
                    cheatVolume ++;
                }
                else if (value.toString().equals("2")){
                    normalVolume ++;
                }
            }

            value_result.set(key.toString() +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(cheatVolume) +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(normalVolume) +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf((double)cheatVolume/(double)normalVolume));

            context.write(NullWritable.get(),value_result);
            reachVolumeCt.increment(1);

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println(otherArgs.length);
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"GenerateDomainAdzoneBlackListViaCookie");
        conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
        conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(GenerateDomainAdzoneBlackListViaCookie.class);
        job.setReducerClass(GenerateDomainAdzoneBlackListViaCookieReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(Properties.Base.BS_REDUCER_NUM);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogPath = otherArgs[1];
        String ipSetPath = otherArgs[2];
        job.getConfiguration().set("ipSetPath", ipSetPath);
        job.getConfiguration().set("mapred.queue.name", "algo-dev");

        CommUtil.addInputFileCommSucc(job, fs, bidLogPath, TextInputFormat.class, GenerateDomainAdzoneBlackListViaCookieMapper.class, "all");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
