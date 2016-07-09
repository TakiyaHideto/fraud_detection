package com.mr.effect_analysis;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.UrlUtil;
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
import java.util.*;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/5/4.
 */
public class EvaluateDomainOffline {
    public static class ExtractCookieMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> indexSet = new HashSet<String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String indexFile = context.getConfiguration().get("indexFile");
            fs = FileSystem.get(conf);
            loadCache(fs, indexFile, this.indexSet);
        }

        public static void loadCache(FileSystem fs,String path, HashSet<String> indexSet)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String domain = line.split(Properties.Base.BS_SEPARATOR_TAB,-1)[0];
                indexSet.add(domain);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("DomainMapper", "validInput");
            Counter validOutputCt = context.getCounter("DomainMapper", "validOutputCt");
            Counter arrayIndexOutOfBoundsExceptionCt = context.getCounter("DomainMapper", "arrayIndexOutOfBoundsExceptionCt");

            validInputCt.increment(1);
            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);

            String index = "";
            if (context.getConfiguration().get("index").equals("ip")){
                index = elementInfo[40];
            } else if (context.getConfiguration().get("index").equals("domain")){
                index = elementInfo[22];
            } else if (context.getConfiguration().get("index").equals("adzone")){
                index = elementInfo[27];
            } else {
                return;
            }

            String yoyiCookie = elementInfo[42];

            if (!this.indexSet.contains(index) || index.equals(""))
                return;

            key_result.set(yoyiCookie);
            value_result.set("YOYI_COOKIE_EVALUATED");
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class EvaluateDomainOfflineMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("EvaluateDomainOfflineMapper", "validInput");
            Counter validOutputCt = context.getCounter("EvaluateDomainOfflineMapper", "validOutputCt");
            Counter arrayIndexOutOfBoundsExceptionCt = context.getCounter("EvaluateDomainOfflineMapper", "arrayIndexOutOfBoundsExceptionCt");

            validInputCt.increment(1);
            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.BS_SEPARATOR_TAB,-1);

            String yoyiCookie = elementInfo[0];
            String keepTime = elementInfo[1];
            String deep = elementInfo[2];
            String secondBounce = elementInfo[3];

            key_result.set(yoyiCookie);
            value_result.set(keepTime +
                    Properties.Base.BS_SEPARATOR_TAB + deep +
                    Properties.Base.BS_SEPARATOR_TAB + secondBounce);
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class EvaluateDomainOfflineReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("EvaluateDomainOfflineReducer", "validOutputCt");

            ArrayList<String> databankData = new ArrayList<String>();

            Boolean isEvaluatedCookie = false;

            for(Text value: values){
                if (value.toString().equals("YOYI_COOKIE_EVALUATED")){
                    isEvaluatedCookie = true;
                } else {
                    databankData.add(value.toString());
                }
            }

            if (isEvaluatedCookie){
                for (String data: databankData) {
                    value_result.set(key.toString() + Properties.Base.BS_SEPARATOR_TAB + data);
                    context.write(NullWritable.get(), value_result);
                    validOutputCt.increment(1);
                }
            } else {
                return;
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"EvaluateDomainOffline");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(EvaluateDomainOffline.class);
        job.setReducerClass(EvaluateDomainOfflineReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String newBasedata = otherArgs[1];
        String databank = otherArgs[2];
        String indexFile = otherArgs[3];
        String index = otherArgs[4];

        job.getConfiguration().set("indexFile",indexFile);
        job.getConfiguration().set("index",index);

        CommUtil.addInputFileComm(job, fs, databank, TextInputFormat.class, EvaluateDomainOfflineMapper.class);
        CommUtil.addInputFileComm(job, fs, newBasedata, TextInputFormat.class, ExtractCookieMapper.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
