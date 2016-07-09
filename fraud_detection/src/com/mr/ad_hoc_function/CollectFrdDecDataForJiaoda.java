package com.mr.ad_hoc_function;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/3/9.
 */
public class CollectFrdDecDataForJiaoda {
    public static class pcDataMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> cookieSet = new HashSet<String>();
        private HashMap<String,String> domainIndexMap = new HashMap<String,String>();
        private HashMap<String,String> hostIndexMap = new HashMap<String,String>();
        private HashMap<String,String> genderIndexMap = new HashMap<String,String>();
        private HashMap<String,String> tagIndexMap = new HashMap<String,String>();
        private HashMap<String,String> ipIndexMap = new HashMap<String,String>();
        private FileSystem fs = null;

//        protected void setup(Context context) throws IOException,InterruptedException{
//            Configuration conf = context.getConfiguration();
//            fs = FileSystem.get(conf);
//            String domainIndex = context.getConfiguration().get("domainIndex");
//            String hostIndex = context.getConfiguration().get("hostIndex");
//            String genderIndex = context.getConfiguration().get("genderIndex");
//            String tagIndex = context.getConfiguration().get("tagIndex");
////            String ipIndex = context.getConfiguration().get("ipIndex");
//            loadIndexMap(fs,domainIndex,this.domainIndexMap);
//            loadIndexMap(fs,hostIndex,this.hostIndexMap);
//            loadIndexMap(fs,genderIndex,this.genderIndexMap);
//            loadIndexMap(fs,tagIndex,this.tagIndexMap);
////            loadIndexMap(fs,ipIndex,this.ipIndexMap);
//        }

        public static void loadIndexMap(FileSystem fs, String path, HashMap<String,String> map)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String plainText = line.split(Properties.Base.BS_SEPARATOR_TAB, -1)[0];
                String index = line.split(Properties.Base.BS_SEPARATOR_TAB, -1)[1];
                map.put(plainText,index);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("pcDataMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("pcDataMapper", "validOutputCt");
            Counter illegalColumnCt = context.getCounter("pcDataMapper", "illegalColumnCt");


            // initialization
            String line = value.toString();
            String[] elementsInfo = line.split(Properties.Base.CTRL_A, -1);

            validInputCt.increment(1);

            // action_monitor
            elementsInfo[5] = "";
            // bid_way
            elementsInfo[6] = "";
            // algo_data
            elementsInfo[7] = "";
            // domain
//            elementsInfo[22] = this.domainIndexMap.get(elementsInfo[22]);
            // host
//            elementsInfo[23] = this.hostIndexMap.get(elementsInfo[23]);
            // url
            elementsInfo[24] = "";
            // refer_url
            elementsInfo[25] = "";
            // ip
//            elementsInfo[40] = this.domainIndexMap.get(elementsInfo[40]);
            // adx_cookie
            elementsInfo[41] = "";
            // gender
//            elementsInfo[43] = this.genderIndexMap.get(elementsInfo[43]);
            // tag
//            String tagString = "";
//            for (String tag: elementsInfo[44].split(Properties.Base.CTRL_B)) {
//                tagString += this.tagIndexMap.get(tag) + Properties.Base.CTRL_B;
//            }
//            try {
//                tagString.substring(0, tagString.length() - 1);
//            } catch (StringIndexOutOfBoundsException e){
//                tagString = "";
//            }
//            elementsInfo[44] = tagString;

            String infoString = StringUtil.listToString(elementsInfo,Properties.Base.CTRL_A);

            if (infoString.split(Properties.Base.CTRL_A,-1).length!=45){
                illegalColumnCt.increment(1);
                return;
            }

            value_result.set(infoString);
            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CollectFrdDecDataForJiaoda");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CollectFrdDecDataForJiaoda.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        conf.set("mapreduce.reduce.memory.mb ", "2000");
        conf.set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedataPcPath = otherArgs[1];
//        String domainIndex = otherArgs[2];
//        String hostIndex = otherArgs[3];
//        String genderIndex = otherArgs[4];
//        String tagIndex = otherArgs[5];
//        String ipIndex = otherArgs[6];
//        String currentDate = otherArgs[7];

//        job.getConfiguration().set("domainIndex", domainIndex);
//        job.getConfiguration().set("hostIndex", hostIndex);
//        job.getConfiguration().set("genderIndex", genderIndex);
//        job.getConfiguration().set("tagIndex", tagIndex);
//        job.getConfiguration().set("ipIndex",ipIndex);

        CommUtil.addInputFileComm(job, fs, basedataPcPath, TextInputFormat.class, pcDataMapper.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
