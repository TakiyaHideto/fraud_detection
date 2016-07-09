package com.mr.code_backup;

import java.net.*;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RatioStatMapper{
	public static class RatioStatFraudMapper extends Mapper<Object, Text, Text, MapWritable>{
		private Text key_result = new Text();
        private Text value_result = new Text();

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			
            String line = value.toString().trim();
            String[] elements = line.split("\t");

            try{
            	String urlString = String.valueOf(elements[4]);
            	URL url = new URL(urlString);

            	// key info
            	String pageDomain = url.getHost();

            	// value info
            	RatioStatMapperHandler ratioStatMapperHandler = new RatioStatMapperHandler(line);
            	MapWritable dataInfo = ratioStatMapperHandler.processDataInfo(line);

            	// mapper output
            	key_result.set(pageDomain);
            	context.write(key_result, dataInfo);
            } catch (Exception e){
                e.printStackTrace();
                return;
            }
		}
	}
}
















