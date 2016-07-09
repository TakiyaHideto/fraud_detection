package com.mr.code_backup.basedata;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mr.config.Properties;
import com.mr.utils.StringUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CookieInfoReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private Text result;
	Counter impCt;
	Counter outCt;
	
	private List<String> impList;
	
	public void setup(Context context) {}

	public void cleanup(Context context) throws IOException {}
	
	public String getBdSexId(String bdSex){
		String strResult = "";
		if(bdSex.equals("MALE"))
			strResult = "1";
		if(bdSex.equals("FEMALE"))
			strResult = "2";
		if(bdSex.equals("UNKOWN")) // should be unknown, but it is while received
			strResult = "3";	
		
		return strResult;
	}
	
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		result = new Text();
		impList = new ArrayList<String>();
		impCt = context.getCounter("COOKIE_REDUCE_COUNT", "impCt");
		outCt = context.getCounter("COOKIE_REDUCE_COUNT", "outCt");
		
		String impInfo = "";
		String bdSex = "";
		String bdInterest = "";
		String mzInterest = "";
		
		for (Text val : values) {
			String strValues = val.toString();	
			String[] info = strValues.split(Properties.Base.BS_SEPARATOR, -1);
	        if (info[0].equals("PC_IMP_LOG_TYPE") && info.length == 33){
	        	impInfo = strValues.replaceAll("PC_IMP_LOG_TYPE","");
	        	impInfo = impInfo.substring(1);
	        	if (impInfo.split(Properties.Base.BS_SEPARATOR, -1).length == Properties.Base.BS_FIRST_PHASE_OUT_COL_COUNT){
		        	impList.add(impInfo);
		        	impCt.increment(1);        		
	        	}
	        } else if (info[0].equals("COOKIE_LOG_TYPE")){
	        	bdSex = getBdSexId(info[1]);
	        	bdInterest = info[2];
	        	mzInterest = info[3];
	        } else {
	        	continue;
	        }
		}
		
		for (int i=0; i<impList.size(); i++){
			outCt.increment(1);
			impInfo = impList.get(i);
			impInfo += Properties.Base.BS_SEPARATOR + bdSex
					+ Properties.Base.BS_SEPARATOR + bdInterest 
					+ Properties.Base.BS_SEPARATOR + mzInterest;
			
			impInfo = impInfo.replaceAll("\t","");
			impInfo = impInfo.replaceAll("\n","");
			impInfo = impInfo.replaceAll("\r","");
			
			if (impInfo.split(Properties.Base.BS_SEPARATOR, -1).length == (Properties.Base.BS_FIRST_PHASE_OUT_COL_COUNT + Properties.Base.BS_CROWD_TAG_LEN)){
				result.set(impInfo);
				context.write(NullWritable.get(), result);			
			}
		}
	}
}
