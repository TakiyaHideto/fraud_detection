package com.mr.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Counter;

import com.mr.config.Properties;

public class CommUtil {
	public static void addInputFileComm(Job job, FileSystem fs, String filePaths, Class InputFormatClass, Class mapClass) 
			throws IOException {
		Path path = new Path(filePaths);
		if (fs.exists(path)) {
			FileStatus[] fileStatus = fs.listStatus(path);
			for (int i = 0; i < fileStatus.length; i++) {
				FileStatus fileStatu = fileStatus[i];
				if (fileStatu.isDir()) {
					addInputFileComm(job,fs,fileStatu.getPath().toString(), InputFormatClass, mapClass);
				} else {
					if (fileStatu.getLen()>0) {
						System.out.println(fileStatu.getPath());
						MultipleInputs.addInputPath(job, fileStatu.getPath(), InputFormatClass, mapClass);
					}
				}
			}
		}
	}

	public static void addInputFileCommSpecialJob(Job job, FileSystem fs, String filePaths, Class InputFormatClass, Class mapClass, String jobName) 
			throws IOException {
		Path path = new Path(filePaths);
		if (fs.exists(path)) {
			FileStatus[] fileStatus = fs.listStatus(path);
			for (int i = 0; i < fileStatus.length; i++) {
				FileStatus fileStatu = fileStatus[i];
				if (fileStatu.isDir()) {
					addInputFileComm(job,fs,fileStatu.getPath().toString(), InputFormatClass, mapClass);
				} else {
					if (fileStatu.getLen()>0 && fileStatu.getPath().toString().contains(jobName)) {
						System.out.println(fileStatu.getPath());
						MultipleInputs.addInputPath(job, fileStatu.getPath(), InputFormatClass, mapClass);
					}
				}
			}
		}
	}
	
	public static void addInputFileComm(Job job, FileSystem fs, String filePaths, Class InputFormatClass, Class classname, String hour) throws IOException {
        Path path = new Path(filePaths);
        if (fs.exists(path)) {
            FileStatus[] fileStatus = fs.listStatus(path);
            for (int i = 0; i < fileStatus.length; i ++) {
                FileStatus fileStatu = fileStatus[i];
                if (fileStatu.isDir()) {
                    addInputFileComm(job,fs,fileStatu.getPath().toString(), InputFormatClass, classname, hour);
                } else {
					String strPath = fileStatu.getPath().toString();
					if(!hour.equals("all") && checkNumber(hour)){
						if (fileStatu.getLen() > 0 /*&& !strPath.contains("log_type=2")*/ && !strPath.contains("pmp") && strPath.contains("log_hour=" + hour)) {
	                        System.out.println(fileStatu.getPath());
	                        MultipleInputs.addInputPath(job, fileStatu.getPath(), InputFormatClass, classname);	
						}
					} else {
						if (fileStatu.getLen() > 0 /*&& !strPath.contains("log_type=2")*/ && !strPath.contains("pmp")){
	                        System.out.println(fileStatu.getPath());
	                        MultipleInputs.addInputPath(job, fileStatu.getPath(), InputFormatClass, classname);	
						}
					}
                }
            }
        }
    }

	public static void addInputFileCommSucc(Job job, FileSystem fs, String filePaths, Class InputFormatClass, Class classname, String hour) throws IOException {
		Path path = new Path(filePaths);
		if (fs.exists(path)) {
			FileStatus[] fileStatus = fs.listStatus(path);
			for (int i = 0; i < fileStatus.length; i ++) {
				FileStatus fileStatu = fileStatus[i];
				if (fileStatu.isDir()) {
					addInputFileCommSucc(job,fs,fileStatu.getPath().toString(), InputFormatClass, classname, hour);
				} else {
					String strPath = fileStatu.getPath().toString();
					if(!hour.equals("all") && checkNumber(hour)){
						if (fileStatu.getLen() > 0 && !strPath.contains("log_type=2") && !strPath.contains("pmp") && strPath.contains("log_hour=" + hour)) {
							System.out.println(fileStatu.getPath());
							MultipleInputs.addInputPath(job, fileStatu.getPath(), InputFormatClass, classname);
						}
					} else {
						if (fileStatu.getLen() > 0 && !strPath.contains("log_type=2") && !strPath.contains("pmp")){
							System.out.println(fileStatu.getPath());
							MultipleInputs.addInputPath(job, fileStatu.getPath(), InputFormatClass, classname);
						}
					}
				}
			}
		}
	}
	
	public static void fetchDsp3NormalTableInfo(FileSystem fs, String path, int colLenth, Map<String,String> table) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
		String line;
		while((line = br.readLine())!=null){
			String [] arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length == colLenth) {
				table.put(arrayValue[0], line);
			}
		}
		br.close();
	}
	
	public static void fetchMediaTableInfo(FileSystem fs, String path, int colLenth, Map<String,String> table) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
		String line;
		while((line = br.readLine())!=null){
			String [] arrayValue = line.split(Properties.Base.BS_SEPARATOR);
			if (arrayValue.length == colLenth) {
				table.put(arrayValue[1] + Properties.Base.BS_SUB_SEPARATOR + arrayValue[2], arrayValue[0]);
			}
		}
		br.close();
	}

	public static void readDataFromOrderTanx(FileSystem fs, String path, Set<String> setTemp, Counter orderCt) throws FileNotFoundException, IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        int orderVolume = 0;
        String line;
        while((line = br.readLine())!=null && !line.equals("")){
            String [] arrayValue = line.trim().split(Properties.Base.BS_SEPARATOR_TAB);
            if (!arrayValue[0].equals(""))
            	setTemp.add(arrayValue[0]);   
            orderVolume ++;
            orderCt.increment(1);       
        }
        br.close(); 
    }
	
	public static boolean checkNumber(String strSource) {
		if (strSource.length() < 1) {
			return false;
		}
		return strSource.matches("\\d+");
	}
	
	public static String adxIdMappingOldToNew(String oldAdxId){
		String newAdxId = "";
		if(oldAdxId.equals("2"))
			newAdxId = "2";
		else if (oldAdxId.equals("35"))
			newAdxId = "7";
		else if (oldAdxId.equals("7"))
			newAdxId = "1";
		else if (oldAdxId.equals("11"))
			newAdxId = "3";
		else if (oldAdxId.equals("20"))
			newAdxId = "4";
		else if (oldAdxId.equals("26"))
			newAdxId = "5";
		else if (oldAdxId.equals("36"))
			newAdxId = "8";
		else if (oldAdxId.equals("37"))
			newAdxId = "10";
		else if (oldAdxId.equals("17"))
			newAdxId = "6";
		
		return newAdxId;
	}
	
	public static String adxIdMappingNewToOld(String newdxId){
		String oldAdxId = "";
		if(newdxId.equals("2"))
			oldAdxId = "2";
		else if (newdxId.equals("7"))
			oldAdxId = "35";
		else if (newdxId.equals("1"))
			oldAdxId = "7";
		else if (newdxId.equals("3"))
			oldAdxId = "11";
		else if (newdxId.equals("4"))
			oldAdxId = "20";
		else if (newdxId.equals("5"))
			oldAdxId = "26";
		else if (newdxId.equals("8"))
			oldAdxId = "36";
		else if (newdxId.equals("10"))
			oldAdxId = "37";
		else if (newdxId.equals("6"))
			oldAdxId = "17";
		
		return oldAdxId;
	}

	public static void addInfoToMap(HashMap<String,Integer> map, String keyInfo){
		if (map.containsKey(keyInfo)){
			int count = map.get(keyInfo);
			map.remove(keyInfo);
			map.put(keyInfo,count+1);
		} else {
			map.put(keyInfo,1);
		}
	}

	public static void countKeyFrquecyMap(HashMap<String,Integer> map, String keyInfo, int frequency){
		if (map.containsKey(keyInfo)){
			int count = map.get(keyInfo);
			map.remove(keyInfo);
			map.put(keyInfo,count+frequency);
		} else {
			map.put(keyInfo,frequency);
		}
	}

	public static void addInfoToMapWithSet(HashMap<String,HashSet<String>> map, String keyInfo, String valueInfo){
		if (map.containsKey(keyInfo)){
			HashSet<String> temp = map.get(keyInfo);
			map.remove(keyInfo);
			temp.add(valueInfo);
			map.put(keyInfo,temp);
		} else {
			HashSet<String> temp = new HashSet<String>();
			temp.add(valueInfo);
			map.put(keyInfo,temp);
		}
	}

	public static String findKeyWithMaxValueInMap(HashMap<String,Integer> map){
		String keyString = "";
		int maxValue = -10000;
		for (String key: map.keySet()){
			if (maxValue < map.get(key)) {
				maxValue = map.get(key);
				keyString = key;
			}
		}
		return keyString;
	}

//	public static String findKeyWithMaxValueInMap(HashMap<String,Integer> map){
//		String keyString = "";
//		int maxValue = -10000;
//		for (String key: map.keySet()){
//			if (key.startsWith("工作日")) {
//				if (maxValue < map.get(key) / 5) {
//					maxValue = map.get(key) / 5;
//					keyString = key;
//				}
//			} else {
//				if (maxValue < map.get(key) / 2) {
//					maxValue = map.get(key) / 2;
//					keyString = key;
//				}
//			}
//		}
//		return keyString;
//	}
}
