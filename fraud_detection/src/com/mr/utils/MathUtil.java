package com.mr.utils;

import java.util.*;
import java.util.Map.Entry;

/**
 * Created by TakiyaHideto on 16/3/3.
 */
public class MathUtil {
    public static double calculatePoissonDistribution(int lambda,int k){
        double possionValue = 0.0;
        possionValue = Math.exp(-lambda)*Math.pow(lambda,k)/calFactorial(k);
        return possionValue;
    }

    public static double calculateGaussianDistribution(double value,double mean){
        double gaussianValue = 0.0;
        gaussianValue = Math.exp(-(Math.pow(value-mean,2))/2)/(Math.sqrt(2*Math.PI));
        return gaussianValue;
    }

    public static int calFactorial(int maxValue){
        int result = 1;
        for(int i=maxValue;i>=1;i--){
            result *= maxValue;
        }
        return result;
    }

    public static HashMap<String,Double> calCtrForKey(HashMap<String,Integer> impMap,
                                                      HashMap<String,Integer> clkMap,
                                                      HashMap<String,Double> ctrMap){
        for (String campaign: impMap.keySet()){
            if (!clkMap.containsKey(campaign)){
                ctrMap.put(campaign,0.0);
            } else {
                int clkNum = clkMap.get(campaign);
                int impNum = impMap.get(campaign);
                double ctr = ((double)clkNum/(double)impNum);
                ctrMap.put(campaign,ctr);
            }
        }
        return ctrMap;
    }

    public static double calMeanMapInteger(HashMap<String,Integer> map){
        double mean;
        double sum = 0.0;
        int totalCampaignNum = map.size();
        for (String campaign: map.keySet()){
            sum += (double)map.get(campaign);
        }
        mean = sum/(double)totalCampaignNum;
        sum = 0.0;
        return mean;
    }

    public static double calMeanMapDouble(HashMap<String,Double> map){
        double mean;
        double sum = 0.0;
        int totalCampaignNum = map.size();
        for (String campaign: map.keySet()){
            sum += (double)map.get(campaign);
        }
        mean = sum/(double)totalCampaignNum;
        sum = 0.0;
        return mean;
    }

    public static double calStandardDeviationMapInteger(HashMap<String, Integer> map, double mean){
        double standardVariance;
        double sum = 0.0;
        for (String campaign: map.keySet()){
            sum += Math.pow(((double)map.get(campaign)-mean), 2.0);
        }
        standardVariance = Math.sqrt(sum/(double)map.size());
        sum = 0.0;
        return standardVariance;
    }

    public static double calStandardDeviationMapDouble(HashMap<String, Double> map, double mean){
        double standardVariance;
        double sum = 0.0;
        for (String campaign: map.keySet()){
            sum += Math.pow(((double)map.get(campaign)-mean), 2.0);
        }
        standardVariance = Math.sqrt(sum/(double)map.size());
        sum = 0.0;
        return standardVariance;
    }

    public static int calSumInteger(HashMap<String, Integer> map){
        int sum = 0;
        for (String key: map.keySet()){
            sum += map.get(key);
        }
        return sum;
    }

    public static LinkedHashMap sortMap(LinkedHashMap<String,Integer> oldMap) {
        ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(oldMap.entrySet());
        Collections.sort(list, new Comparator<Entry<String, Integer>>() {
            public int compare(Entry<String, Integer> arg0,
                               Entry<String, Integer> arg1) {
                return arg1.getValue() - arg0.getValue();
            }
        });
        LinkedHashMap newMap = new LinkedHashMap();
        for (int i = 0; i < list.size(); i++) {
            newMap.put(list.get(i).getKey(), list.get(i).getValue());
        }
        return newMap;
    }

    public static int calRatioCumulative(LinkedHashMap<String,Integer> map,double ratio,int sum){
        Iterator it = map.entrySet().iterator();
        int sumCumulative = 0;
        int sumKey = 0;
        while(it.hasNext()){
            Map.Entry entry = (Map.Entry) it.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
            sumCumulative += Integer.parseInt(entry.getValue().toString());
            sumKey++;
            if ((double)sumCumulative/(double)sum>ratio){
                break;
            }
        }
        return sumKey;
    }
}
