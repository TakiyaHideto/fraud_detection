package com.mr.utils;

import com.mr.config.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/4/5.
 */
public class ModelUtil {
    public static void loadTreeDumpFile(FileSystem fs, String path, LinkedList<HashMap<String,String>> treeList)
            throws FileNotFoundException,IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        String line;
        HashMap<String,String> dumpMap = new HashMap<String,String>();
        int nextIndexNum = 0;
        while((line = br.readLine())!=null){
            line = line.replaceAll(Properties.Base.BS_SEPARATOR_TAB,"");
            if (line.startsWith("booster")){
                if (nextIndexNum>0){
                    treeList.add(dumpMap);
                    dumpMap.clear();
                }
                nextIndexNum ++;
            } else if ((line.contains("yes") &&
                    line.contains("no")) || line.contains("leaf=")) {
                String[] branchInfo = line.split(":",-1);
                String index = branchInfo[0];
                String info = branchInfo[1];
                dumpMap.put(index,info);
            }
        }
        if (!dumpMap.isEmpty()){
            treeList.add(dumpMap);
            dumpMap.clear();
        }
        br.close();
    }

    public static HashMap<String,String> parseBranchInfo(String branchInfo){

        HashMap<String, String> branchInfoMap = new HashMap<String, String>();

        if (branchInfo.contains("yes") && branchInfo.contains("no")) {
            String featureCond = branchInfo.split(Properties.Base.BS_SEPARATOR_SPACE, -1)[0].
                    replaceAll("\\[", "").replaceAll("]", "");
            String feature = featureCond.split("<", -1)[0];
            String condition = featureCond.split("<", -1)[1];
            String[] branch = branchInfo.split(Properties.Base.BS_SEPARATOR_SPACE, -1)[1].split(",", -1);
            String yesBrc = branch[0].split("=", -1)[1];
            String noBrc = branch[1].split("=", -1)[1];
            String missingBrc = branch[2].split("=", -1)[1];

            branchInfoMap.put("feature", feature);
            branchInfoMap.put("condition", condition);
            branchInfoMap.put("yes", yesBrc);
            branchInfoMap.put("no", noBrc);
            branchInfoMap.put("missing", missingBrc);
        } else if (branchInfo.contains("leaf=")){
            String value = branchInfo.split("=",-1)[1];
            branchInfoMap.put("value",value);
        }

        return branchInfoMap;
    }

    public static double calSigmoidProbability(double linearValue){
        double probability = 1.0/(1+Math.exp(-linearValue));
        return probability;
    }
}
