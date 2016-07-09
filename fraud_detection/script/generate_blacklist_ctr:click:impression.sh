#!/usr/bin/env bash
ONE_DAY_AGO=`date +%Y-%m-%d --date="-0 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO
period=3
bltype=ctr

python BlackListByOrderScript.py $ONE_DAY_AGO $period $bltype
python /data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/data_analysis_python/new_collectBlackList.py $bltype
python /data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/data_analysis_python/new_addBlackListSet.py

hadoop dfs -rmr /share_data/blacklist_filter
hadoop dfs -mkdir /share_data/blacklist_filter
hadoop dfs -put /data/dongjiahao/svnProject/fraudDetection/trunk/blacklist/*Set.txt /share_data/blacklist_filter