#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-3 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

sh /data/dongjiahao/svnProject/basedata/trunk/script/traffic/databankAnalysis.sh $ONE_DAY_AGO
hadoop dfs -rm /mroutput/dongjiahao/databank/text_tmp/miaoche_cookies.txt
hadoop dfs -mkdir /mroutput/dongjiahao/databank/text_tmp
hadoop dfs -put /data/dongjiahao/svnProject/fraudDetection/dataLog/miaoche_cookies.txt /mroutput/dongjiahao/databank/text_tmp
sh /data/dongjiahao/svnProject/fraudDetection/trunk/script/AttachThirdPartyCookieTag.sh
hadoop dfs -text /mroutput/dongjiahao/new_basedata/miaoche_cookies_tag/part-r-* > /data/dongjiahao/svnProject/fraudDetection/dataLog/miaoche_cookies_tag.txt
hadoop dfs -rm /mroutput/dongjiahao/databank/text_tmp/miaoche_cookies_tag.txt
hadoop dfs -put /data/dongjiahao/svnProject/fraudDetection/dataLog/miaoche_cookies_tag.txt /mroutput/dongjiahao/databank/text_tmp
ssh ads@gw1.data.yoyi sh /home/ads/AT_CM_data/trunk/src/main/java/com/yoyi/dongjiahao/script/pull_miaoche_cookie_to_hdfs.sh
rm /data/dongjiahao/svnProject/fraudDetection/dataLog/miaoche_cookies.txt
rm /data/dongjiahao/svnProject/fraudDetection/dataLog/miaoche_cookies
hadoop dfs -rm /mroutput/dongjiahao/databank/text_tmp/miaoche_cookies_tag.txt