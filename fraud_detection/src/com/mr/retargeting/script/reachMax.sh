#!/usr/bin/env bash


ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

reach_max_cookie=/user/ads/log/reach-max/log_date=${ONE_DAY_AGO}
file=/data/dongjiahao/svnProject/reach_max/reach_max_${ONE_DAY_AGO}
temp=${file}_temp
rm ${file}

#if i<10;then
#path=${reach_max_cookie}/log_hour=0"${i}"
#else
#path=${reach_max_cookie}/log_hour="${i}"
#fi
#echo ${path}
hadoop dfs -text ${reach_max_cookie}/log_hour=00/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=01/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=02/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=03/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=04/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=05/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=06/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=07/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=08/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=09/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=10/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=11/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=12/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=13/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=14/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=15/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=16/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=17/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=18/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=19/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=20/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=21/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=22/* >> ${file}
hadoop dfs -text ${reach_max_cookie}/log_hour=23/* >> ${file}

cat ${file} | awk -F"\001" '{print $1"\001HUISHI201603"}' > ${temp}
cat ${temp} > ${file}
rm ${temp}

hadoop dfs -rm /mroutput/dongjiahao/reachMax/reach_max_${ONE_DAY_AGO}
hadoop dfs -put ${file} /mroutput/dongjiahao/reachMax