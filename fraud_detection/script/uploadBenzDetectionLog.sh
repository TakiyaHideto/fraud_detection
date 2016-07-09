ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
echo ONE_DAY_AGO=$ONE_DAY_AGO

cat /data/dongjiahao/svnProject/basedata/trunk/dataLog/231/*$ONE_DAY_AGO* > /data/dongjiahao/svnProject/basedata/trunk/dataLog/benz_2015-11-24
cat /data/dongjiahao/svnProject/basedata/trunk/dataLog/232/*$ONE_DAY_AGO* >> /data/dongjiahao/svnProject/basedata/trunk/dataLog/benz_2015-11-24

cat /data/dongjiahao/svnProject/basedata/trunk/dataLog/benz_2015-11-24 | wc -l

hadoop dfs -mkdir /share_data/benz_camp_log/log_date=$ONE_DAY_AGO
hadoop dfs -put /data/dongjiahao/svnProject/basedata/trunk/dataLog/benz_2015-11-24 /share_data/benz_camp_log/log_date=$ONE_DAY_AGO