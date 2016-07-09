hadoop dfs -text /share_data/benz_camp_timestamp/log_date=2015-11-18/part-r-00000 > /data/dongjiahao/svnProject/basedata/trunk/src/com/mr/data_analysis_python/benzInfo.txt
python /data/dongjiahao/svnProject/basedata/trunk/src/com/mr/data_analysis_python/new_benzTimeGapDistributionGraph.py
rm /data/dongjiahao/svnProject/basedata/trunk/src/com/mr/data_analysis_python/benzInfo.txt