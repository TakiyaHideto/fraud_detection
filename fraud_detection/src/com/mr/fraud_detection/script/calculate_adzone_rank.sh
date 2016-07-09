#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-4 day"`

python_script_dir=/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/data_analysis_python

original_adzone_file=/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/original_adzone_file_${ONE_DAY_AGO}
adzone_rank_file=/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/adzone_rank_${ONE_DAY_AGO}

dateClkMean=`less ${original_adzone_file} | awk -F"\t" '{sum+=$5} END {print sum/NR}'`
ipClkMean=`less ${original_adzone_file} | awk -F"\t" '{sum+=$8} END {print sum/NR}'`

python ${python_script_dir}/AdzoneRank.py ${dateClkMean} ${ipClkMean} ${ONE_DAY_AGO}

cat ${adzone_rank_file} | sort -t $'\t' -k 10 -g -r | awk -F"\t" '{print NR"\t"$0}'> temp
cat temp > ${adzone_rank_file}