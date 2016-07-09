#!/usr/bin/env bash


ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

script_dir="/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/crossplatform/script"
sh ${script_dir}/YoyiCpPart1JoiningSingleDay.sh $ONE_DAY_AGO
sh ${script_dir}/YoyiCpPart2JoiningMultiDay.sh $ONE_DAY_AGO
sh ${script_dir}/YoyiCpPart3ExcludeCp.sh $ONE_DAY_AGO
sh ${script_dir}/YoyiCpPart4ExtractData.sh $ONE_DAY_AGO
sh ${script_dir}/YoyiCpPart5CollectAndPredict.sh $ONE_DAY_AGO
