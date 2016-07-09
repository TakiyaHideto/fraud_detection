#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`
dir_local=/data/dongjiahao/svn_project/fraudDetection/file/retargeting
mkdir $dir_local

hadoop fs -text /share_data/Tag/ams/cookiePackage/14269935/*/* > ${dir_local}/RONGCHAN_1028_PinPaiCi_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14269935* >> ${dir_local}/RONGCHAN_1028_PinPaiCi_$ONE_DAY_AGO

hadoop fs -text /share_data/Tag/ams/cookiePackage/14269941/*/* > ${dir_local}/RONGCHAN_1028_ChanPingCi_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14418500/*/* >> ${dir_local}/RONGCHAN_1028_ChanPingCi_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14269941* >> ${dir_local}/RONGCHAN_1028_ChanPingCi_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14418500* >> ${dir_local}/RONGCHAN_1028_ChanPingCi_$ONE_DAY_AGO

hadoop fs -text /share_data/Tag/ams/cookiePackage/14269945/*/* > ${dir_local}/RONGCHAN_1028_RenQunCi_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14425229/*/* >> ${dir_local}/RONGCHAN_1028_RenQunCi_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14269945* >> ${dir_local}/RONGCHAN_1028_RenQunCi_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14425229* >> ${dir_local}/RONGCHAN_1028_RenQunCi_$ONE_DAY_AGO

hadoop fs -text /share_data/Tag/ams/cookiePackage/14269949/*/* > ${dir_local}/RONGCHAN_1028_JingPinCi_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14418503/*/* >> ${dir_local}/RONGCHAN_1028_JingPinCi_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14269949* >> ${dir_local}/RONGCHAN_1028_JingPinCi_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14418503* >> ${dir_local}/RONGCHAN_1028_JingPinCi_$ONE_DAY_AGO

hadoop fs -text /share_data/Tag/ams/cookiePackage/14244729/*/* > ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14425236/*/* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14418651/*/* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14418652/*/* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14484176/*/* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14491446/*/* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
hadoop fs -text /share_data/Tag/ams/cookiePackage/14491449/*/* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14244729* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14425236* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14418651* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14418652* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14484176* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14491446* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO
cat ${dir_local}/Yoyi_RC_mids/14491449* >> ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO


cat ${dir_local}/RONGCHAN_1028_PinPaiCi_$ONE_DAY_AGO | awk '{print $0"\001RONGCHAN_1028_PinPaiCi"}' > ${dir_local}/RONGCHAN_0311_PinPaiCi_$ONE_DAY_AGO
cat ${dir_local}/RONGCHAN_1028_ChanPingCi_$ONE_DAY_AGO | awk '{print $0"\001RONGCHAN_1028_ChanPingCi"}' > ${dir_local}/RONGCHAN_0311_ChanPingCi_$ONE_DAY_AGO
cat ${dir_local}/RONGCHAN_1028_RenQunCi_$ONE_DAY_AGO | awk '{print $0"\001RONGCHAN_1028_RenQunCi"}' > ${dir_local}/RONGCHAN_0311_RenQunCi_$ONE_DAY_AGO
cat ${dir_local}/RONGCHAN_1028_JingPinCi_$ONE_DAY_AGO | awk '{print $0"\001RONGCHAN_1028_JingPinCi"}' > ${dir_local}/RONGCHAN_0311_JingPinCi_$ONE_DAY_AGO
cat ${dir_local}/RONGCHAN_1028_TongYongCi_All_$ONE_DAY_AGO | awk '{print $0"\001RONGCHAN_1028_TongYongCi_All"}' > ${dir_local}/RONGCHAN_0311_TongYongCi_All_$ONE_DAY_AGO

hadoop fs -rm /mroutput/ads/thirdparty_cookie/rongchan_1028_project/RONGCHAN_0311*
hadoop fs -put ${dir_local}/RONGCHAN_0311* /mroutput/ads/thirdparty_cookie/rongchan_1028_project