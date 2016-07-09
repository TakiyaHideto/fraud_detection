#!/usr/bin/env bash
#
#for i in {1..15}
#do
#ONE_DAY_AGO=`date +%Y-%m-%d --date="-$i day"`
#hadoop fs -text /share_data/new_basedata/log_date=${ONE_DAY_AGO} > basedata_local_${ONE_DAY_AGO}
#done

## this part is aiming to make feat index for pc basedata
#hadoop fs -text /mroutput/dongjiahao/ExtractFeature/log_date=${ONE_DAY_AGO}/domain* > /data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/domain
#hadoop fs -text /mroutput/dongjiahao/ExtractFeature/log_date=${ONE_DAY_AGO}/host* > /data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/host
#hadoop fs -text /mroutput/dongjiahao/ExtractFeature/log_date=${ONE_DAY_AGO}/url* > /data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/url
#hadoop fs -text /mroutput/dongjiahao/ExtractFeature/log_date=${ONE_DAY_AGO}/gender* > /data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/gender
#hadoop fs -text /mroutput/dongjiahao/ExtractFeature/log_date=${ONE_DAY_AGO}/tag* > /data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/tag
#hadoop fs -text /mroutput/dongjiahao/ExtractFeature/log_date=${ONE_DAY_AGO}/ip* > /data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/ip
#
#dir=/data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/
#
#cat ${dir}/domain | awk '{print $0"\t"NR}' > ${dir}/domain_index
#cat ${dir}/host | awk '{print $0"\t"NR}' > ${dir}/host_index
#cat ${dir}/url | awk '{print $0"\t"NR}' > ${dir}/url_index
#cat ${dir}/gender | awk '{print $0"\t"NR}' > ${dir}/gender_index
#cat ${dir}/tag | awk '{print $0"\t"NR}' > ${dir}/tag_index
#cat ${dir}/ip | awk '{print $0"\t"NR}' > ${dir}/ip_index
#
#hadoop fs -rm /share_data/cooperation_project/jiaoda_frd_det/feat_id_index/*
#hadoop fs -put ${dir}/*_index /share_data/cooperation_project/jiaoda_frd_det/feat_id_index

##################################################################################
# this part is aiming to make feat index for mobile basedata

ONE_DAY_AGO=`date +%Y-%m-%d --date="-2 day"`
dir=/data/dongjiahao/svn_project/fraudDetection/file/feature_id_dict/mobile_index
mkdir ${dir}

hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/domain* > ${dir}/domain
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/host* > ${dir}/host
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/url* > ${dir}/url
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/gender* > ${dir}/gender
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/tag* > ${dir}/tag
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/ip* > ${dir}/ip

hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/devicePlatform* > ${dir}/devicePlatform
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/deviceOs* > ${dir}/deviceOs
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/deviceOsVersion* > ${dir}/deviceOsVersion
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/deviceBrand* > ${dir}/deviceBrand
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/deviceModel* > ${dir}/deviceModel
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/deviceLocation* > ${dir}/deviceLocation
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/deviceResultion* > ${dir}/deviceResultion
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/networkType* > ${dir}/networkType
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/networkCarrier* > ${dir}/networkCarrier
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/appName* > ${dir}/appName
hadoop fs -text /mroutput/dongjiahao/ExtractFeatureMobile/log_date=${ONE_DAY_AGO}/appCategories* > ${dir}/appCategories

cat ${dir}/domain | awk '{print $0"\t"NR}' > ${dir}/domain_index
cat ${dir}/host | awk '{print $0"\t"NR}' > ${dir}/host_index
cat ${dir}/url | awk '{print $0"\t"NR}' > ${dir}/url_index
cat ${dir}/gender | awk '{print $0"\t"NR}' > ${dir}/gender_index
cat ${dir}/tag | awk '{print $0"\t"NR}' > ${dir}/tag_index
cat ${dir}/ip | awk '{print $0"\t"NR}' > ${dir}/ip_index

cat ${dir}/devicePlatform | awk '{print $0"\t"NR}' > ${dir}/devicePlatform_index
cat ${dir}/deviceOs | awk '{print $0"\t"NR}' > ${dir}/deviceOs_index
cat ${dir}/deviceOsVersion | awk '{print $0"\t"NR}' > ${dir}/deviceOsVersion_index
cat ${dir}/deviceBrand | awk '{print $0"\t"NR}' > ${dir}/deviceBrand_index
cat ${dir}/deviceModel | awk '{print $0"\t"NR}' > ${dir}/deviceModel_index
cat ${dir}/deviceLocation | awk '{print $0"\t"NR}' > ${dir}/deviceLocation_index
cat ${dir}/deviceResultion | awk '{print $0"\t"NR}' > ${dir}/deviceResultion_index
cat ${dir}/networkType | awk '{print $0"\t"NR}' > ${dir}/networkType_index
cat ${dir}/networkCarrier | awk '{print $0"\t"NR}' > ${dir}/networkCarrier_index
cat ${dir}/appName | awk '{print $0"\t"NR}' > ${dir}/appName_index
cat ${dir}/appCategories | awk '{print $0"\t"NR}' > ${dir}/appCategories_index

hadoop fs -rm /share_data/cooperation_project/jiaoda_cp/feat_id_index/mobile/*
hadoop fs -put ${dir}/*_index /share_data/cooperation_project/jiaoda_cp/feat_id_index/mobile
