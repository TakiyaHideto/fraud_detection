#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-1 day"`

#set timeout 3000
#spawn ssh dongjiahao@10.0.8.3 sh /data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/retargeting/script/reachMax.sh
#expect {
#  "(yes/no)?"
#  {send "yes\n";exp_continue}
#  "password:"
#  {send "a1S0lGyTB4JMQEcr0bYT\n"}
#}
#interact

hadoop distcp -update -skipcrccheck hftp://hm.stat.dsp:50070/mroutput/dongjiahao/reachMax/reach_max_${ONE_DAY_AGO} /mroutput/ads/thirdparty_cookie/reach_max