#!/usr/bin/env bash

ONE_DAY_AGO=`date +%Y-%m-%d --date="-0 day"`

sh /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/fraud_detection/script/PredictFutureFrdIpHadoop.sh

assh -h rtpr1a1.sora.cm1 "sudo -u ads scp dongjiahao@10.0.3.95:/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_$ONE_DAY_AGO /home/ads/rtp-server/data/fraud_ip.txt"
assh -h rtpr1a2.sora.cm1 "sudo -u ads scp dongjiahao@10.0.3.95:/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_$ONE_DAY_AGO /home/ads/rtp-server/data/fraud_ip.txt"
assh -h rtpr1a3.sora.cm1 "sudo -u ads scp dongjiahao@10.0.3.95:/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_$ONE_DAY_AGO /home/ads/rtp-server/data/fraud_ip.txt"
assh -h rtpr1a4.sora.cm1 "sudo -u ads scp dongjiahao@10.0.3.95:/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_$ONE_DAY_AGO /home/ads/rtp-server/data/fraud_ip.txt"
assh -h rtpr2a1.sora.cm1 "sudo -u ads scp dongjiahao@10.0.3.95:/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_$ONE_DAY_AGO /home/ads/rtp-server/data/fraud_ip.txt"
assh -h rtpr2a2.sora.cm1 "sudo -u ads scp dongjiahao@10.0.3.95:/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_$ONE_DAY_AGO /home/ads/rtp-server/data/fraud_ip.txt"
assh -h rtpr2a3.sora.cm1 "sudo -u ads scp dongjiahao@10.0.3.95:/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_$ONE_DAY_AGO /home/ads/rtp-server/data/fraud_ip.txt"
assh -h rtpr2a4.sora.cm1 "sudo -u ads scp dongjiahao@10.0.3.95:/data/dongjiahao/svn_project/fraudDetection/file/fraud_clk_ip_shielded/fraud_ip_all_$ONE_DAY_AGO /home/ads/rtp-server/data/fraud_ip.txt"

assh -h rtpr1a1.sora.cm1 'sudo -u ads echo 3 > /home/ads/rtp-server/data/updater_flag.txt'
assh -h rtpr1a2.sora.cm1 'sudo -u ads echo 3 > /home/ads/rtp-server/data/updater_flag.txt'
assh -h rtpr1a3.sora.cm1 'sudo -u ads echo 3 > /home/ads/rtp-server/data/updater_flag.txt'
assh -h rtpr1a4.sora.cm1 'sudo -u ads echo 3 > /home/ads/rtp-server/data/updater_flag.txt'
assh -h rtpr2a1.sora.cm1 'sudo -u ads echo 3 > /home/ads/rtp-server/data/updater_flag.txt'
assh -h rtpr2a2.sora.cm1 'sudo -u ads echo 3 > /home/ads/rtp-server/data/updater_flag.txt'
assh -h rtpr2a3.sora.cm1 'sudo -u ads echo 3 > /home/ads/rtp-server/data/updater_flag.txt'
assh -h rtpr2a4.sora.cm1 'sudo -u ads echo 3 > /home/ads/rtp-server/data/updater_flag.txt'

