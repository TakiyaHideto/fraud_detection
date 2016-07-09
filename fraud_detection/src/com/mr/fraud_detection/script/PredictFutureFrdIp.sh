#!/usr/bin/env bash

set timeout 3000
spawn sh /data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/fraud_detection/script/PredictFutureFrdIpExpectSh.sh
expect {
    "(yes/no)?"
    {send "yes\n";exp_continue}
    "dongjiahao@rtpr1a1.sora.cm1's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "dongjiahao@rtpr1a2.sora.cm1's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "dongjiahao@rtpr1a3.sora.cm1's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "dongjiahao@rtpr1a4.sora.cm1's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "dongjiahao@rtpr2a1.sora.cm1's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "dongjiahao@rtpr2a2.sora.cm1's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "dongjiahao@rtpr2a3.sora.cm1's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "dongjiahao@rtpr2a4.sora.cm1's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "dongjiahao@10.0.3.95's password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n";exp_continue}
    "Password:"
    {send "a1S0lGyTB4JMQEcr0bYT\n"}}
#interact

