__author__ = 'Jiahao Dong'

import math
import os
import sys

class BlackListIpWithInfo:
    def __init__(self, inputFile, outputFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.ipDateInfoMap = {}
        self.ipFieldSd = {}

    def __loadIpData(self):
        with open(self.inputFile, "r") as fileIn:
            ipCfield = ""
            for line in fileIn:
                try:
                    # 0     ipInfo
                    # 1     logDate
                    # 2     ipField
                    # 3     uaCount
                    # 4     browserCount
                    # 5     osCount
                    # 6     yoyiCookieCount
                    # 7     domainCount
                    # 8     adzoneIdCount
                    # 9     browserCount
                    # 10    jaccardCookie
                    # 11    jaccardDomain
                    # 12    jaccardAdzoneId
                    # 13    jaccardBrowser
                    # 14    jaccardUserAgent
                    # 15    impressionNum
                    # 16    clickNum
                    # 17    ctr
                    # 18    meanIpImp
                    # 19    sdIpImp
                    # 20    meanIpClk
                    # 21    sdIpClk

                    elements = line.rstrip().split("\t")
                    ip = elements[0]
                    logDate = elements[1]
                    ipField = elements[2]
                    cookieSimi = elements[10]
                    domainSimi = elements[11]
                    adzoneSimi = elements[12]
                    browserSimi = elements[13]
                    userAgentSimi = elements[14]
                    impression = elements[15]
                    impSd = elements[19];
                    clkSd = elements[21];
                    if float(clkSd)<10.0:
                        if float(domainSimi)>0.0 \
                                and float(adzoneSimi)>0.0 \
                                and float(browserSimi)>0.0 :
                            if ipField+logDate in self.ipDateInfoMap.keys():
                                self.ipDateInfoMap[ipField+logDate].append(line)
                            else:
                                self.ipDateInfoMap[ipField+logDate] = [line]
                except IndexError:
                    pass

    def __writeToFile(self):
        with open(self.outputFile, "w") as fileOut:
            for key in self.ipDateInfoMap.keys():
                if (len(self.ipDateInfoMap[key])>2):
                    print len(self.ipDateInfoMap[key])
                    for info in self.ipDateInfoMap[key]:
                        fileOut.write("{0}".format(info))
                else:
                    continue

    def runMe(self):
        self.__loadIpData()
        self.__writeToFile()

if __name__ == "__main__":
    jobIpInputFile = sys.argv[1]
    jobIpOutPutFile = sys.argv[2]
    job = BlackListIpWithInfo(jobIpInputFile,jobIpOutPutFile)
    job.runMe()