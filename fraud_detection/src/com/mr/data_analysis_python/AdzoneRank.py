__author__ = 'TakiyaHideto'

import sys
import math

class AdzoneRank:
    def __init__(self, inputFile, outputFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.meanCtr = sys.argv[2]
        self.dateClkMean = sys.argv[3]
        self.ipClkMean = sys.argv[4]
        self.valueMaxMinDict = {}
        self.alpha = 1
        self.rankInfoList = []

    def __loadData(self):
        print "dateClkMean:",self.dateClkMean
        print "ipClkMean:",self.ipClkMean
        with open(self.inputFile, "r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                adzoneId = elements[0]
                domain = elements[1]
                impressionSum = elements[2]
                clickSum = elements[3]
                adzoneCtrGap = float(clickSum)/float(impressionSum) - float(self.meanCtr)
                sdCampaignCtr = elements[4]
                dateClkMeanGap = str(float(elements[5]) - float(self.dateClkMean))
                sdClkDate = elements[6]
                sdCtrDate = elements[7]
                ipClkMeanGap = str(float(elements[8]) - float(self.ipClkMean))
                ipCumu = elements[9]
                self.__putInfoToDict(key="adzoneCtrGap", value=adzoneCtrGap, indicator="max")
                self.__putInfoToDict(key="adzoneCtrGap", value=adzoneCtrGap, indicator="min")
                self.__putInfoToDict(key="sdCampaignCtr", value=sdCampaignCtr, indicator="max")
                self.__putInfoToDict(key="sdCampaignCtr", value=sdCampaignCtr, indicator="min")
                self.__putInfoToDict(key="dateClkMeanGap", value=dateClkMeanGap, indicator="max")
                self.__putInfoToDict(key="dateClkMeanGap", value=dateClkMeanGap, indicator="min")
                self.__putInfoToDict(key="sdClkDate", value=sdClkDate, indicator="max")
                self.__putInfoToDict(key="sdClkDate", value=sdClkDate, indicator="min")
                self.__putInfoToDict(key="sdCtrDate", value=sdCtrDate, indicator="max")
                self.__putInfoToDict(key="sdCtrDate", value=sdCtrDate, indicator="min")
                self.__putInfoToDict(key="ipClkMeanGap", value=ipClkMeanGap, indicator="max")
                self.__putInfoToDict(key="ipClkMeanGap", value=ipClkMeanGap, indicator="min")
                self.__putInfoToDict(key="ipCumu", value=ipCumu, indicator="max")
                self.__putInfoToDict(key="ipCumu", value=ipCumu, indicator="min")

    def __putInfoToDict(self, key, value, indicator):
        if key+indicator in self.valueMaxMinDict.keys():
            if indicator == "min":
                if float(value) < self.valueMaxMinDict.get(key+indicator):
                    self.valueMaxMinDict[key+indicator] = float(value)
            elif indicator == "max":
                if float(value) > self.valueMaxMinDict.get(key+indicator):
                    self.valueMaxMinDict[key+indicator] = float(value)
        else:
            if indicator == "min":
                self.valueMaxMinDict[key+indicator] = float(value)
            elif indicator == "max":
                self.valueMaxMinDict[key+indicator] = float(value)

    def __calZscore(self):
        with open(self.inputFile, "r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                adzoneId = elements[0]
                domain = elements[1]
                impressClk = elements[2]
                clickSum = elements[3]
                adzoneCtrGap = self.__zscore("adzoneCtrGap", float(clickSum)/float(impressClk)) + self.alpha
                sdCampaignCtr = self.__zscore("sdCampaignCtr", elements[4]) + self.alpha
                dateClkMeanGap = self.__zscore("dateClkMeanGap", str(float(elements[5]) - float(self.dateClkMean))) + self.alpha
                sdClkDate = self.__zscore("sdClkDate", elements[6]) + self.alpha
                sdCtrDate = self.__zscore("sdCtrDate", elements[7]) + self.alpha
                ipClkMeanGap = self.__zscore("ipClkMeanGap", str(float(elements[8]) - float(self.ipClkMean))) + self.alpha
                ipCumu = self.__zscore("ipCumu", elements[9]) + self.alpha
                finalScore = self.__calFinalScore(adzoneCtrGap,sdCampaignCtr,dateClkMeanGap, sdClkDate, sdCtrDate, ipClkMeanGap, ipCumu)
                if domain == "":
                    domain = "unknown_domain"
                self.rankInfoList.append(adzoneId +
                                         "\t" + domain +
                                         "\t" + clickSum +
                                         "\t" + str(float(clickSum)/float(impressClk)) +
                                         "\t" + str(adzoneCtrGap) +
                                         "\t" + str(sdCampaignCtr) +
                                         "\t" + str(dateClkMeanGap) +
                                         "\t" + str(sdClkDate) +
                                         "\t" + str(sdCtrDate) +
                                         "\t" + str(ipClkMeanGap) +
                                         "\t" + str(ipCumu) +
                                         "\t" + str(finalScore))

    def __zscore(self, field, value):
        max = self.valueMaxMinDict[field+"max"]
        min = self.valueMaxMinDict[field+"min"]
        zscoreValue = (float(value)-min)/(max-min)
        return zscoreValue * 100

    def __printExtremNum(self):
        for key in self.valueMaxMinDict.keys():
            print "{0}:{1}\n".format(key,self.valueMaxMinDict[key])

    def __calFinalScore(self, adzoneCtrGap, sdCampaignCtr, dateClkMeanGap, sdClkDate, sdCtrDate, ipClkMeanGap, ipCumu):
        finalScore = adzoneCtrGap * sdCampaignCtr * dateClkMeanGap * sdClkDate * sdCtrDate * ipClkMeanGap / ipCumu
        finalScore = adzoneCtrGap * sdCampaignCtr * sdCtrDate\
                     / ipCumu
        return finalScore

    def __writeRankInfoToFile(self):
        with open(self.outputFile, "w") as fileOut:
            for info in self.rankInfoList:
                fileOut.write("{0}\n".format(info))

    def runMe(self):
        self.__loadData()
        self.__calZscore()
        self.__writeRankInfoToFile()
        self.__printExtremNum()

class DomainRankFromAdzone:
    def __init__(self, inputFile, outputFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.domainScoreMap = {}

    def __loadData(self):
        with open(self.inputFile, "r") as fileIn:
            for line in fileIn:
                elements = line.split("\t")
                domain = elements[2]
                adzoneRank = float(elements[0])
                if domain in self.domainScoreMap.keys():
                    self.domainScoreMap[domain].append(adzoneRank)
                else:
                    self.domainScoreMap[domain] = [adzoneRank]

    def __calStandardDeviation(self, valueList):
        sum = 0.0
        n = 0
        mean = self.__calMean(valueList)
        for value in valueList:
            sum += math.pow(value-mean,2)
            n += 1
        sd = math.sqrt(sum/float(n))
        return sd

    def __calMean(self,list):
        sum = 0.0
        n = 0
        for value in list:
            sum += value
            n += 1
        mean = sum/float(n)
        return mean

    def __calRankScore(self, mean, sd):
        return mean - sd

    def __generateDomainRank(self):
        with open(self.outputFile, "w") as fileOut:
            for key in self.domainScoreMap.keys():
                valueList = self.domainScoreMap[key]
                mean = self.__calMean(valueList)
                sd = self.__calStandardDeviation(valueList)
                score = self.__calRankScore(mean, sd)
                fileOut.write("{0}\t{1}\n".format(key, str(score)))

    def runMe(self):
        self.__loadData()
        self.__generateDomainRank()


if __name__ == "__main__":
    if sys.argv[5] == "adzone":
        inputFile = "/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/original_adzone_file_"+sys.argv[1]
        outputFile = "/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/adzone_rank_"+sys.argv[1]
        job1 = AdzoneRank(inputFile, outputFile)
        job1.runMe()
    elif sys.argv[5] == "domain":
        inputFile = "/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/adzone_rank_file_for_domain_"+sys.argv[1]
        outputFile = "/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/domain_rank_"+sys.argv[1]
        job2 = DomainRankFromAdzone(inputFile, outputFile)
        job2.runMe()