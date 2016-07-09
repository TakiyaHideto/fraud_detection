__author__ = 'TakiyaHideto'

class EvaluateDomainOffline:
    def __init__(self, inputFile, outputFile, domainBlFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.domainBlFile = domainBlFile
        self.domainSet = set()
        self.domainKeepTimeMap = {}
        self.domainKeepTimeCtMap = {}
        self.domainDeepMap = {}
        self.domainDeepCtMap = {}
        self.domainSecondeBounceMap = {}
        self.domainSecondeBounceCtMap = {}

    def __loadDomain(self):
        count = 0
        with open(self.domainBlFile, "r") as fileIn:
            for line in fileIn:
                if count > 1000:
                    break
                self.domainSet.add(line.rstrip().split("\t")[1])
                count += 1
        print "finish loading domain set"

    def __loadData(self):
        with open(self.inputFile, "r") as fileIn:
            for line in fileIn:
                element = line.rstrip().split("\t")
                domain = element[0]
                if domain == "" or (domain not in self.domainSet):
                    continue
                keepTime = element[1]
                deep = element[2]
                secondBounce = element[3]
                if domain in self.domainKeepTimeMap.keys():
                    self.domainKeepTimeMap[domain] += keepTime
                    self.domainKeepTimeCtMap[domain] += 1
                else:
                    self.domainKeepTimeMap[domain] = keepTime
                    self.domainKeepTimeCtMap[domain] = 1
                if domain in self.domainDeepMap.keys():
                    self.domainDeepMap[domain] += deep
                    self.domainDeepCtMap[domain] += 1
                else:
                    self.domainDeepMap[domain] = deep
                    self.domainDeepCtMap[domain] = 1
                if domain in self.domainSecondeBounceMap.keys():
                    self.domainSecondeBounceMap[domain] += secondBounce
                    self.domainSecondeBounceCtMap[domain] += 1
                else:
                    self.domainSecondeBounceMap[domain] = secondBounce
                    self.domainSecondeBounceCtMap[domain] = 1

    def __calEvaluation(self):
        keepTimeSum = 0
        keepTimeCt = 0
        deepSum = 0
        deepCt = 0
        secondBounceSum = 0
        secondBounceCt = 0
        for domain in self.domainSet:
            try:
                keepTimeSum += self.domainKeepTimeMap[domain]*self.domainKeepTimeCtMap[domain]
                keepTimeCt += self.domainKeepTimeCtMap[domain]
                deepSum += self.domainDeepMap[domain]*self.domainDeepCtMap[domain]
                deepCt += self.domainDeepCtMap[domain]
                secondBounceSum += self.domainSecondeBounceMap[domain]*self.domainSecondeBounceCtMap[domain]
                secondBounceCt += self.domainSecondeBounceCtMap[domain]
            except KeyError:
                continue
        keepTime = float(keepTimeSum)/float(keepTimeCt)
        deep = float(deepSum)/float(deepCt)
        secondBounce = float(secondBounceSum)/float(secondBounceCt)
        return keepTime, deep, secondBounce

    def __showEvaluation(self):
        keepTime, deep, secondBounce = self.__calEvaluation()
        print "keep_time:{0}\n deep:{1}\n second_bounce_rate:{2}\n".format(keepTime, deep, secondBounce)

    def runMe(self):
        self.__loadDomain()
        self.__loadData()
        self.__showEvaluation()

if __name__ == "__main__":
    inputFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/effect_analysis/script/evaluation_data"
    outputFile = ""
    domainBlFile = "/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/domain_rank_2016-04-29"
    job = EvaluateDomainOffline(inputFile, outputFile, domainBlFile)
    job.runMe()