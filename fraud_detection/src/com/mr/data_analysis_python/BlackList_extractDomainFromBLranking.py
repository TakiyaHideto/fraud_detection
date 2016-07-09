__author__ = 'Jiahao Dong'

class ExtractDomainFromRankingList:
    def __init__(self, input, output):
        self.input = input
        self.output = output
        self.domainSet = set()

    def loadRankingList(self):
        counter = 0
        with open(self.input,"r") as fileIn:
            for line in fileIn:
                for li in line.split("\r"):
                    # print counter
                    if counter>9500:
                        break
                    counter += 1
                    element = li.rstrip().split("\t")
                    self.domainSet.add(element[0])
        print counter


    def outputDomain(self):
        with open(self.output,"w") as fileOut:
            for domain in self.domainSet:
                fileOut.write("{}\n".format(domain))

    def runMe(self):
        self.loadRankingList()
        self.outputDomain()

if __name__ == "__main__":
    input = "/data/dongjiahao/svnProject/fraudDetection/dataLog/BLdomianRanking.txt"
    output = "/data/dongjiahao/svnProject/fraudDetection/hive_result/domainBL"
    job = ExtractDomainFromRankingList(input,output)
    job.runMe()