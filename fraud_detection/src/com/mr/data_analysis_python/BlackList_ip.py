__author__ = 'Jiahao Dong'

class BlackListIp:
    def __init__(self, inputFile, outputFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.ipSet = set()
        self.ipList = []
        self.tempSet = set()

    def __loadIpData(self):
        with open(self.inputFile, "r") as fileIn:
            ipCfield = ""
            for line in fileIn:
                elements = line.rstrip().split("\t")
                ipEle = elements[0].split(".")
                if len(ipEle) != 4:
                    continue
                # initialize ipCfield
                if ipCfield == "":
                    ipCfield = ipEle[0] + "." + ipEle[1] + "." + ipEle[2]
                try:
                    if ipCfield in elements[0]:
                        self.tempSet.add(elements[0])
                        continue
                    else:
                        if len(self.tempSet)>2:
                            for ip in self.tempSet:
                                self.ipSet.add(ip)
                                self.ipList.append(ip)
                    self.tempSet = set()
                    ipCfield = ipEle[0] + "." + ipEle[1] + "." + ipEle[2]
                    self.tempSet.add(elements[0])
                except IndexError:
                    pass

    def __writeToFile(self):
        with open(self.outputFile, "w") as fileOut:
            for cookie in self.ipList:
                fileOut.write("{}\n".format(cookie))

    def runMe(self):
        self.__loadIpData()
        self.__writeToFile()

if __name__ == "__main__":
    jobIpInputFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/ipInput"
    jobIpOutPutFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/ipBL"
    job = BlackListIp(jobIpInputFile,jobIpOutPutFile)
    job.runMe()