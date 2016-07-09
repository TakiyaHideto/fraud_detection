__author__ = 'Jiahao Dong'

class BlackListIpWithInfo:
    def __init__(self, inputFile, outputFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.ipSet = set()
        self.ipList = []
        self.tempSet = set()
        self.infoTempList = []
        self.infoList = []

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
                    print ipCfield
                try:
                    if ipCfield in elements[0]:
                        print ipCfield
                        self.infoTempList.append(line)
                        continue
                    else:
                        if len(self.infoTempList)>2:
                            for info in self.infoTempList:
                                self.infoList.append(info)
                    self.infoTempList = []
                    self.infoTempList.append(line)
                    ipCfield = ipEle[0] + "." + ipEle[1] + "." + ipEle[2]
                except IndexError:
                    pass

    def __writeToFile(self):
        with open(self.outputFile, "w") as fileOut:
            for info in self.infoList:
                fileOut.write("{}".format(info))

    def runMe(self):
        self.__loadIpData()
        self.__writeToFile()

if __name__ == "__main__":
    print "--"
    jobIpInputFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/ipInput"
    jobIpOutPutFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/ipBL"
    job = BlackListIpWithInfo(jobIpInputFile,jobIpOutPutFile)
    job.runMe()