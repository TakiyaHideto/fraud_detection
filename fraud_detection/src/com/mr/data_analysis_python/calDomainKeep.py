class CalDomainKeepTime:
    def __init__(self, input, output):
        self.input = input
        self.output = output
        self.domainTimeDict = {}
        self.domainNumDict = {}

    def __loadData(self):
        with open(self.input,"r") as fileIn:
            for line in fileIn:
                try:
                    elements = line.rstrip().split("\t")
                    if len(elements) == 2:
                        if elements[0] in self.domainTimeDict.keys():
                            self.domainTimeDict[elements[0]] += int(elements[1])
                        else:
                            self.domainTimeDict[elements[0]] = int(elements[1])
                        if elements[0] in self.domainNumDict.keys():
                            self.domainNumDict[elements[0]] += 1
                        else:
                            self.domainNumDict[elements[0]] = 1
                except IndexError:
                    print line
                except ValueError:
                    print line

    def __writeToFile(self):
        with open(self.output,"w") as fileOut:
            for domain in self.domainTimeDict:
                meanKeepTime = float(self.domainTimeDict[domain])/float(self.domainNumDict[domain])
                fileOut.write("{0}:\t{1}\t{2}\n"
                              .format(domain,meanKeepTime,self.domainNumDict[domain]))

    def runMe(self):
        self.__loadData()
        self.__writeToFile()

if __name__ == "__main__":
    inputFile="/home/dongjiahao/temp1"
    outputFile=inputFile+"_"
    job = CalDomainKeepTime(inputFile, outputFile)
    job.runMe()