
class CalKeepTimeGraph:
    def __init__(self,inputFile):
        self.inputFile = inputFile
        self.keepTimeDict = {}
        self.timeInterval = 10
        self.timeIntervalNum = 3600/self.timeInterval

    def __loadData(self):
        with open(self.inputFile,"r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
