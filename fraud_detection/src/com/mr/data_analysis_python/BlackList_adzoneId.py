__author__ = 'Jiahao Dong'

class BlackListAdzoneId:
    def __init__(self, inputFile, outputFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.adzoneIdset = set()

    def __loadAdzoneIdData(self):
        with open(self.inputFile, "r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                try:
                    # 4:impression
                    # 6:ctr
                    if int(elements[4])>50 and float(elements[6])>0.03:
                        self.adzoneIdset.add(elements[0])
                except IndexError:
                    pass

    def __writeToFile(self):
        with open(self.outputFile, "w") as fileOut:
            for cookie in self.adzoneIdset:
                fileOut.write("{}\n".format(cookie))

    def runMe(self):
        self.__loadAdzoneIdData()
        self.__writeToFile()


