__author__ = 'Jiahao Dong'

class BlackListYoyiCookie:
    def __init__(self, inputFile, outputFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.cookieSet = set()

    def __loadCookieData(self):
        with open(self.inputFile, "r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                try:
                    if "%3D" in elements[0]:
                        cookie = elements[0][3:]
                    else:
                        cookie = elements[0]
                    self.cookieSet.add(cookie)
                except IndexError:
                    pass

    def __writeToFile(self):
        with open(self.outputFile, "w") as fileOut:
            for cookie in self.cookieSet:
                fileOut.write("{}\n".format(cookie))

    def runMe(self):
        self.__loadCookieData()
        self.__writeToFile()

if __name__ == "__main__":
    input = ""
    output = ""
    job = BlackListYoyiCookie(input, output)
    job.runMe()