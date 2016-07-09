import sys
import os

class HandleOriginDataForXgboost:
    def __init__(self, input, output, featMapPath):
        self.input = input
        self.output = output
        self.featMapPath = featMapPath
        self.featMap = {}
        self.dataList = []
        self.processCt = 0

    def __showProcess(self):
        self.processCt += 1
        if self.processCt%1000000 == 1:
            print "loading {0} data".format(str(self.processCt))

    def __loadData(self):
        print "loading original data"
        count = len(self.featMap) + 1
        newLine = ""
        with open(self.input, "r") as fileIn:
            for line in fileIn:
                self.__showProcess()
                elements = line.rstrip().split(" ")
                if elements[0] == "1":
                    label = "1"
                elif elements[0] == "0":
                    label = "0"
                for i in range(1,len(elements)):
                    try:
                        if elements[i].startswith("_:"):
                            continue
                        if elements[i]!="":
                            feat = elements[i].split(":")[0]
                            weight = elements[i].split(":")[1]
                            if weight=="" or " " in weight:
                                print "\t\t illegal feature:", weight
                                continue
                            if feat in self.featMap:
                                newLine += " " + self.featMap[feat] + ":" + weight
                            else:
                                newLine += " " + str(count) + ":" + weight
                                self.featMap[feat] = str(count)
                                count += 1
                    except IndexError:
                        pass
                newLine = label + newLine
                self.dataList.append(newLine)
                newLine = ""

    def __loadFeatMap(self):
        if os.path.exists(featMapPath):
            print "loading feature map"
            with open(self.featMapPath, "r") as fileIn:
                for line in fileIn:
                    elements = line.rstrip().split("\t")
                    self.featMap[elements[1]] = elements[0]
            print "have loaded {0} features".format(len(self.featMap))
        else:
            print "do not exist feature map and initialize new feature map"

    def __outputFeatMap(self):
        print "saving feature map"
        with open(self.featMapPath, "w") as fileOut:
            for key in self.featMap.keys():
                fileOut.write("{0}\t{1}\t{2}\n".format(self.featMap[key], key, "i"))

    def __outputDataToFile(self):
        print "generating new format data"
        with open(self.output,"w") as fileOut:
            for data in self.dataList:
                fileOut.write("{0}\n".format(data))

    def runMe(self):
        self.__loadFeatMap()
        self.__loadData()
        self.__outputFeatMap()
        self.__outputDataToFile()

if __name__ == "__main__":
    input = sys.argv[1]
    featMapPath = sys.argv[2]
    output = input + "_XgFormat"
    if len(sys.argv) != 3:
        print len(sys.argv)
        print "<original data path> <feature map path> "
        exit(1)
    job = HandleOriginDataForXgboost(input,output, featMapPath)
    job.runMe()