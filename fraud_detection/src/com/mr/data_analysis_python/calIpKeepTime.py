__author__ = 'TakiyaHideto'

import os
from pylab import *
import numpy as np

class CalIpKeepTime:
    def __init__(self,inputFile):
        self.inputFile = inputFile
        self.reachNumDict = {}
        self.bounceNumDict = {}

    def __loadData(self):
        with open(self.inputFile,"r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                reachNum = elements[2]
                bounceNum = elements[5]
                if reachNum in self.reachNumDict.keys():
                    self.reachNumDict[reachNum] += 1
                else:
                    self.reachNumDict[reachNum] = 1
                if bounceNum in self.bounceNumDict.keys():
                    self.bounceNumDict[bounceNum] += 1
                else:
                    self.bounceNumDict[bounceNum] = 1

    def __convertStrToInt(self, strList):
        return map(lambda x: int(x), strList)

    def __convertIntToStr(self, intList):
        return map(lambda x: str(x), intList)

    def __plotGraph(self,set,name):
        while True:
            try:
                a = self.__convertIntToStr(sorted(self.__convertStrToInt(set.keys())))
                b = []
                for key in a:
                    b.append(set[key])
                xlabel("reach/bounce")
                ylabel("volume")
                plot(a,b)
                savefig("./{}".format(name))
                # show()
                close("all")
            except StopIteration:
                break

    def runMe(self):
        self.__loadData()
        self.__plotGraph(self.bounceNumDict,name="1")
        self.__plotGraph(self.reachNumDict,name="2")

if __name__ == "__main__":
    inputFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/fraud_detection/script/temp_function"
    inputFile = "/data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/data_analysis_python/temp_function"
    job = CalIpKeepTime(inputFile)
    job.runMe()

