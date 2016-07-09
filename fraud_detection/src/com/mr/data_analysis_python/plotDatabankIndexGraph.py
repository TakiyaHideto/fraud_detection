__author__ = 'TakiyaHideto'

from pylab import *
import os
import sys
import random

class PlotDatabankIndexGraph:
    def __init__(self, input):
        self.input = input
        self.indexNumMap = {}

    def __loadData(self):
        with open(self.input, "r") as fileIn:
            for line in fileIn:
                # if random.randint(0,99) > 10:
                #     continue
                elements = line.rstrip().split("\t")
                index = elements[0]
                if index.isdigit() and int(index)>0 and int(index)<100:
                    if index in self.indexNumMap.keys():
                        self.indexNumMap[index] += 1
                    else:
                        self.indexNumMap[index] = 1

    def __convertStrToInt(self, strList):
        return map(lambda x: int(x), strList)

    def __convertIntToStr(self, intList):
        return map(lambda x: str(x), intList)

    def __extractMapToList(self, dict):
        list1 = sorted(self.__convertStrToInt(dict.keys()))
        list2 = []
        count = 0
        for key in self.__convertIntToStr(list1):
            list2.append(self.indexNumMap[key])
        return list1, list2

    def __plotGraph(self):
        xlabel("index")
        ylabel("num")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.indexNumMap)
        plot(xList, yList, label="keep_time")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'index.png'))
        close()

    def runMe(self):
        self.__loadData()
        self.__plotGraph()

if __name__ == "__main__":
    inputFile = "/home/dongjiahao/tempResult3.txt"
    job = PlotDatabankIndexGraph(inputFile)
    job.runMe()