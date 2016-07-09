__author__ = 'TakiyaHideto'

from pylab import *
import os
import sys
import random

class PlotCumuGraph:
    def __init__(self, input1, input2, date):
        self.input1 = input1
        self.input2 = input2
        self.date = date
        self.indexNumMap1 = {}
        self.indexNumMap2 = {}
        self.cumuDistriMap = {}
        self.keySet = set()

    def __loadDataToDict(self, inputFile):
        dict = {}
        with open(inputFile, "r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                index = elements[0]
                if index.isdigit():
                    if index in dict.keys():
                        dict[index] += 1
                    else:
                        dict[index] = 1
                else:
                    print line
        return dict

    def __convertStrToInt(self, strList):
        return map(lambda x: int(x), strList)

    def __convertIntToStr(self, intList):
        return map(lambda x: str(x), intList)

    def __extractMapToList(self, dict):
        list1 = sorted(self.__convertStrToInt(dict.keys()))
        list2 = []
        for key in self.__convertIntToStr(list1):
            list2.append(dict[key])
        return list1, list2

    def __calValueSum(self, dict):
        sum = 0
        for key in dict.keys():
            sum += dict[key]
        return sum

    def __loadCumuMap(self, dict):
        self.cumuDistriMap = {}
        sum = self.__calValueSum(dict)
        tempSum = 0
        for key in self.__convertIntToStr(sorted(self.__convertStrToInt(dict.keys()))):
            tempSum += dict[key]
            self.cumuDistriMap[key] = float(tempSum)/float(sum)
        return self.cumuDistriMap

    def __plotGraph(self, dict1, dict2):
        xlabel("price")
        ylabel("probability")
        xList, yList = self.__extractMapToList(self.__loadCumuMap(dict1))
        plot(xList, yList, label="price_probability_cumu_1")
        xList, yList = self.__extractMapToList(self.__loadCumuMap(dict2))
        plot(xList, yList, label="price_probability_cumu_2")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'order_rival_{}.png'.format(self.date)))
        close()

    def runMe(self):
        self.indexNumMap1 = self.__loadDataToDict(self.input1)
        self.indexNumMap2 = self.__loadDataToDict(self.input2)
        self.__plotGraph(self.indexNumMap1, self.indexNumMap2)

if __name__ == "__main__":
    inputFile1 = "/home/dongjiahao/tempResult6.txt"
    inputFile2 = "/home/dongjiahao/tempResult5.txt"
    date = "2016-04-14"
    job = PlotCumuGraph(inputFile1, inputFile2, date)
    job.runMe()
