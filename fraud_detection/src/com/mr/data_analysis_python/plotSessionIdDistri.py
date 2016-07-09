__author__ = 'TakiyaHideto'

from pylab import *
import os

class PlotSessionIdDistri:
    def __init__(self, input, date):
        self.input = input
        self.date = date
        self.indexNumMap = {}

    def __loadData(self):
        with open(self.input, "r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                index = elements[1]
                if index.isdigit():
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
        xlabel("order_rival_num")
        ylabel("count")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.indexNumMap)
        plot(xList, yList, label="order_rival")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'order_rival_{}.png'.format(self.date)))
        close()

    def runMe(self):
        self.__loadData()
        self.__plotGraph()

if __name__ == "__main__":
    inputFile = "/home/dongjiahao/tempResult5.txt"
    date = "2016-04-14"
    job = PlotSessionIdDistri(inputFile,date)
    job.runMe()
    inputFile = "/home/dongjiahao/tempResult6.txt"
    date = "2016-04-16"
    job = PlotSessionIdDistri(inputFile,date)
    job.runMe()