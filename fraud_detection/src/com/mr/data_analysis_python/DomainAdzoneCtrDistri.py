__author__ = 'TakiyaHideto'

from pylab import *
import os

class DomainAdzoneCtrDistri:
    def __init__(self, inputFile):
        self.inputFile = inputFile
        self.domainMap = {}
        self.domainImpMap = {}
        self.ctrIntervalMap = {}

    def __loadData(self):
        with open(self.inputFile, "r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                domain = elements[0]
                imp = elements[1]
                clk = elements[2]
                ctr = elements[3]
                self.domainMap[domain] = float(ctr)
                self.domainImpMap[domain] = int(imp)

    def __ctrIntervalMap(self, ctr):
        gap = 0.0001
        for i in range(1,200):
            if ctr < float(i)*gap:
                return str(i)
        return "201"

    def __intervalMap(self):
        for domain in self.domainMap.keys():
            if self.domainMap[domain]==0.00:
                # print domain
                continue
            interval = self.__ctrIntervalMap(self.domainMap[domain])
            if interval in self.ctrIntervalMap.keys():
                self.ctrIntervalMap[interval] += 1
            else:
                self.ctrIntervalMap[interval] = 1

    def __convertStrToInt(self, strList):
        return map(lambda x: int(x), strList)

    def __convertIntToStr(self, intList):
        return map(lambda x: str(x), intList)

    def __extractMapToList(self, dict):
        xlist = (sorted(self.__convertStrToInt(dict.keys())))
        ylist = []
        for key in xlist:
            ylist.append(int(dict[str(key)]))
        return xlist, ylist

    def __plotGraph(self):
        xlabel("ctr interval index")
        ylabel("num or traffic")

        xList, yList = self.__extractMapToList(self.ctrIntervalMap)
        bar(xList, (yList), label="adzone_count")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'domain_adzone_ctr_distri_bar.png'))
        close()

    def runMe(self):
        self.__loadData()
        self.__intervalMap()
        self.__plotGraph()

if __name__ == "__main__":
    inputFile = "/home/dongjiahao/tempResult3.txt"
    job = DomainAdzoneCtrDistri(inputFile)
    job.runMe()
