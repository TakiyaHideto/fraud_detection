__author__ = 'TakiyaHideto'

from pylab import *
import os

class PlotDomainDistribution:
    def __init__(self, input):
        self.input = input
        self.domainSetAll = set()
        self.domainMap09 = {}
        self.domainMap10 = {}
        self.domainMap11 = {}
        self.domainMap12 = {}
        self.domainMap13 = {}
        self.domainMap14 = {}
        self.domainMap15 = {}
        self.domainMap16 = {}
        self.domainMap17 = {}

    def __loadData(self):
        with open(self.input, "r") as fileIn:
            for line in fileIn:
                elements = line.rstrip().split("\t")
                domain = elements[0]
                date = elements[1]
                bid = elements[2]
                imp = int(elements[3])
                clk = int(elements[4])
                reach = int(elements[5])
                reach_rate = float(elements[7])
                value = imp
                # if ctrBucket=="FM" or ctrBucket=="FFM" or ctrBucket=="ID_lr_UA":
                if (clk>=0):
                    self.domainSetAll.add(domain)
                    if (date=="2016-04-09"):
                        if domain in self.domainMap09.keys():
                            self.domainMap09[domain] += value
                        else:
                            self.domainMap09[domain] = value
                    elif (date=="2016-04-10"):
                        if domain in self.domainMap10.keys():
                            self.domainMap10[domain] += value
                        else:
                            self.domainMap10[domain] = value
                    elif (date=="2016-04-11"):
                        if domain in self.domainMap11.keys():
                            self.domainMap11[domain] += value
                        else:
                            self.domainMap11[domain] = value
                    elif (date=="2016-04-12"):
                        if domain in self.domainMap12.keys():
                            self.domainMap12[domain] += value
                        else:
                            self.domainMap12[domain] = value
                    elif (date=="2016-04-13"):
                        if domain in self.domainMap13.keys():
                            self.domainMap13[domain] += value
                        else:
                            self.domainMap13[domain] = value
                    elif (date=="2016-04-14"):
                        if domain in self.domainMap14.keys():
                            self.domainMap14[domain] += value
                        else:
                            self.domainMap14[domain] = value
                    elif (date=="2016-04-15"):
                        if domain in self.domainMap15.keys():
                            self.domainMap15[domain] += value
                        else:
                            self.domainMap15[domain] = value
                    elif (date=="2016-04-16"):
                        if domain in self.domainMap16.keys():
                            self.domainMap16[domain] += value
                        else:
                            self.domainMap16[domain] = value
                    elif (date=="2016-04-17"):
                        if domain in self.domainMap17.keys():
                            self.domainMap17[domain] += value
                        else:
                            self.domainMap17[domain] = value

    def __extractMapToList(self, dict):
        list1 = []
        list2 = []
        count = 0
        for key in self.domainSetAll:
            list1.append(str(count))
            if key in dict.keys():
                list2.append(dict[key])
            else:
                list2.append(0.0)
            count += 1
        return list1, list2

    def __convertStrToInt(self, strList):
        return map(lambda x: int(x), strList)

    def __plotGraph(self):
        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap09)
        plot(self.__convertStrToInt(xList), (yList), label="09")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_09.png'))
        close()

        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap10)
        plot(self.__convertStrToInt(xList), (yList), label="10")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_10.png'))
        close()

        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap11)
        plot(self.__convertStrToInt(xList), (yList), label="11")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_11.png'))
        close()

        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap12)
        plot(self.__convertStrToInt(xList), (yList), label="12")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_12.png'))
        close()

        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap13)
        plot(self.__convertStrToInt(xList), (yList), label="13")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_13.png'))
        close()

        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap14)
        plot(self.__convertStrToInt(xList), (yList), label="14")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_14.png'))
        close()

        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap15)
        plot(self.__convertStrToInt(xList), (yList), label="15")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_15.png'))
        close()

        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap16)
        plot(self.__convertStrToInt(xList), (yList), label="16")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_16.png'))
        close()

        xlabel("domain_index")
        ylabel("#imp or #clk")
        # ylim(0,200000)
        xList, yList = self.__extractMapToList(self.domainMap17)
        plot(self.__convertStrToInt(xList), (yList), label="17")
        legend()
        savefig(os.path.join("/home/dongjiahao/",'graph_17.png'))
        close()

    def runMe(self):
        self.__loadData()
        self.__plotGraph()

if __name__ == "__main__":
    inputFile = "/home/dongjiahao/tempResult3.txt"
    job = PlotDomainDistribution(inputFile)
    job.runMe()
