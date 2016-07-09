__author__ = 'Hideto Dong'

import numpy as np
import os
from pylab import *

class ImpreRtbDistributionGraph:

    def __init__(self, data_file_path):
        self.data_file_path = data_file_path
        self.cost_number_dict_1 = {}
        self.cost_number_dict_2 = {}
        self.cost_number_dict_3 = {}
        self.cost_number_dict_4 = {}
        self.cost_number_dict_5 = {}
        self.cost_number_dict_6 = {}
        self.cost_number_dict_7 = {}
        self.sum_1 = 0
        self.sum_2 = 0
        self.sum_3 = 0
        self.sum_4 = 0
        self.sum_5 = 0
        self.sum_6 = 0
        self.sum_7 = 0
        pass

    def __loadDataFromFile(self):
        with open(self.data_file_path, "r") as file_in:
            for line in file_in:
                elements = line.rstrip().split("\t")
                if float(elements[1])/100000.0 <3.0 and float(elements[1])/100000.0 > 0.1:
                    if elements[0][0] == "1":
                        if elements[1] in self.cost_number_dict_1:
                            self.cost_number_dict_1[elements[1]] += 1
                        else:
                            self.cost_number_dict_1[elements[1]] = 1
                    elif elements[0][0] == "2":
                        print elements[0]
                        if elements[1] in self.cost_number_dict_2:
                            self.cost_number_dict_2[elements[1]] += 1
                        else:
                            self.cost_number_dict_2[elements[1]] = 1
                    elif elements[0][0] == "7":
                        if elements[1] in self.cost_number_dict_7:
                            self.cost_number_dict_7[elements[1]] += 1
                        else:
                            self.cost_number_dict_7[elements[1]] = 1

    def __sumUp(self):
        for key in self.cost_number_dict_1:
            self.sum_1 += int(self.cost_number_dict_1[key])
        for key in self.cost_number_dict_2:
            self.sum_2 += int(self.cost_number_dict_2[key])
        for key in self.cost_number_dict_7:
            self.sum_7 += int(self.cost_number_dict_7[key])

    def __convertStrToInt(self, strList):
        return map(lambda x: int(x), strList)

    def __convertToSortedDict(self, cost_number_dict):
        temp = {}
        for key in sorted(self.__convertStrToInt(cost_number_dict.keys())):
            temp[str(key)] = int(cost_number_dict[str(key)])
        return temp

    def __convertUnit(self, intList):
        return map(lambda x: float(x)/100000.0, intList)

    def __plotGraph(self):
        dict_1 = self.__convertToSortedDict(self.cost_number_dict_1)
        dict_2 = self.__convertToSortedDict(self.cost_number_dict_2)
        dict_7 = self.__convertToSortedDict(self.cost_number_dict_7)
        self.__sumUp()
        a_1 = (sorted(self.__convertStrToInt(dict_1.keys())))
        a_2 = (sorted(self.__convertStrToInt(dict_2.keys())))
        a_7 = (sorted(self.__convertStrToInt(dict_7.keys())))
        b_1 = []
        b_2 = []
        b_7 = []
        currentSum_1 = 0
        currentSum_2 = 0
        currentSum_7 = 0
        for key in a_1:
            currentSum_1 += dict_1[str(key)]
            b_1.append(float(currentSum_1)/float(self.sum_1))
        for key in a_2:
            currentSum_2 += dict_2[str(key)]
            b_2.append(float(currentSum_2)/float(self.sum_2))
        for key in a_7:
            currentSum_7 += dict_7[str(key)]
            b_7.append(float(currentSum_7)/float(self.sum_7))

        xlabel("number")
        ylabel("percentage")
        plot(self.__convertUnit(a_1),b_1, label="1")
        plot(self.__convertUnit(a_2),b_2, label="2")
        plot(self.__convertUnit(a_7),b_7, label="7")
        legend()
        savefig(os.path.join(os.getcwd(),'graph.png'))

    def runMe(self):
        self.__loadDataFromFile()
        self.__plotGraph()

if __name__ == "__main__":
    data_file_path = "/data/dongjiahao/svnProject/fraudDetection/dataLog/hive_data.txt"
    job = ImpreRtbDistributionGraph(data_file_path)
    job.runMe()




# import numpy as np
# import os
# from pylab import *
#
# # Create some test data
# dict1 = {1:1.22,2:1.26,3:8.9}
# a = dict1.keys()
# b = []
# for key in a:
#     b.append(dict1[key])
#
# # Plot both
#
# xlabel('number')
# ylabel('percentage(%)')
# plot(a,b)
#
# savefig(os.path.join(os.getcwd(),'pie.png'))
#
# show()
