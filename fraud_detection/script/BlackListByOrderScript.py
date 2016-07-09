__author__ = 'Hideto Dong'

import os
import sys

class BlackListByOrderScript:

    def __init__(self, currentDate, period, bltype):
        self.blType = bltype
        self.rootDir = "/data/dongjiahao/svnProject/basedata/trunk/hive_result"
        self.orderOutputPath = "{}/{}/orderList".format(self.rootDir, self.blType)
        self.orderAdxList = []
        self.currentDate = currentDate
        self.period = period

    def __exeGenerateOrderList(self, logDate):
        command = "./generate_orderList_highCtr.sh {} {} {} {}".format(self.orderOutputPath, logDate, self.blType, self.rootDir)
        os.popen(command)

    def __readOrderHighCtr(self, orderHighCtrInputPath):
        # 0: campaign_id
        # 1: order_id
        # 2: adx_id
        # 3: count(*) as impression
        # 4: sum(clk) as click
        # 5: sum(clk)/count(*) as ctr
        counter = 0
        with open(orderHighCtrInputPath, "r") as file_in:
            for line in file_in:
                elements = line.rstrip().split("\t")
                if counter<5:
                    self.orderAdxList.append("{}_{}".format(elements[1],elements[2]))
                    counter += 1

    def __generateBlackListByOrder(self, logDate):
        for orderAdx in self.orderAdxList:
            order = orderAdx.split("_")[0]
            adx = orderAdx.split("_")[1]
            command = "./BlackList_ctr_click_impression.sh {} {} {} {} {}".format(order, adx, logDate, self.blType, self.rootDir)
            os.popen(command)

    def __cleanUp(self):
        self.orderAdxList = []

    def runMe(self):
        for i in range(int(self.currentDate)-int(self.period),int(self.currentDate)):
            if i<10:
                logDate = "2015-11-0{}".format(str(i))
            else:
                logDate = "2015-11-{}".format(str(i))
            path = "{}{}.txt".format(self.orderOutputPath,logDate)
            if not os.path.exists(path):
                self.__exeGenerateOrderList(logDate)
                self.__readOrderHighCtr(path)
                self.__generateBlackListByOrder(logDate)
                self.__cleanUp()

if __name__ == "__main__":
    currentDate = sys.argv[1].rstrip().split("-")[2]
    period = sys.argv[2].rstrip()
    bltype = sys.argv[3].rstrip()
    job = BlackListByOrderScript(currentDate, period, bltype)
    job.runMe()