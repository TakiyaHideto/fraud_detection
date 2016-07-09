__author__ = 'TakiyaHideto'

import sys

class CalBounceRate:
    def __init__(self,inputFile):
        self.inputFile = inputFile
        self.durationSum = 0
        self.bounceNum = 0
        self.reachNum = 0
        self.cookieNum = 0

    def __loadData(self):
        with open(self.inputFile,"r") as fileIn:
            for line in fileIn:
                bounce = False
                element = line.rstrip().split("\t")
                if (len(element)>100):
                    continue
                self.cookieNum += 1
                for ele in element:
                    if int(ele) > 3000:
                        continue
                    self.reachNum += 1
                    if ele != "0":
                        try:

                            self.durationSum += int(ele)
                            bounce = True
                        except ValueError:
                            print ele
                    if bounce:
                        self.bounceNum += 1

    def runMe(self):
        self.__loadData()
        print str(self.cookieNum) + \
              "\t" + str(self.durationSum) + \
              "\t" + str(self.bounceNum) + \
              "\t" + str(self.reachNum) + \
              "\t" + str(self.durationSum/self.reachNum) + \
              "\t" + str(self.durationSum/self.bounceNum) + \
              "\t" + str(float(self.bounceNum)/float(self.reachNum)) + \
              "\t" + str(float(self.durationSum)/float(self.cookieNum))

if __name__ == "__main__":

    print "normal"
    inputFile = "/data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/fraud_detection/script/normal"
    inputFile = sys.argv[2]
    job = CalBounceRate(inputFile)
    job.runMe()
    print "fraud"
    inputFile = "/data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/fraud_detection/script/fraud"
    inputFile = sys.argv[1]
    job = CalBounceRate(inputFile)
    job.runMe()


