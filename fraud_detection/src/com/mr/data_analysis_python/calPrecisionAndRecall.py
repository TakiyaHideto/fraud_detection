__author__ = 'TakiyaHideto'

import sys

class CalPrecisionAndRecall:
    def __init__(self, inputTrueLabelFile, inputPredictionlFile):
        self.inputTrueLabelFile = inputTrueLabelFile
        self.inputPredictionlFile = inputPredictionlFile
        self.trueLabelList = []
        self.predictionList = []

    def __loadData(self):
        with open(self.inputTrueLabelFile, "r") as fileIn:
            for line in fileIn:
                label = int(line.rstrip())
                self.trueLabelList.append(label)
        with open(self.inputPredictionlFile, "r") as fileIn:
            for line in fileIn:
                label = float(line.rstrip())
                self.predictionList.append(label)

    def __calPrecision(self):
        trueCount = 0
        falseCount = 0
        for i in range(0,len(self.trueLabelList)):
            if float(self.predictionList[i]) > 0.5 and int(self.trueLabelList[i])==1:
                trueCount += 1
            if float(self.predictionList[i]) > 0.5 and int(self.trueLabelList[i])==0:
                falseCount += 1
        precision = float(trueCount)/(float(trueCount)+float(falseCount))
        print precision

    def __calRecal(self):
        trueCount = 0
        falseCount = 0
        for i in range(0,len(self.trueLabelList)):
            if float(self.predictionList[i]) > 0.5 and int(self.trueLabelList[i])==1:
                trueCount += 1
            if float(self.predictionList[i]) < 0.5 and int(self.trueLabelList[i])==1:
                falseCount += 1
        recal = float(trueCount)/(float(trueCount)+float(falseCount))
        print recal

    def __calAccuracy(self):
        trueCount = 0
        falseCount = 0
        for i in range(0, len(self.trueLabelList)):
            if (float(self.predictionList[i]) > 0.5 and int(self.trueLabelList[i])==1) or \
                    (float(self.predictionList[i]) < 0.5 and int(self.trueLabelList[i])==0):
                trueCount += 1
            else:
                falseCount += 1
        accuracy = float(trueCount)/(float(trueCount)+float(falseCount))
        print accuracy

    def runMe(self):
        self.__loadData()
        self.__calPrecision()
        self.__calRecal()
        self.__calAccuracy()

if __name__ == "__main__":
    inputTrueLabelFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/machine_learning/true_label"
    inputPredictionlFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/machine_learning/prediction"
    inputTrueLabelFile = sys.argv[1] + "/true_label"
    inputPredictionlFile = sys.argv[1] + "/pred.txt"
    job = CalPrecisionAndRecall(inputTrueLabelFile, inputPredictionlFile)
    job.runMe()
