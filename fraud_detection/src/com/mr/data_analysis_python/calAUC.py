__author__ = 'Hideto Dong'

import numpy as np
from sklearn.metrics import roc_auc_score

class CalAUC:
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

    def __calAuc(self):
        print roc_auc_score(self.trueLabelList, self.predictionList)

    def runMe(self):
        self.__loadData()
        self.__calAuc()

if __name__ == "__main__":
    inputTrueLabelFile = "/data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/machine_learning/true_label"
    inputPredictionlFile = "/data/dongjiahao/svnProject/fraudDetection/trunk/src/com/mr/machine_learning/prediction"
    job = CalAUC(inputTrueLabelFile, inputPredictionlFile)
    job.runMe()