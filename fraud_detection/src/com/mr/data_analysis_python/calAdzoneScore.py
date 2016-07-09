__author__ = 'TakiyaHideto'

# import os
# import sys
# import commands
#
# class CallAdzoneScore:
#     def __init__(self, inputFile, outputFile):
#         self.inputFile = inputFile
#         self.outputFile = outputFile
#         self.dateClkMean = float(sys.argv[1])
#         self.ipClkMean = float(sys.argv[2])
#         self.dataList = []
#
#     def __calZscore(self):
#         with open(inputFile,"r") as fileIn:
#             for line in fileIn:
#                 elements = line.rstrip().split(" ")
#                 adzone = elements[0]
#                 domain = elements[1]
#                 clickSum = int(elements[2])
#                 sdCampaignCtr = float(elements[3])
#                 dateClkMean = float(elements[4]) - self.dateClkMean
#                 sdClkDate = float(elements[5])
#                 sdDateCtr = float(elements[6])
#                 ipClkMean = float(elements[7]) - self.ipClkMean
#                 ipRatioCumulative = float(elements[8])
#                 self.dataList.append("0:{0} 1:{1} 2:{2} 3:{3} 4:{4} 5:{5} 6:{6} 7:{7} 8:{8} 9:{9}".
#                                      format())
#
#
#     def runMe(self):
#
#
# if __name__ == "__main__":
#     inputFile = ""
#     outputFile = ""
#     print sys.argv[1]
#     job = CallAdzoneScore(inputFile,outputFile)
#     job.runMe()