__author__ = 'TakiyaHideto'

import sys
import random

class SampleFraudData:
    def __init__(self, input, output):
        self.input = input
        self.output = output
        self.sampleRatio = float(sys.argv[3])/float(sys.argv[4])

    def __sample(self):
        with open(self.output, "w") as fileOut:
            with open(self.input, "r") as fileIn:
                for line in fileIn:
                    if line.startswith("0"):
                        if random.random() < self.sampleRatio:
                            fileOut.write(line)
                    elif line.startswith("1"):
                        fileOut.write(line)

    def runMe(self):
        self.__sample()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print "<inputFile> <outputSampledFile> <fraudDataQuant> <normalDataQuant>"
        exit(1)
    job = SampleFraudData(sys.argv[1], sys.argv[2])
    job.runMe()