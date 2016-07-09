__author__ = 'TakiyaHideto'

inputFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp"
outputFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp1"

dict = {}
dictCookie = {}

with open(inputFile, "r") as fileIn:
    for line in fileIn:
        element = line.rstrip().split("\t")
        for pair in element:
            try:
                key = pair.split(":")[0]
                value = pair.split(":")[1]
                if key in dict.keys():
                    dict[key] += int(value)
                else:
                    dict[key] = int(value)
                if key in dictCookie.keys():
                    dictCookie[key] += 1
                else:
                    dictCookie[key] = 1
            except IndexError:
                print element

with open(outputFile, "w") as fileOut:
    for key in dict.keys():
        fileOut.write("{0}\t{1}\t{2}\n".format(key, str(dict[key]), dictCookie[key]))