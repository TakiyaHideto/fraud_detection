__author__ = 'TakiyaHideto'

input1 = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp1"
input2 = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp3"
output = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp5"

dict1 = {}
dict2 = {}

with open(input1, "r") as fileIn:
    for line in fileIn:
        element = line.rstrip().split("\t")[0]
        dict1[element] = line.rstrip()

with open(input2, "r") as fileIn:
    for line in fileIn:
        element = line.rstrip().split("\t")[0]
        dict2[element] = line.rstrip()

with open(output, "w") as fileOut:
    for key in dict1.keys():
        if key in dict2.keys():
            fileOut.write("{0}\t{1}\n".format(dict1[key],dict2[key]))
        else:
            try:
                fileOut.write("{0}\t{1}\t{2}\t{3}\n".format(dict1[key],key,"0","0"))
            except KeyError:
                print key