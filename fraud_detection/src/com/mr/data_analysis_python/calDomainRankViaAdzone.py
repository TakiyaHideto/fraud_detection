
domainAdzoneNumDict = {}
domainAdzoneRankDict = {}

with open("/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/adzone_rank_2016-03-20", "r") as fileIn:
    for line in fileIn:
        elements = line.rstrip().split("\t")
        rank = elements[0]
        domain = elements[2]
        if domain in domainAdzoneRankDict.keys():
            domainAdzoneRankDict[domain] += int(rank)
        else:
            domainAdzoneRankDict[domain] = int(rank)
        if domain in domainAdzoneNumDict.keys():
            domainAdzoneNumDict[domain] += 1
        else:
            domainAdzoneNumDict[domain] = 1

with open("/data/dongjiahao/svn_project/fraudDetection/file/adzone_rank/adzone_rank_2016-03-20_domainRank", "w") as fileOut:
    for key in domainAdzoneRankDict.keys():
        fileOut.write("{0}\t{1}\n".format(key, str(float(domainAdzoneRankDict[key])/float(domainAdzoneNumDict[key]))))