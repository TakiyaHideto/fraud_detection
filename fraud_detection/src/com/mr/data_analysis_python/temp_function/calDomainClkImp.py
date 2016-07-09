__author__ = 'TakiyaHideto'

inputFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp2"
outputFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp3"

dictBid = {}
dictImp = {}
dictClk = {}

with open(inputFile, "r") as fileIn:
    for line in fileIn:
        element = line.rstrip().split("\t")
        try:
            domain = element[0]
            bid = element[2]
            imp = element[3]
            clk = element[4]
            if domain in dictBid.keys():
                dictBid[domain] += int(bid)
            else:
                dictBid[domain] = int(bid)
            if domain in dictImp.keys():
                dictImp[domain] += int(imp)
            else:
                dictImp[domain] = int(imp)
            if domain in dictClk.keys():
                dictClk[domain] += int(clk)
            else:
                dictClk[domain] = int(clk)
        except IndexError:
            print element

with open(outputFile, "w") as fileOut:
    for domain in dictImp.keys():
        if dictImp[domain] != 0:
            ctr = str(float(dictClk[domain])/float(dictImp[domain]))
            succRate = str(float(dictImp[domain])/float(dictBid[domain]))
            fileOut.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(domain,
                                    str(dictBid[domain]), str(dictImp[domain]),
                                    str(dictClk[domain]), ctr, succRate))