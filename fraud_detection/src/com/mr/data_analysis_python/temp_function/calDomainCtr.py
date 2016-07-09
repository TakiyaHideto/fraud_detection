__author__ = 'TakiyaHideto'

inputFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp"
outputFile = "/data/dongjiahao/svn_project/fraudDetection/trunk/src/com/mr/temp/script/temp1"

domainImp = {}
domainClk = {}

with open(inputFile, "r") as fileIn:
    for line in fileIn:
        elements = line.rstrip().split("\t")
        domain = elements[0]
        imp = elements[3]
        clk = elements[4]
        if domain in domainImp.keys():
            domainImp[domain] += int(imp)
        else:
            domainImp[domain] = int(imp)
        if domain in domainClk.keys():
            domainClk[domain] += int(clk)
        else:
            domainClk[domain] = int(clk)

with open(outputFile,"w") as fileOut:
    for domain in domainImp.keys():
        if domain in domainClk.keys():
            fileOut.write("{0}\t{1}\t{2}\t{3}\n".format(domain, str(domainImp[domain]),
                                                        str(domainClk[domain]), float(domainClk[domain])/float(domainImp[domain])))
        else:
            fileOut.write("{0}\t{1}\t{2}\t{3}\n".format(domain, str(domainImp[domain]),
                                                        "0", "0.0"))