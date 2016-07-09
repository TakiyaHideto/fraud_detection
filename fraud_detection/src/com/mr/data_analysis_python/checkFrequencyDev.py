__author__ = 'Hideto Dong'

class CheckFrequency:
    def __init__(self, file_path, output_path):
        self.file_path = file_path
        self.output_path = output_path
        self.host_ctr_dict = {}
        self.host_info_dict = {}

    def __loadFrequencyData(self):
        with open(self.file_path, "r") as file_in:
            for line in file_in:
                elements = line.rstrip().split("\t")
                if int(elements[1].split(":")[1]) > 500:
                    pageHost = elements[0]
                    ctr = float(elements[6].split(":")[1])
                    self.host_ctr_dict[pageHost] = ctr
                    # print elements[7]

                    if len(elements[7].split(":")[1]) > 0:
                        click_sequence = self.__sortClickTime(elements[7])
                        self.host_info_dict[pageHost] = elements[0] + "\t" + elements[1] + "\t" +\
                                                        elements[2] + "\t" + elements[3] + "\t" + \
                                                        elements[4] + "\t" + elements[5] + "\t" + \
                                                        elements[6] + "\t" + self.__collectClickTime(self.__convertListToString(self.__convertIntToStr(click_sequence))) + \
                                                        "\n"
                    else:
                        self.host_info_dict[pageHost] = line

    def __sortClickTime(self, click_sequence):
        click_time = click_sequence.split(":")[1].rstrip("-").split("-")
        click_time_int = self.__convertStrToInt(click_time)
        return sorted(click_time_int)

    def __convertStrToInt(self, strList):
        return map(lambda x: int(x), strList)

    def __convertIntToStr(self, intList):
        return map(lambda x: str(x), intList)

    def __convertListToString(self, clickList):
        click_string = ""
        return click_string.join(map(lambda  x: str(x)+"-", clickList))

    def __collectClickTime(self, clickList):
        temp_dict = {}
        clickTime = clickList.rstrip("").split("-")
        temp_string = ""
        for ele in clickTime:
            if ele in temp_dict:
                temp_dict[ele] += 1
            else:
                temp_dict[ele] = 1
        for key in temp_dict.keys():
            temp_string += key + ":" + str(temp_dict[key]) + " "
        return temp_string

    def __getDictSortedByValue(self, original_dict):
        return sorted(original_dict.iteritems(), key=lambda d:d[1], reverse=True)

    def __writeResult(self):
        with open(self.output_path, "w") as file_out:
            new_dict = self.__getDictSortedByValue(self.host_ctr_dict)
            for key in new_dict:
                file_out.write("{}".format(self.host_info_dict[key[0]]))

    def runMe(self):
        self.__loadFrequencyData()
        self.__writeResult()

if __name__ == "__main__":
    for i in range(25,33):
        data_file = "/data/dongjiahao/svnProject/basedata/trunk/dataLog/frequencyInfo{}.txt".format(str(i))
        output_file_path = "./freListBL{}.txt".format(str(i))
        job = CheckFrequency(data_file, output_file_path)
        job.runMe()