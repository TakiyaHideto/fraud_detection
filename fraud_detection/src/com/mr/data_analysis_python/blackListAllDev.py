__author__ = 'Hideto Dong'

class BlackListAllDev:

    def __init__(self, output_file):
        self.black_list_23 = "./blacklist_23.txt"
        self.black_list_24 = "./blacklist_24.txt"
        self.black_list_25 = "./blacklist_25.txt"
        self.black_list_26 = "./blacklist_26.txt"
        self.black_list_27 = "./blacklist_27.txt"
        self.black_list_28 = "./blacklist_28.txt"
        self.black_list_29 = "./blacklist_29.txt"
        self.black_list_dict = {}
        self.output_file = output_file

    def __loadBlackList(self):
        for i in range(23,30):
            with open("./blacklist_{}.txt".format(str(i)), "r") as file_in:
                for line in file_in:
                    element = line.rstrip().split("\t")
                    if element[0] in self.black_list_dict:
                        self.black_list_dict[element[0]] += int(element[1])
                    else:
                        self.black_list_dict[element[0]] = int(element[1])

    def __writeOutput(self):
        with open(self.output_file, "w") as file_out:
            for key in self.black_list_dict.keys():
                file_out.write("{}\t{}\n".format(key, self.black_list_dict[key]))

    def runMe(self):
        self.__loadBlackList()
        self.__writeOutput()


if __name__ == "__main__":
    output_file = "./blacklist.txt"
    job = BlackListAllDev(output_file)
    job.runMe()
