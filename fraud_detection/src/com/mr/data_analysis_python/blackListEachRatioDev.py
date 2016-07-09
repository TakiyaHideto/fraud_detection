__author__ = 'Hideto Dong'

import os

class BlackListEachRatio:

    def __init__(self, input_dir_path, output_dir_path, original_file):
        self.imp_ip_list = []
        self.imp_ua_list = []
        self.imp_userarea_list = []
        self.ip_ua_list = []
        self.ip_userarea_list = []
        self.imp_ip_file_in = input_dir_path + "/Imp_IP.txt"
        self.imp_ua_file_in = input_dir_path + "/Imp_UA.txt"
        self.imp_userarea_file_in = input_dir_path + "/Imp_UserArea.txt"
        self.ip_ua_file_in = input_dir_path + "/IP_UA.txt"
        self.ip_userarea_file_in = input_dir_path + "/IP_UserArea.txt"

        self.imp_ip_file = output_dir_path + "/BL_Imp_IP.txt"
        self.imp_ua_file = output_dir_path + "/BL_Imp_UA.txt"
        self.imp_userarea_file = output_dir_path + "/BL_Imp_UserArea.txt"
        self.ip_ua_file = output_dir_path + "/BL_IP_UA.txt"
        self.ip_userarea_file = output_dir_path + "/BL_IP_UserArea.txt"

        self.original_file = original_file

    def __loadRatioInfo(self):
        with open(self.imp_ip_file_in, "r") as file_in:
            for line in file_in:
                ratio = line.rstrip().split("\t")
                if float(ratio[1]) < 10.0:
                    self.imp_ip_list.append(ratio[0])

        with open(self.imp_ua_file_in, "r") as file_in:
            for line in file_in:
                ratio = line.rstrip().split("\t")
                if float(ratio[1]) < 10.0:
                    self.imp_ua_list.append(ratio[0])

        with open(self.imp_userarea_file_in, "r") as file_in:
            for line in file_in:
                ratio = line.rstrip().split("\t")
                if float(ratio[1]) < 10.0:
                    self.imp_userarea_list.append(ratio[0])

        with open(self.ip_ua_file_in, "r") as file_in:
            for line in file_in:
                ratio = line.rstrip().split("\t")
                if float(ratio[1]) < 10.0:
                    self.ip_ua_list.append(ratio[0])

        with open(self.ip_userarea_file_in, "r") as file_in:
            for line in file_in:
                ratio = line.rstrip().split("\t")
                if float(ratio[1]) < 10.0:
                    self.ip_userarea_list.append(ratio[0])

    def __checkPotentialFraud(self):
        with open(self.original_file, "r") as file_in:
            with open(self.imp_ip_file, "w") as file_out:
                for line in file_in:
                    info_elements = line.rstrip().split("\t")
                    page_domain = info_elements[0]
                    if str(round(float(info_elements[1].split(":")[1]))) in self.imp_ip_list:
                        file_out.write("{}\n".format(page_domain))
        with open(self.original_file, "r") as file_in:
            with open(self.imp_ua_file, "w") as file_out:
                for line in file_in:
                    info_elements = line.rstrip().split("\t")
                    page_domain = info_elements[0]
                    if str(round(float(info_elements[2].split(":")[1]))) in self.imp_ua_list:
                        file_out.write("{}\n".format(page_domain))
        with open(self.original_file, "r") as file_in:
            with open(self.imp_userarea_file, "w") as file_out:
                for line in file_in:
                    info_elements = line.rstrip().split("\t")
                    page_domain = info_elements[0]
                    if str(round(float(info_elements[3].split(":")[1]))) in self.imp_userarea_list:
                        file_out.write("{}\n".format(page_domain))
        with open(self.original_file, "r") as file_in:
            with open(self.ip_ua_file, "w") as file_out:
                for line in file_in:
                    info_elements = line.rstrip().split("\t")
                    page_domain = info_elements[0]
                    if str(round(float(info_elements[4].split(":")[1]))) in self.ip_ua_list:
                        file_out.write("{}\n".format(page_domain))
        with open(self.original_file, "r") as file_in:
            with open(self.ip_userarea_file, "w") as file_out:
                for line in file_in:
                    info_elements = line.rstrip().split("\t")
                    page_domain = info_elements[0]
                    if str(round(float(info_elements[5].split(":")[1]))) in self.ip_userarea_list:
                        file_out.write("{}\n".format(page_domain))

    def runMe(self):
        self.__loadRatioInfo()
        self.__checkPotentialFraud()

if __name__ == "__main__":
    for i in range(3,10):
        input_dir_path = "./2" + str(i)
        output_dir_path = "./BL_2" + str(i)
        original_file = "/data/dongjiahao/svnProject/basedata/trunk/dataLog/ratio102{}.txt".format(str(i))
        if not os.path.isdir(output_dir_path):
            os.mkdir(output_dir_path)
        job = BlackListEachRatio(input_dir_path, output_dir_path, original_file)
        job.runMe()