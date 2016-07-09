__author__ = 'Hideto Dong'

class BlackListDayDev():

    def __init__(self, original_file, input_dir_path, output_file_path):
        self.original_file = original_file
        self.output_file_path = output_file_path
        self.imp_ip_file = input_dir_path + "/BL_Imp_IP.txt"
        self.imp_ua_file = input_dir_path + "/BL_Imp_UA.txt"
        self.imp_userarea_file = input_dir_path + "/BL_Imp_UserArea.txt"
        self.ip_ua_file = input_dir_path + "/BL_IP_UA.txt"
        self.ip_userarea_file = input_dir_path + "/BL_IP_UserArea.txt"
        self.imp_ip_list = []
        self.imp_ua_list = []
        self.imp_userarea_list = []
        self.ip_ua_list = []
        self.ip_userarea_list = []
        self.page_counter = 0

    def __loadPageDomainInfo(self):
        with open(self.imp_ip_file, "r") as file_in:
            for line in file_in:
                page_domain = line.rstrip()
                self.imp_ip_list.append(page_domain)
        with open(self.imp_ua_file, "r") as file_in:
            for line in file_in:
                page_domain = line.rstrip()
                self.imp_ua_list.append(page_domain)
        with open(self.imp_userarea_file, "r") as file_in:
            for line in file_in:
                page_domain = line.rstrip()
                self.imp_userarea_list.append(page_domain)
        with open(self.ip_ua_file, "r") as file_in:
            for line in file_in:
                page_domain = line.rstrip()
                self.ip_ua_list.append(page_domain)
        with open(self.ip_userarea_file, "r") as file_in:
            for line in file_in:
                page_domain = line.rstrip()
                self.ip_userarea_list.append(page_domain)

    def __comparePageDomain(self):
        with open(self.original_file, "r") as file_in:
            with open(self.output_file_path, "w") as file_out:
                for line in file_in:
                    page_domain = line.rstrip().split("\t")[0]
                    if page_domain in self.imp_ip_list:
                        self.page_counter += 1
                    if page_domain in self.imp_ua_list:
                        self.page_counter += 1
                    if page_domain in self.imp_userarea_list:
                        self.page_counter += 1
                    if page_domain in self.ip_ua_list:
                        self.page_counter += 1
                    if page_domain in self.ip_userarea_list:
                        self.page_counter += 1
                    if self.page_counter != 0:
                        file_out.write("{}\t{}\n".format(page_domain, self.page_counter))
                    self.page_counter = 0

    def runMe(self):
        self.__loadPageDomainInfo()
        self.__comparePageDomain()

if __name__ == "__main__":
    for i in range(3,10):
        original_file = "/data/dongjiahao/svnProject/basedata/trunk/dataLog/ratio102{}.txt".format(str(i))
        input_dir_path = "./BL_2{}".format(str(i))
        output_file_path = "./blacklist_2{}.txt".format(str(i))
        job = BlackListDayDev(original_file, input_dir_path, output_file_path)
        job.runMe()
