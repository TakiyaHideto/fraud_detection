__author__ = 'Jiahao Dong'

from BlackList_adzoneId import BlackListAdzoneId
from BlackList_ip import BlackListIp
from BlackList_yoyiCookie import BlackListYoyiCookie

class BlackListEngine:
    def __init__(self):
        self.jobAdzoneIdInputFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/adzoneIdInput"
        self.jobAdzoneIdOutputFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/adzoneIdBL"
        self.jobIpInputFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/ipInput"
        self.jobIpOutPutFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/ipBL"
        self.jobYoyiCookieInputFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/yoyiCookieInput"
        self.jobYoyiCookieOutputFile = "/data/dongjiahao/svnProject/fraudDetection/hive_result/yoyiCookieBL"

    def runMe(self):
        jobYoyiCookie = BlackListYoyiCookie(self.jobYoyiCookieInputFile, self.jobYoyiCookieOutputFile)
        jobAdzoneId = BlackListAdzoneId(self.jobAdzoneIdInputFile, self.jobAdzoneIdOutputFile)
        jobIp = BlackListIp(self.jobIpInputFile, self.jobIpOutPutFile)
        jobAdzoneId.runMe()
        jobIp.runMe()
        jobYoyiCookie.runMe()

if __name__ == "__main__":
    job = BlackListEngine()
    job.runMe()