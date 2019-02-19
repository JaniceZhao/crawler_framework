import urllib.parse
import requests
import grequests
import re
import time

class Crawler(object):
    """docstring for Crawler"""
    def __init__(self):
        super(Crawler, self).__init__()
    
        with open('proxy_api.txt', 'r') as f:
            self.API_URL = f.read()
            print('We have read the API_URL as {}'.format(self.API_URL))

        self.proxy_host = None
        self.proxy_host_checker = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,6}")


    def try_crawl_url(self, url, **kwargs):

        for try_times in range(self.MAX_TRY_TIMES):
            try:
                self.update_proxy_()
                response = requests.get(url, proxies={"https": self.proxy_host}, **kwargs)
                if response and response.status_code == 200:
                    return response.text
            except Exception as e:
                print("Try {} times due to {}".format(try_times, e))

        print("Fail to crawl {} in {} try".format(url, try_times))



    def try_group_crawl_url(self, urls, **kwargs):


        for try_times in range(self.MAX_TRY_TIMES):
            try:
                self.update_proxy_()
                reqs = map(lambda x:grequests.get(x, proxies={"https": self.proxy_host}, **kwargs), urls)
                responses = grequests.map(reqs)
                good_responses = list(filter(lambda x: x and x.status_code == 200, responses))

                if(len(good_responses) >= len(responses) * self.GOOD_RATIO):
                    print("Fetched {}/{} valid pages".format(len(good_responses), len(responses)))
                    return list(map(lambda x: x.text, good_responses))

                print('Remap {} times'.format(try_times))

            except Exception as e:
                print("Try {} times due to {}".format(try_times, e))
        
        print("Fail to crawl in {} try".format(try_times))



    def update_proxy_(self):
        
        while True:

            proxy_host = urllib.request.urlopen(self.API_URL).read().decode("UTF8").strip("\n")

            if self.proxy_host_checker.match(proxy_host) is None:
                print("Get a invalid proxy {}", proxy_host)
            else:
                if not proxy_host == self.proxy_host:
                    self.proxy_host = proxy_host
                    return
            time.sleep(0.5)


    MAX_TRY_TIMES = 5
    REQUEST_HEADER = None
    GOOD_RATIO = 0.9
