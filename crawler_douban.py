#coding=utf-8
from datetime import datetime
import itertools
import math
import json
from bs4 import BeautifulSoup


from impl.workspace import Website, WorkSpace
from impl.crawler import Crawler


class Douban_Moive(object):
    """docstring for Douban_Moive"""

    def __init__(self, workspace_path):
        super(Douban_Moive, self).__init__()

        self.workspace = WorkSpace(workspace_path)
        self.crawler = Crawler()


    def begin_crawl(self):
        
        for tag in self.TAGS:

            self.crawl_save_book_list(tag)
            self.workspace.save()

            for book in self.workspace.uncrawled_working_urls_all():
                self.crawl_save_comments_per_book(book)
                self.workspace.save()


    def crawl_save_book_list(self, tag):

        def get_book_list_URL(tag, start):
            return "https://api.douban.com/v2/book/search?tag={}&start={}&count={}". \
                format(tag, start, self.BOOK_INFO_NUM_PER_QUERY)

        for i in range(self.MAX_BOOK_LISTS_NUM):

            url = get_book_list_URL(tag, self.BOOK_INFO_NUM_PER_QUERY * i)

            if not self.workspace.has_crawled_this_meta_url(url):

                response = self.crawler.try_crawl_url(url)
                book_info = json.loads(response)["books"] if response else []
                book_urls = [Website(book['alt'], {'id':book['id'], 'title':book['title'], 'tag':tag}) for book in book_info]

                self.workspace.append_working_urls(book_urls)
                self.workspace.add_crawled_meta_url(url)

                print('crawled {} book url from {}th page in {}'.format(len(book_info), i, tag))



    def crawl_save_comments_per_book(self, website):

        book_name, tag = website.meta_data['title'], website.meta_data['tag']
        raw_comments = self.crawl_comments_per_book_(website.url)
        comments = list(map(lambda x : x.update({'book_name':book_name, 'tag':tag}), raw_comments))

        self.workspace.db.insert(comments)
        self.workspace.remove_working_url(website)
    
    

    def crawl_comments_per_book_(self, url):

        raw_comments = []

        begin_time = datetime.now()

        for urls in self.get_chunked_comments_urls_(url, total_page = self.get_num_of_page_(url)):
            raw_comments += self.crawl_one_trunk_comments_(urls)

        elapsed_time = (datetime.now() - begin_time).seconds

        print("We got {} comments in {} seconds ({:.2f} comments/s)"
            .format(len(raw_comments), elapsed_time, len(raw_comments) / (elapsed_time + 1e-5)))


    def crawl_one_trunk_comments_(self, urls):

        responses = self.crawler.try_group_crawl_url(urls)
        comments = list(itertools.chain.from_iterable(map(self.parse_comments_from_response_, responses)))
        return comments

        # print("Crawled {} comments".format(len(comments)))



    def append_page2comment_url_(self, url, i):
            return url + "comments/new?p=" + str(i)

    def get_chunked_comments_urls_(self, url, total_page):
            urls = list(map(lambda x: self.append_page2comment_url_(url, x), range(1, total_page+1)))
            for i in range(0, len(urls), self.CHUNK_SIZE):
                yield urls[i:i + self.CHUNK_SIZE]



    def get_num_of_page_(self, url):

        comments_num = 0

        response = self.crawler.try_crawl_url(self.append_page2comment_url_(url, 1))

        if not response == None:
            comments_html = BeautifulSoup(response, "html.parser").find("span", {"id": "total-comments"})
            if comments_html:
                comments_num = int(comments_html.text.split(' ')[1])


        return min(math.ceil(comments_num / self.COMMENTS_PER_PAGE), self.MAX_PAGE)


    def parse_comments_from_response_(self, response):

        def parse_comment_from_comments(comment_html):

            def get_star():
                try:
                    star_title = comment_html.find(
                        "span", class_="user-stars").get_attribute_list("title")[0]
                    return self.STAR_TITLE_TO_VALUE[star_title]
                except AttributeError:
                    return self.NO_STAR

            result = { "name": comment_html.find("span", class_="comment-info").text,
                       "star": get_star(), 
                       "comment": comment_html.find("p", class_="comment-content").text, 
                       "vote_count": comment_html.find("span", class_="vote-count").text
                     }
            return result


        comments = BeautifulSoup(response, "html.parser").find_all("div", class_="comment")

        return list(map(parse_comment_from_comments, comments))



    STAR_TITLE_TO_VALUE = {
        "力荐": 5,
        "推荐": 4,
        "还行": 3,
        "较差": 2,
        "很差": 1
    }

    NO_STAR = -1

    COMMENTS_PER_PAGE = 20
    BOOK_INFO_NUM_PER_QUERY = 100
    MAX_BOOK_LISTS_NUM = 5
    MAX_PAGE = 400
    CHUNK_SIZE = 10


    TAGS = ["小说", "外国文学", "文学", "随笔", "中国文学", "经典", "日本文学", "散文", "村上春树", 
            "诗歌", "童话", "儿童文学", "古典文学", "王小波", "名著", "杂文", "余华", "张爱玲", "当代文学", 
            "钱钟书", "外国名著", "鲁迅", "诗词", "茨威格", "米兰·昆德拉", "杜拉斯", "港台", "漫画", "推理", "绘本", 
            "青春", "东野圭吾", "科幻", "言情", "悬疑", "奇幻", "武侠", "日本漫画", "韩寒", "推理小说", "耽美", "亦舒", 
            "网络小说", "三毛", "安妮宝贝", "阿加莎·克里斯蒂", "郭敬明", "穿越", "金庸", "科幻小说", "轻小说", 
            "青春文学", "魔幻", "几米", "幾米", "张小娴", "J.K.罗琳", "古龙", "高木直子", "沧月", "校园", "落落", 
            "张悦然", "历史", "心理学", "哲学", "传记", "文化", "社会学", "艺术", "设计", "社会", "政治", "建筑", 
            "宗教", "电影", "政治学", "数学", "中国历史", "回忆录", "思想", "国学", "人物传记", "人文", "音乐", "艺术史", 
            "绘画", "戏剧", "西方哲学", "二战", "军事", "佛教", "近代史", "考古", "自由主义", "美术", "爱情", "旅行", 
            "成长", "生活", "心理", "励志", "女性", "摄影", "职场", "教育", "美食", "游记", "灵修", "健康", "情感", "两性", 
            "人际关系", "手工", "养生", "家居", "自助游", "经济学", "管理", "经济", "商业", "金融", "投资", "营销", "理财", 
            "创业", "广告", "股票", "企业史", "策划", "科普", "互联网", "编程", "科学", "交互设计", "用户体验", "算法", 
            "科技", "web", "UE", "交互", "通信", "UCD", "神经网络", "程序"]





def main():

    douban = Douban_Moive('temp.pickle')
    douban.begin_crawl()




if __name__ == "__main__":
    main()
