"""集成开发斗图表情包爬虫

Todo: 添加异常处理；开发图片爬虫；
用 argparse 处理命令行参数，参数用于不同 spider 的 parse 方法和 save 方法
"""
import requests
from bs4 import BeautifulSoup
import json
import random
import inspect
from collections import defaultdict
import pymongo
import base64
import os
from multiprocessing.dummy import Pool as ThreadPool
from urllib.parse import urlparse


class Spider:
    """爬虫基类，每个子类必须要有 name 属性，且必须唯一，用于表述爬虫名称；
    一个 parse 方法，用于启动爬虫爬取网页；
    一个 save 方法，保存爬取到的数据"""
    # User-Agent 列表
    USER_AGENT_LIST = [
        'Opera/9.20 (Macintosh; Intel Mac OS X; U; en)',
        'Opera/9.0 (Macintosh; PPC Mac OS X; U; en)',
        'iTunes/9.0.3 (Macintosh; U; Intel Mac OS X 10_6_2; en-ca)',
        'Mozilla/4.76 [en_jp] (X11; U; SunOS 5.8 sun4u)',
        'iTunes/4.2 (Macintosh; U; PPC Mac OS X 10.2)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:5.0) Gecko/20100101 Firefox/5.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:9.0) Gecko/20100101 Firefox/9.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:16.0) Gecko/20120813 Firefox/16.0',
        'Mozilla/4.77 [en] (X11; I; IRIX;64 6.5 IP30)',
        'Mozilla/4.8 [en] (X11; U; SunOS; 5.7 sun4u)'
    ]


class KeywordSpider(Spider):
    """爬取每套表情包的名字"""
    name = "keyword"

    def __init__(self):
        self.keywords = []

    def parse(self, pages=100):
        """
        params:
        - pages: 爬取的关键字页数
        """
        # 热门表情包链接
        urls = ['https://www.doutula.com/article/list/?page={}'.format(i) for i in range(1, pages+1)]
        for url in urls:
            # 随机选取一个 User-Agent
            response = requests.get(url, headers={"User-Agent": random.choice(self.USER_AGENT_LIST)})
            soup = BeautifulSoup(response.content, "lxml")
            # 提取 tag
            keyword_list = [tag.contents[0] for tag in soup.find_all("div", class_="random_title")]
            self.keywords.extend(keyword_list)
            yield keyword_list

    def save(self, fn="keywords.json"):
        with open(fn, 'w') as f:
            json.dump(self.keywords, f)


class ImgUrlSpider(Spider):
    """通过官方提供的API获取对应关键字的图片链接以及图片名称"""
    name = "img_url"

    def __init__(self):
        self.url = "https://www.doutula.com/api/search"
        # 读取关键字列表
        with open("keywords.json") as f:
            self.keyword_list = json.load(f)
        # 用于存储每个关键字下所有图片链接和图片名称的字典
        self.img_links = defaultdict(list)

    def parse(self, page=30):
        """
        params:
        - page: 每个关键字爬取的页数，最多为50页
        """
        for keyword in self.keyword_list:
            for i in range(page):
                # 参数含义：https://www.doutula.com/apidoc
                params={"keyword": keyword, "mime": 0, "page": i}
                response = requests.get(self.url, params=params, headers={"User-Agent": random.choice(self.USER_AGENT_LIST)})
                try:
                    result = response.json()
                except:
                    print('oh')
                    continue
                if result['status'] == 0 or result['data']['more'] == 0:
                    break

                pics = result['data']['list']
                self.img_links[keyword].extend(pics)

            yield keyword

    def save(self, fn="img_links.json"):
        with open(fn, 'w') as f:
            json.dump(dict(self.img_links), f)
        client = pymongo.MongoClient()
        db = client['doutu']
        img_links = dict(self.img_links)
        for name, data in img_links.items():
            coll = db[name]
            coll.insert_many(data)
        client.close()


class PictureSpider(Spider):
    name = "picture"

    def __init__(self):
        client = pymongo.MongoClient()
        self.db = client['doutu']  # 存储图片url
        self.img_db = client['doutu_image']  # 存储图片 base64 编码

    def parse(self):
        yield "Parsing"

    def save(self, fn="pictures/"):
        collections = self.db.collection_names()
        if "system.indexes" in collections:
            collections.remove("system.indexes")
        for col_name in collections:
            col = self.db[col_name]
            img_col = self.img_db[col_name]
            data = col.find()

            # 创建目录
            if not os.path.exists(os.path.join(fn, col_name)):
                os.mkdir(os.path.join(fn, col_name))
            else:
                continue

            def parse(d):
                """爬取图片的函数"""
                url = d['image_url']
                # 存为图片
                path = urlparse(url).path
                filename = path.split("/")[-1]
                # 判断是否是文件格式
                if not os.path.splitext(filename)[-1]:
                    return

                try:
                    response = requests.get(url, headers={"User-Agent": random.choice(self.USER_AGENT_LIST)})
                # 有非法url
                except requests.exceptions.MissingSchema:
                    response = requests.get("https:"+url, headers={"User-Agent": random.choice(self.USER_AGENT_LIST)})
                # 连接超时
                except requests.exceptions.ConnectionError:
                    time.sleep(60)
                    return None

                content = response.content
                # 将图片二进制转换成 base64 存储在 MongoDB 中
                content_b64 = base64.b64encode(content)
                img_col.update({"filename": filename}, {"$set": {"image_base64": content_b64}}, True)

                with open(os.path.join(fn, col_name, filename), 'wb') as f:
                    f.write(content)

            pool = ThreadPool(4)
            pool.map(parse, data)
            pool.close()
            pool.join()

            try:
                print(col_name)
            else:
                print(repr(col_name))


if __name__ == '__main__':
    import sys
    doc = """
    python3 doutu_spider.py <spider>

    spider:
    - keyword
    - picture
    - img_url
    """

    # 用于存储程序中所有的 Spider 类
    cls = {}
    p = globals().copy()
    for v in p.values():
        # 判断是否是一个类
        if not inspect.isclass(v):
            continue
        # 判断是否有 name 属性
        if hasattr(v, "name"):
            # 获取到爬虫名称并存放在 cls
            spider_name = getattr(v, "name")
            cls[spider_name] = v

    # 命令行需要传入爬虫名称，使用 Python3 
    if len(sys.argv) < 2 or sys.version_info[0] < 3:
        print(doc)
        sys.exit(-1)

    try:
        # 获取爬虫名称
        spider = cls[sys.argv[1]]
    except KeyError:
        print(doc)
        sys.exit(-1)

    # 实例化爬虫
    s = spider()
    # 爬取
    s_gener = s.parse()
    for l in s_gener:
        # 输出调试信息
        print(l)
    # 保存
    s.save()

