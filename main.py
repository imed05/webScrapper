import argparse
import random
import threading
from time import sleep
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def retry(func, retries=10):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print(e)
                sleep(10)
                attempts += 1

    return retry_wrapper


class MongodbManager:
    def __init__(self):
        self.client = MongoClient("mongodb://10.13.53.131:27017")
        self.db = self.client['Scrapper']
        self.collect = self.db['Scrapper_data']
        self.collectLink = self.db['Scrapper_Link']
        self.collect.create_index("link", unique=True)
        self.collectSession = self.db['Session']

    def insert(self, session, link, content, title, header, emphasis):
        metadata = []
        for head in header:
            metadata.append({"key": head[0], "value": head[1]})
        for emphasi in emphasis:
            metadata.append({"key": emphasi[0], "value": emphasi[1]})
        document = {"sessionId": session, "link": link, "content": content, "title": title, "metadata": metadata}
        try:
            return self.collect.insert_one(document)
        except DuplicateKeyError as e:
            print(" duplicate ", link, str(e))

    def numberofdoc(self, idsession):
        return self.collectSession.find_one_and_update({"_id": idsession}, {"$inc": {"restParsedPage": -1}})

    def getpage(self, idsession):
        return self.collect.find_one({"sessionId": idsession})

    def getsession(self, link):
        return self.collectSession.find_one({"url": link})

    def getlink(self, id):
        return self.collectLink.find_one({"sessionId": id, "parsed": False})

    def insertlinks(self, links, id, idsession):
        linkss = []
        for link in links:
            linkss.append({"link": link[0], "value": link[1], "idPage": id, "sessionId": idsession, "parsed": False})
        if not linkss == []:
            self.collectLink.insert_many(linkss)

    def updateparsedlink(self, id):
        self.collectLink.update_one({"_id": id}, {"$set": {"parsed": True}})

    def insertsession(self, url):
        data = {'url': url, 'date': datetime.now(), 'restParsedPage': 10}
        return self.collectSession.insert_one(data)


class WebScrapper:
    def __init__(self, url, limit, Cookies=None):
        self.url = url
        self.limit = limit
        self.cookies = Cookies
        self.domain = self.setdomain()
        self.prefix = self.setprefix()

    def setprefix(self):
        link = str(self.url).split("/")
        return link[0] + "//" + link[2]

    def setdomain(self):
        link = str(self.url).split("/")
        link = link[2].split(".")
        if len(link) > 2:
            return link[-2] + "." + link[-1]
        else:
            return link[0] + "." + link[1]

    def extract_title(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.string
        else:
            return None

    def extract_headings(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        headings = []
        for level in range(1, 7):
            heading_tags = soup.find_all(f'h{level}')
            for heading_tag in heading_tags:
                headings.append(("h" + str(level), heading_tag.text.strip()))
        return headings

    @retry
    def gethtmlcontent(self):
        response = None
        if self.cookies is not None:
            response = requests.get(self.url, cookies=self.cookies)
        else:
            response = requests.get(self.url)

        self.cookies = response.cookies
        if response.status_code == 200:
            return response.content, response.text
        else:
            return None

        # scrapping emphasis

    def extract_emphasis(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        emphasis_tags = soup.find_all(['em', 'strong', 'i', 'b'])
        emphasis_data = []
        for tag in emphasis_tags:
            tag_name = tag.name
            tag_content = tag.get_text().strip()
            emphasis_data.append((tag_name, tag_content))
        return emphasis_data

    def scope(self, link):
        return self.domain in link

    def extract_links(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            valeur = link.text
            if href:
                if href.startswith("//"):
                    href = "https:" + href
                    if self.scope(href):
                        links.append((href, valeur))
                elif href.startswith('/'):
                    href = self.prefix + href
                    links.append((href, valeur))
                else:

                    if self.scope(href):
                        links.append((href, valeur))
        return list(set(links))


def exec():
    session = mongodb.getsession(args.url)
    while session is None:
        session = mongodb.getsession(args.url)

    while mongodb.numberofdoc(session.get("_id")).get("restParsedPage") > 1:
        page = mongodb.getpage(session.get("_id"))
        link = mongodb.getlink(session.get("_id"))
        ws = WebScrapper(link.get("link"), 10)
        content, text = ws.gethtmlcontent()
        if content is not None:
            links = ws.extract_links(content)
            result = mongodb.insert(session.get("_id"), link.get("link"), text, ws.extract_title(content),
                                    ws.extract_headings(content), ws.extract_emphasis(content))

            mongodb.insertlinks(links, result.inserted_id, session.get("_id"))
            mongodb.updateparsedlink(link.get("_id"))


parser = argparse.ArgumentParser()
parser.add_argument('url', help='url')
parser.add_argument('first', help='first to execute')
args = parser.parse_args()

if __name__ == '__main__':
    mongodb = MongodbManager()
    if args.first == 'True':
        ws = WebScrapper(args.url, 10)
        content, text = ws.gethtmlcontent()
        if content is not None:
            links = ws.extract_links(content)
            try:
                session = mongodb.insertsession(args.url)
                result = mongodb.insert(session.inserted_id, args.url, text, ws.extract_title(content),
                                        ws.extract_headings(content), ws.extract_emphasis(content))
                mongodb.insertlinks(links, result.inserted_id, session.inserted_id)
            except DuplicateKeyError as e:
                print(str(e))
            exec()
    else:
        exec()
