# This is a sample Python script.
import argparse
import random
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
                sleep(60)
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
    def insert(self,session ,link,content ,title, header,emphasis):
        metadata =[]
        for head in header:
            metadata.append({"key":head[0], "value": head[1]})
        for emphasi in emphasis:
            metadata.append({"key": emphasi[0], "value": emphasi[1]})
        document = {"sessionId":session,"link": link, "content": content,"title": title, "metadata": metadata}
        try:
            return self.collect.insert_one(document)
        except DuplicateKeyError as e:
            print(" duplicate ", link ,str(e))

    def numbreOfDoc(self, idsession):
        return self.collectSession.find_one({"_id": idsession})

    def numbreOfDocAndUpdate(self, idsession):
        return self.collectSession.find_one_and_update({"_id": idsession}, {"$inc": {"restParsedPage": -1}})
    def getPage(self, idsession):
        return self.collect.find_one({"sessionId":idsession})
    def getSession(self, link):
        return self.collectSession.find_one({"url":link})
    def getLink(self, id):
        numdoc = self.collectLink.count_documents({"sessionId": id, "parsed":False})
        while numdoc == 0:
            numdoc = self.collectLink.count_documents({"sessionId": id, "parsed": False})
        RandomNumber = random.randint(1,numdoc)
        return self.collectLink.find({"sessionId": id, "parsed":False}).limit(-1).skip(RandomNumber).next()
    def insertLinks(self, links, id, idsession):
        linkss= []
        for link in links:
            linkss.append({"link": link[0],"value": link[1],"idPage":id,"sessionId":idsession ,"parsed": False})
        if not linkss == []:
            self.collectLink.insert_many(linkss)
    def UpdateParsedLink(self,id):
        self.collectLink.update_one({"_id":id},{"$set":{"parsed":True}})

    def insertSession(self,url):
        data = {'url':url,'date':datetime.now(),'restParsedPage': 9}
        return self.collectSession.insert_one(data)

class WebScrapper:
    def __init__(self, url, limit ,Cookies=None):
        self.url = url
        self.limit = limit
        self.cookies = Cookies
        self.domain = self.setDomain()
        self.prefix = self.setPrefix()
    def setPrefix(self):
        link =str( self.url).split("/")
        return link[0] + "//" + link[2]
    def setDomain(self):
        link =str( self.url).split("/")
        link = link[2].split(".")
        if len(link)>2:
            return link[-2]+"."+link[-1]
        else:
            return link[0]+"."+link[1]

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
                 headings.append(("h"+str(level),heading_tag.text.strip()))
        return headings
    @retry
    def getHtmlContent(self):
        response = None
        for i in range(0,10):
            if(self.cookies is not None) :
                response = requests.get(self.url, cookies=self.cookies)
            else:
                response = requests.get(self.url)
            self.cookies = response.cookies
            if response.status_code == 200:
                return (response.content, response.text)
            else:
                print(self.url)
                sleep(10)
        return None,None

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
    def Scope(self, link):
        return self.domain in link
    def extract_Links(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            valeur = link.text
            if href:
                if href.startswith("//"):
                    href = "https:" + href
                    if self.Scope(href):
                        links.append((href, valeur))
                elif href.startswith('/'):
                    href = self.prefix + href
                    links.append((href,valeur))
                else:

                    if self.Scope(href):
                        links.append((href, valeur))
        return list(set(links))
def run():
    session = mongodb.getSession(args.url)
    while session is None:
        session = mongodb.getSession(args.url)
    while mongodb.numbreOfDoc(session.get("_id")).get("restParsedPage") > 1:
        link = mongodb.getLink(session.get("_id"))
        ws = WebScrapper(link.get("link"), 10)
        content, text = ws.getHtmlContent()
        if content is not None:
            links = ws.extract_Links(content)
            if mongodb.numbreOfDocAndUpdate(session.get("_id")).get("restParsedPage") > 0:
                result = mongodb.insert(session.get("_id"),link.get("link"), text, ws.extract_title(content),
                                    ws.extract_headings(content), ws.extract_emphasis(content))
                mongodb.insertLinks(links, result.inserted_id,session.get("_id"))
        mongodb.UpdateParsedLink(link.get("_id"))
parser = argparse.ArgumentParser()
parser.add_argument('url', help='url')
parser.add_argument('first', help='first to execute')
args = parser.parse_args()
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    mongodb = MongodbManager()
    if args.first == 'True':
        ws = WebScrapper(args.url, 10)
        content,text = ws.getHtmlContent()
        if content is not None:
            links = ws.extract_Links(content)
            try:
                session = mongodb.insertSession(args.url)
                result = mongodb.insert(session.inserted_id, args.url, text,ws.extract_title(content),ws.extract_headings(content),ws.extract_emphasis(content))
                mongodb.insertLinks(links,result.inserted_id,session.inserted_id)
            except DuplicateKeyError as e:
                    print(str(e))
            run()
    else:
        run()



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
