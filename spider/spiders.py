# -*- coding: utf-8 -*-
'''
Created on Jun 10, 2017

@author: Jam
'''
import ConfigParser
import Queue
import json
import threading
import time
import requests
import re
import os.path

from bs4 import BeautifulSoup
from db.datasource import DataSource
from util.common import Logger

conf = ConfigParser.ConfigParser()
conf.read("../spider.ini")        
ImgRepoDir = conf.get("baseconf", "imgRepoDir")
ShouldDownloadCover = bool(conf.get("baseconf", "downloadCover")) # 是否下载封面
IntervalSecondPerCrawl = max(int(conf.get("baseconf", "intervalSecondPerCrawl")), 1)

ConstUserAgent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'

def downloadImage(connSession, refererUrl, imgUrl, toFilepath):
    '''
            下载图片并保存到指定路径。根据需要创建目标路径的父目录
    '''
    if os.path.exists(toFilepath):
        Logger.log("目标路径已使用， toFilepath:{}, imgUrl:{}".format(toFilepath, imgUrl))
        return
    
    fileDir = os.path.dirname(toFilepath)
    if not os.path.exists(fileDir):
        os.makedirs(fileDir)
                
    reqHeaders = {'Referer':refererUrl, 'User-Agent':ConstUserAgent}
    respImg = connSession.get(imgUrl, headers = reqHeaders)
        
    targetFile = open(toFilepath,'wb')
    targetFile.write(respImg.content)
    targetFile.close

class MovieIdSpider(threading.Thread):
    def __init__(self, taskQueue):
        threading.Thread.__init__(self)
        
        self.connSession = None
        self.taskMovieIds = taskQueue
        
    def ensureSession(self):
        # how to test the session is available or not??
        if self.connSession == None:
            self.connSession = requests.Session()
        
        return self.connSession
    
    def run(self):
        
        targetUrl = "http://www.80s.tw/movie/list"
        while (True) :            
            try:                
                targetUrl = self.tryCrawlMovieIds(targetUrl)
                if targetUrl == None:
                    break
            except Exception as e:
                Logger.log("[MovieIdSpider] failed, url:{}, error:{}".format(targetUrl, e))
                break
            
            time.sleep(IntervalSecondPerCrawl)
                            
    def tryCrawlMovieIds(self, url):
        '''
        return NextUrl | None
        '''
        Logger.log("[MovieIdSpider] tryCrawlMovieIds, url:{}".format(url))
                
        session = self.ensureSession()
        resp = session.get(url, headers = {'User-Agent':ConstUserAgent})
        if resp.status_code != 200:
            Logger.log("[MovieIdSpider] get movieIds failed, url:{}, resp:{}".format(url, resp))
            return False
        
        movieIds, nextUrl = self.parsePage(resp.text)
        #Logger.log("[MovieIdSpider] newIds:{}, nextUrl:{}".format(movieIds, nextUrl))
        for movieId in movieIds:
            self.taskMovieIds.put_nowait(movieId)
        
        return nextUrl
    
    def parsePage(self, pageContent):
        '''
          [movieId], nextUrl
        '''
        soup = BeautifulSoup(pageContent, "lxml")
        
        movieLists = soup.find("ul", class_="me1")
        movieLinks = movieLists.find_all('a')
        
        movieIds = []
        for movieLink in movieLinks:
            href = movieLink.get('href')
            movieIds.append(re.findall('/movie/(.*)', href)[0])
        
        nextUrl = None
        pageDiv = soup.find("div", class_ = "pager")
        pageLinks = pageDiv.find_all('a')
        last2ndLink = pageLinks[-2]
        if last2ndLink.text.encode('utf-8') == '下一页':
            nextUrl = "http://www.80s.tw" + last2ndLink.get('href')
        
        return movieIds, nextUrl
        
    # http://www.80s.tw/movie/list    

class MovieInfoSpider(threading.Thread):
    '''
            电影信息收集器
    '''
    def __init__(self, taskQueue):
        threading.Thread.__init__(self)
        
        self.taskMovieIds = taskQueue # 需要爬取的任务电影id队列
        self.connSession = None
        self.continuousFailedTimes = 0
        self.datasource = DataSource()
    
        
    def run(self):
        noTaskCount = 0
        while (True) :
            try:
                movieId = self.taskMovieIds.get(True, 1)
                noTaskCount = 0
            except Exception as _e:
                # no task return
                noTaskCount += 1
                if noTaskCount > 10:
                    Logger.log("[MovieInfoSpider] no task got, thread ready to stop.")
                    break
                
                time.sleep(3)
                continue
            
            try:
                if self.tryCrawlMovieInfo(movieId) == False:
                    self.continuousFailedTimes += 1
                    if self.continuousFailedTimes >= 100:
                        Logger.log("[MovieInfoSpider] stop crawling cause too much fail.")
                        break
                else:
                    self.continuousFailedTimes = 0
            except Exception as e:
                Logger.log("[MovieInfoSpider] failed, movieId:{}, error:{}".format(movieId, e))
            
            time.sleep(IntervalSecondPerCrawl)
            
    def ensureSession(self):
        # how to test the session is available or not??
        if self.connSession == None:
            self.connSession = requests.Session()
        
        return self.connSession
    
    def addTasks(self, movieIds):
        '''
        movieIds - array of movieId
        '''
        for movieId in movieIds:
            self.taskMovieIds.put_nowait(movieId)
            
    # http://www.80s.tw/movie/${movieId}    
    def tryCrawlMovieInfo(self, movieId):
        '''
        return true:success, false:failed
        '''
        #Logger.log("[MovieInfoSpider] try handle movie:{}".format(movieId))
        
        movieIndexUrl = "http://www.80s.tw/movie/" + str(movieId)
        
        session = self.ensureSession()
        resp = session.get(movieIndexUrl, headers = {'User-Agent':ConstUserAgent})
        if resp.status_code != 200:
            Logger.log("[MovieInfoSpider] get movie info failed, url:{}, resp:{}".format(movieIndexUrl, resp))
            return False
        
        movieData = self.parsePage(resp.text)
        movieData['movieId'] = movieId
        
        self.sync2DB(movieData)
        
        if ShouldDownloadCover == True and bool(movieData['coverUrl']):
            coverUrl = movieData['coverUrl']
            coverImgPath = ImgRepoDir + '/covers/80s/' + str(movieId) + '.jpg'
            downloadImage(self.ensureSession(), movieIndexUrl, coverUrl, coverImgPath)
            
        return True
    
    def parsePage(self, pageContent):
        '''
        return {
                'coverUrl':"",
                'name':"",
                'aliases':[],
                'stars':[],
                'genres':[],
                'region':"",
                'languages':[],
                'director':"",
                'showTime':"",
                'platformUpdatedAt':",
                'duration':"", 
                'doubanScore':""
                'doubanCommentLink':"",
                'outline':""
                }
        '''
        soup = BeautifulSoup(pageContent, "lxml")
        
        coverUrl = "http:" + soup.find("img").get("src")
        
        infoView = soup.find("div", class_="info")
        name = infoView.find("h1").string.encode('utf-8')
        
        spanViews = infoView.find_all("span", class_="")
        
        aliasesView = spanViews[1]
        aliases = aliasesView.contents[-1].encode('utf-8').strip()
        
        starLinks = spanViews[2].find_all('a')
        stars = []
        for starLink in starLinks:
            stars.append(starLink.string.encode('utf-8'))
        
        divViews = infoView.find_all("div", class_="clearfix")
        
        # div[0]: 类型 + 地区 + 语言 + 导演 + 上映时间 + 片长 + 更新时间
        spanViews = divViews[0].find_all("span", class_="span_block")
        genreLinks = spanViews[0].find_all("a")
        genres = []
        for genreLink in genreLinks:
            genres.append(genreLink.string.encode('utf-8'))
        
        region = spanViews[1].find("a").string.encode('utf-8')
        languageLinks = spanViews[2].find_all("a")
        languages = []
        for languageLink in languageLinks:
            languages.append(languageLink.string.encode('utf-8'))
        
        director = spanViews[3].find("a").string.encode('utf-8')
        showTime = spanViews[4].contents[-1].encode('utf-8').strip()
        duration = spanViews[5].contents[-1].encode('utf-8').strip()
        platformUpdateAt = spanViews[6].contents[-1].encode('utf-8').strip()
        
        #div[1]: 豆瓣
        spanViews = divViews[1].find_all("span", class_="span_block")
        doubanScore = spanViews[0].contents[-1].encode('utf-8').strip()
        doubanCommentLink = spanViews[1].find_all("a")[1].get('href')
        
        #div[2]: 电影简介
        outline = divViews[2].contents[2].encode('utf-8').strip()
        
        ret = {}
        ret['coverUrl'] = coverUrl
        ret['name'] = name
        ret['stars'] = stars
        ret['aliases'] = aliases
        ret['genres'] = genres
        ret['region'] = region
        ret['languages'] = languages
        ret['director'] = director
        ret['showTime'] = showTime
        ret['duration'] = duration
        ret['platformUpdateAt'] = platformUpdateAt
        ret['doubanScore'] = doubanScore
        ret['doubanCommentLink'] = doubanCommentLink
        ret['outline'] = outline
        
        #Logger.log("[MovieInfoSpider] try handle ret:{}".format(ret))
        #self.sync2DB(ret)
        return ret
    
    def sync2DB(self, movieData):        
        movieId = movieData['movieId']
        name = movieData['name']
        coverUrl = movieData['coverUrl']
        aliases = movieData['aliases']
        stars = "`".join(movieData['stars'])
        genres = "`".join(movieData['genres'])
        
        region = movieData['region']
        languages = "`".join(movieData['languages'])
        director = movieData['director']
        showTime = movieData['showTime']
        duration = movieData['duration']
        platformUpdateAt = movieData['platformUpdateAt']
        doubanScore = movieData['doubanScore']
        doubanCommentLink = movieData['doubanCommentLink']
        outline = movieData['outline']
         
        insertMovieSql = """
                insert into movies_80s(
                        id, name, aliases, stars, genres,
                        region, languages, director, showTime, duration,
                        platformUpdatedAt, doubanScore, doubanCommentLink, outline, createdAt,
                        updatedAt)
                values(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,now(), now())  
                on duplicate key update updatedAt = now()
        """
              
        try:
            self.datasource.execute(insertMovieSql, [movieId, name, aliases, stars, genres, 
                                                     region, languages, director, showTime, duration,
                                                     platformUpdateAt, doubanScore, doubanCommentLink, outline])
        except Exception as e:
            Logger.log("[sync2DB] fail, movieId:{}, error:{}".format(movieId, e))

class MovieSrcSpider(threading.Thread):
    '''
            电影片源收集器
    '''
    def __init__(self, srcTaskQueue):
        threading.Thread.__init__(self)
        
        self.srcTasks = srcTaskQueue # {movieId, format}
        self.connSession = None
        self.continuousFailedTimes = 0
        self.datasource = DataSource()
    
        
    def run(self):
        noTaskCount = 0
        while (True) :
            try:
                movieId = self.taskMovieIds.get(True, 1)
                noTaskCount = 0
            except Exception as _e:
                # no task return
                noTaskCount += 1
                if noTaskCount > 10:
                    Logger.log("[MovieSrcSpider] no task got, thread ready to stop.")
                    break
                
                time.sleep(3)
                continue
            
            try:
                if self.tryCrawlMovieInfo(movieId) == False:
                    self.continuousFailedTimes += 1
                    if self.continuousFailedTimes >= 100:
                        Logger.log("[MovieSrcSpider] stop crawling cause too much fail.")
                        break
                else:
                    self.continuousFailedTimes = 0
            except Exception as e:
                Logger.log("[MovieSrcSpider] failed, movieId:{}, error:{}".format(movieId, e))
            
            time.sleep(IntervalSecondPerCrawl)
            
    def ensureSession(self):
        # how to test the session is available or not??
        if self.connSession == None:
            self.connSession = requests.Session()
        
        return self.connSession
                
    # http://www.80s.tw/movie/${movieId}/${format}-1    
    def tryCrawlMovieSrc(self, movieId, format):
        '''
        return true:success, false:failed
        '''
        
        movieSrcUrl = "http://www.80s.tw/movie/{}/{}-1".format(movieId, format)
                
        session = self.ensureSession()
        resp = session.get(movieSrcUrl, headers = {'User-Agent':ConstUserAgent})
        if resp.status_code != 200:
            Logger.log("[MovieSrcSpider] get movie src failed, url:{}, resp:{}".format(movieSrcUrl, resp))
            return False
        
        movieData = self.parsePage(resp.text)
        movieData['movieId'] = movieId
        
        self.sync2DB(movieData)
        
        if ShouldDownloadCover == True and bool(movieData['coverUrl']):
            coverUrl = movieData['coverUrl']
            coverImgPath = ImgRepoDir + '/covers/80s/' + str(movieId) + '.jpg'
            downloadImage(self.ensureSession(), movieSrcUrl, coverUrl, coverImgPath)
            
        return True
    
    def parsePage(self, pageContent):
        '''
        return {
                'description':"",
                'videos':[{name, size, src}],
                }
        '''
        soup = BeautifulSoup(pageContent, "lxml")
        
        coverUrl = "http:" + soup.find("img").get("src")
        
        infoView = soup.find("div", class_="info")
        name = infoView.find("h1").string.encode('utf-8')
        
        spanViews = infoView.find_all("span", class_="")
        
        aliasesView = spanViews[1]
        aliases = aliasesView.contents[-1].encode('utf-8').strip()
        
        starLinks = spanViews[2].find_all('a')
        stars = []
        for starLink in starLinks:
            stars.append(starLink.string.encode('utf-8'))
        
        divViews = infoView.find_all("div", class_="clearfix")
        
        # div[0]: 类型 + 地区 + 语言 + 导演 + 上映时间 + 片长 + 更新时间
        spanViews = divViews[0].find_all("span", class_="span_block")
        genreLinks = spanViews[0].find_all("a")
        genres = []
        for genreLink in genreLinks:
            genres.append(genreLink.string.encode('utf-8'))
        
        region = spanViews[1].find("a").string.encode('utf-8')
        languageLinks = spanViews[2].find_all("a")
        languages = []
        for languageLink in languageLinks:
            languages.append(languageLink.string.encode('utf-8'))
        
        director = spanViews[3].find("a").string.encode('utf-8')
        showTime = spanViews[4].contents[-1].encode('utf-8').strip()
        duration = spanViews[5].contents[-1].encode('utf-8').strip()
        platformUpdateAt = spanViews[6].contents[-1].encode('utf-8').strip()
        
        #div[1]: 豆瓣
        spanViews = divViews[1].find_all("span", class_="span_block")
        doubanScore = spanViews[0].contents[-1].encode('utf-8').strip()
        doubanCommentLink = spanViews[1].find_all("a")[1].get('href')
        
        #div[2]: 电影简介
        outline = divViews[2].contents[2].encode('utf-8').strip()
        
        ret = {}
        ret['coverUrl'] = coverUrl
        ret['name'] = name
        ret['stars'] = stars
        ret['aliases'] = aliases
        ret['genres'] = genres
        ret['region'] = region
        ret['languages'] = languages
        ret['director'] = director
        ret['showTime'] = showTime
        ret['duration'] = duration
        ret['platformUpdateAt'] = platformUpdateAt
        ret['doubanScore'] = doubanScore
        ret['doubanCommentLink'] = doubanCommentLink
        ret['outline'] = outline
        
        return ret
    
    def sync2DB(self, movieData):        
        movieId = movieData['movieId']
        name = movieData['name']
        coverUrl = movieData['coverUrl']
        aliases = movieData['aliases']
        stars = "`".join(movieData['stars'])
        genres = "`".join(movieData['genres'])
        
        region = movieData['region']
        languages = "`".join(movieData['languages'])
        director = movieData['director']
        showTime = movieData['showTime']
        duration = movieData['duration']
        platformUpdateAt = movieData['platformUpdateAt']
        doubanScore = movieData['doubanScore']
        doubanCommentLink = movieData['doubanCommentLink']
        outline = movieData['outline']
         
        insertMovieSql = """
                insert into movies_80s(
                        id, name, aliases, stars, genres,
                        region, languages, director, showTime, duration,
                        platformUpdatedAt, doubanScore, doubanCommentLink, outline, createdAt,
                        updatedAt)
                values(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,now(), now())  
                on duplicate key update updatedAt = now()
        """
              
        try:
            self.datasource.execute(insertMovieSql, [movieId, name, aliases, stars, genres, 
                                                     region, languages, director, showTime, duration,
                                                     platformUpdateAt, doubanScore, doubanCommentLink, outline])
        except Exception as e:
            Logger.log("[sync2DB] fail, movieId:{}, error:{}".format(movieId, e))

        
if __name__ == '__main__':
    # test only!!
    print "running test."
        
    taskIds = Queue.Queue(0)
    idSpider = MovieIdSpider(taskIds)
    #idSpider.tryCrawlMovieIds("http://www.80s.tw/movie/list")
    idSpider.start()
    
    num = 2
    for _ in range(1, num + 1):
        infoSpider = MovieInfoSpider(taskIds)
        infoSpider.start()
#     spider.addTasksByRange(1003001, 1003100)  # end:1003016
    #spider.tryCrawlMovieInfo(20772)
    
    
    
    time.sleep(1)
    # spider.change2Stop()
    
    
        