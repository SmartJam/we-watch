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
from sets import Set

from bs4 import BeautifulSoup
from db.datasource import DataSource
from util.common import Logger
from _ast import alias

conf = ConfigParser.ConfigParser()
conf.read("../spider.ini")        
ImgRepoDir = conf.get("baseconf", "imgRepoDir")
ShouldDownloadCover = conf.get("baseconf", "downloadCover") in ['true', 'True'] # 是否下载封面
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
        self.allMovieIds = Set()
        
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
            if movieId in self.allMovieIds:
                #Logger.log("[MovieIdSpider] movie already added, id:{}".format(movieId))
                continue
            
            if self.isAlreadyCrawl(movieId):
                continue
            
            self.allMovieIds.add(movieId)
            self.taskMovieIds.put_nowait(movieId)
        
        return nextUrl
    
    def isAlreadyCrawl(self, movieId):
        querySql = "select * from movies_80s where id = %s"
        rowCount, _rows = DataSource().execute(querySql, [movieId])
        return rowCount > 0
    
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
                self.taskMovieIds.task_done()
                noTaskCount = 0
            except Exception as _e:
                # no task return
                noTaskCount += 1
                if noTaskCount > 100:
                    Logger.log("[MovieInfoSpider] no task got, thread ready to stop.")
                    break
                
                time.sleep(30)
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
        Logger.log("[MovieInfoSpider] try handle movie:{}".format(movieId))
        
        movieIndexUrl = "http://www.80s.tw/movie/" + str(movieId)
        
        session = self.ensureSession()
        resp = session.get(movieIndexUrl, headers = {'User-Agent':ConstUserAgent})
        if resp.status_code != 200:
            Logger.log("[MovieInfoSpider] get movie info failed, url:{}, resp:{}".format(movieIndexUrl, resp))
            return False
        
        movieData = self.parsePage(movieId, resp.text)
        movieData['movieId'] = movieId
        
        self.sync2DB(movieData)
        
        if ShouldDownloadCover == True and bool(movieData['coverUrl']):
            coverUrl = movieData['coverUrl']
            coverImgPath = ImgRepoDir + '/covers/80s/' + str(movieId) + '.jpg'
            downloadImage(self.ensureSession(), movieIndexUrl, coverUrl, coverImgPath)
            
        return True
    
    def parsePage(self, movieId, pageContent):
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
                'outline':"",
                'videoFormats':[]
                }
        '''
        soup = BeautifulSoup(pageContent, "lxml")
        
        coverUrl = "http:" + soup.find("img").get("src")
        
        infoView = soup.find("div", class_="info")
        name = infoView.find("h1").string.encode('utf-8')
        
        spanViews = infoView.find_all("span", class_="")
        aliases = None
        stars = []
        for spanView in spanViews:
            subSpan = spanView.find('span')
            if subSpan == None:
                continue
            
            spanTitle = subSpan.string.encode('utf-8')
            if '又名' in spanTitle:
                aliases = spanView.contents[-1].encode('utf-8').strip()
            elif '演员' in spanTitle:
                starLinks = spanView.find_all('a')
                for starLink in starLinks:
                    stars.append(starLink.string.encode('utf-8'))
                
        divViews = infoView.find_all("div", class_="clearfix")
        
        # div[0]: 类型 + 地区 + 语言 + 导演 + 上映时间 + 片长 + 更新时间
        genres = []
        region = ""
        languages = []
        director = ""
        showTime = None
        duration = None
        platformUpdateAt = None
        spanViews = divViews[0].find_all("span", class_="span_block")
        for spanView in spanViews:
            subSpan = spanView.find('span')
            if subSpan == None:
                continue
            
            spanTitle = subSpan.string.encode('utf-8')
            if '类型' in spanTitle:
                genreLinks = spanView.find_all("a")
                for genreLink in genreLinks:
                    genres.append(genreLink.string.encode('utf-8'))
            
            if '地区' in spanTitle:
                regionLink = spanView.find("a")
                if regionLink != None:
                    region = regionLink.string.encode('utf-8')
            
            if '语言' in spanTitle:
                languageLinks = spanViews[2].find_all("a")
                for languageLink in languageLinks:
                    languages.append(languageLink.string.encode('utf-8'))
                    
            if '导演' in spanTitle:
                directorLink = spanView.find("a")
                if directorLink != None:
                    director = directorLink.string.encode('utf-8')
        
            if '上映' in spanTitle:
                showTime = spanView.contents[-1].encode('utf-8').strip()
            
            if '片长' in spanTitle:
                duration = spanView.contents[-1].encode('utf-8').strip()
                    
            if '更新' in spanTitle:
                platformUpdateAt = spanView.contents[-1].encode('utf-8').strip()
        
        
        #div[1]: 豆瓣
        doubanScore = 0
        doubanCommentLink = ""
        spanViews = divViews[1].find_all("span", class_="span_block")
        for spanView in spanViews:
            spanText = spanView.text.encode('utf-8')
            if '豆瓣评分' in spanText:
                doubanScore = spanView.contents[-1].encode('utf-8').strip()
            
            if '豆瓣短评' in spanText:
                doubanCommentLink = spanView.find_all("a")[1].get('href')
                
        #div[2]: 电影简介
        outline = divViews[2].contents[2].encode('utf-8').strip()
        
        idFormatMapping = {'cpdl3':'hd', 'cpdl4':'bd', 'cpdl5':'bt'}
        videoFormats = []
        formatViews = soup.find_all('li', id=re.compile('^cpdl'))
        for formatView in formatViews:
            viewId = formatView.get('id')
            if viewId not in idFormatMapping:
                Logger.log("[MovieInfoSpider] mapping video format failed, movieId:{}, viewId:{}".format(movieId, viewId))
                continue
            else:
                videoFormats.append(idFormatMapping[viewId])
        
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
        ret['videoFormats'] = videoFormats
        
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
                srcTask = self.srcTasks.get(True, 1)
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
                movieId = srcTask['movieId']
                videoFormat = srcTask['format']
                if self.tryCrawlMovieInfo(movieId, videoFormat) == False:
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
    def tryCrawlMovieSrc(self, movieId, videoFormat):
        '''
        return {
            'videoSrcs' : [
                {
                    'title' : ""
                    'size' : ""
                    'src' : ""
                }
            ] 
        }
        '''
        
        movieSrcUrl = "http://www.80s.tw/movie/{}/{}-1".format(movieId, videoFormat)
                
        session = self.ensureSession()
        resp = session.get(movieSrcUrl, headers = {'User-Agent':ConstUserAgent})
        if resp.status_code != 200:
            Logger.log("[MovieSrcSpider] get movie src failed, url:{}, resp:{}".format(movieSrcUrl, resp))
            return False
        
        videoSrcs = self.parsePage(resp.text)
        self.sync2DB(movieId, videoFormat, videoSrcs)
        
        return True
    
    def parsePage(self, pageContent):
        '''
        return [{name, size, src}]
        '''
        soup = BeautifulSoup(pageContent, "lxml")
        
        videoSrcs = []
        
        spanViews = soup.find_all('span', class_="dlname")
        for spanView in spanViews:
            aLink = spanView.find('a')
            if aLink == None:
                continue
            
            name = aLink.text.strip().encode('utf-8')
            size = aLink.parent.contents[-1].strip().encode('utf-8')
            src = aLink.get('href').encode('utf-8')
            
            videoSrc = {}
            videoSrc['name'] = name
            videoSrc['size'] = size
            videoSrc['src'] = src
            videoSrcs.append(videoSrc)
        
        return videoSrcs
    
    def sync2DB(self, movieId, videoFormat, videoSrcs):
        insertVideoSrcSql = """
                replace into video_src_80s(
                        movieId, videoFormat, videoNo, title, size,
                        videoSrc, createdAt, updatedAt)
                values(%s,%s,%s,%s,%s, %s,now(), now())
        """
        rows = []
        videoNo = 0
        for videoSrc in videoSrcs:
            videoNo = videoNo + 1
            row = [movieId, videoFormat, videoNo, videoSrc['name'], videoSrc['size'], videoSrc['src']]
            rows.append(row)   
        try:
            self.datasource.inert_or_update_batch(insertVideoSrcSql, rows)
        except Exception as e:
            Logger.log("[sync2DB] fail, movieId:{}, format:{}, error:{}".format(movieId, videoFormat, e))

        
if __name__ == '__main__':
    # test only!!
    print "running test."
        
    taskIds = Queue.Queue(0)
#     idSpider = MovieIdSpider(taskIds)
#     idSpider.start()
    
#     num = 2
#     for _ in range(1, num + 1):
#         infoSpider = MovieInfoSpider(taskIds)
#         infoSpider.start()


    #infoSpider = MovieInfoSpider(taskIds)
    #infoSpider.tryCrawlMovieInfo(20847)
    #infoSpider.tryCrawlMovieInfo(20895)
    
    # 1198(showTime), 1004(duration), 17130(doubanScore), 5436(stars), 7610(redirect), 5419(conn failed)
    
    srcSpider = MovieSrcSpider(Queue.Queue(0))
    srcSpider.tryCrawlMovieSrc(20852, "bd")
    
    time.sleep(1)
    # spider.change2Stop()
    
    
        