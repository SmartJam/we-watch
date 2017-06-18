# -*- coding: utf-8 -*-
'''
Created on Jan 22, 2017

@author: Jam
'''

import time
import ConfigParser
from db.datasource import DataSource
from util.common import Logger
from spider.spiders import ComicInfoSpider, ChapterDownloader
import os.path

def _buildSuccessResp():
    return {'success':True}

def _buildFailResp(failMsg = "unknown"):
    return {'success':False, 'message':failMsg}

def _changeComicStatus(comicId, toStatus):
    sql = "update comics set status = %s, updatedAt = now() where id = %s"
    updatedCount, _ = DataSource().execute(sql, [toStatus, comicId])
    if updatedCount != 1:
        Logger.log("mark comic failed, comicId:" + str(comicId))
        return _buildFailResp('record not found')
        
    return _buildSuccessResp()

def _toChapterUrl(chapterId, comicId):
    return "/chapter/" + str(chapterId) + "?from=" + str(comicId)

def _toCoverUrl(comicId):
    return "http://oji3qlphh.bkt.clouddn.com/covers/" + str(comicId) + ".jpg"

conf = ConfigParser.ConfigParser()
conf.read("../spider.ini")        
ImgRepoDir = conf.get("baseconf", "imgRepoDir")
def _toChapterImgUrl(chapterId, pageNo):
    basePath = '/chapters/' + str(chapterId) + ('/%03d' % pageNo) + ".jpg"
    
    if os.path.exists(ImgRepoDir + basePath):
        return "/imgs" + basePath
    else:
        return "http://oji3qlphh.bkt.clouddn.com/" + basePath

_comicInfoSpider = None
def _getComicSpider():
    if _comicInfoSpider == None:
        _comicInfoSpider = ComicInfoSpider()
        
    if _comicInfoSpider.isActive() == False:
        _comicInfoSpider.start()    
        
    return _comicInfoSpider
    
    
    
_chapterDownloader = None
    

def get_comics_info(pageNo = 1, pageSize = 10, onlyMarked = False):
    '''
    return: {totalCount, pageNo, pageSize, comics:[{id,name,author,description,coverUrl,status,updatedAt}]}
    '''
    offset = (pageNo - 1) * pageSize
    
    db = DataSource()
    countSql =  "select count(1) from comics"
    if onlyMarked:
        countSql += " where status = 'makred'"
    _, [[totalCount]] = db.execute(countSql)
    
    querySql = "select id,name,author,description,status, updatedAt from comics"
    if onlyMarked:
        querySql += " where status = 'marked'"
    querySql += " limit %s,%s"
    _rowCount, rows = db.execute(querySql, [offset, pageSize])
    
    comics = []
    for row in rows:
        comic = {}
        comic['id'] = row[0]
        comic['name'] = row[1]
        comic['author'] = row[2]
        comic['description'] = row[3]
        comic['coverUrl'] = _toCoverUrl(row[0])
        comic['status'] = row[4]
        comic['updatedAt'] = time.mktime(row[5].timetuple())
        
        comics.append(comic)
    
    result = _buildSuccessResp()
    result['totalCount'] = totalCount
    result['pageNo'] = pageNo
    result['pageSize'] = pageSize
    result['comics'] = comics
    
    return result

def get_comic_info(comicId):
    '''
    return None, None || comicInfo:{id,name,author,description,coverUrl,status,updatedAt}, chapters:[{id,name,status,addedAt,pageCount}]
    '''
    db = DataSource()
    queryComicSql = "select id,name,author,description,status, updatedAt from comics where id = %s"
    rowCount, rows = db.execute(queryComicSql, [comicId])
    if rowCount < 1:
        return None, None
    
    comicRecord = rows[0]
    comic = {}
    comic['id'] = comicRecord[0]
    comic['name'] = comicRecord[1]
    comic['author'] = comicRecord[2]
    comic['description'] = comicRecord[3]
    comic['coverUrl'] = _toCoverUrl(comicRecord[0])
    comic['status'] = comicRecord[4]
    comic['updatedAt'] = time.mktime(comicRecord[5].timetuple())
    
    queryChaptersSql = "select chapterId,chapterName,addedAt,status,pageCount from chapters where comicId = %s order by chapterOrder asc"
    rowCount, rows = db.execute(queryChaptersSql, [comicId])
    chapters = []
    for row in rows:
        chapter = {}
        chapter['id'] = row[0]
        chapter['name'] = row[1]
        chapter['addedAt'] = time.mktime(row[2].timetuple())
        chapter['status'] = row[3]
        chapter['pageCount'] = row[4]
        chapter['url'] = _toChapterUrl(chapter['id'], comicId) 
        chapters.append(chapter)
        
    return comic, chapters

def get_chapter(chapterId, comicId = 0):
    '''
    return None || {chapterName, status, imgUrls:[], nextChapterId}
    nextChapterId - 可能为None
    '''
    queryChapterSql = "select chapterId,comicId,chapterName,status,pageCount,chapterOrder from chapters where chapterId = %s"
    db = DataSource()
    rowCount, rows = db.execute(queryChapterSql, [chapterId])
    if rowCount < 1:
        return None
    
    targetRow = None
    for row in rows:
        if row[1] == comicId:
            targetRow = row
            break
    
    if targetRow == None:
        targetRow = rows[0]
    
    comicId = targetRow[1]
    chapterName = targetRow[2]
    status = targetRow[3]
    pageCount = targetRow[4]
    order = targetRow[5]
    
    imgUrls = []
    for pageNo in range(1, pageCount + 1):
            imgUrls.append(_toChapterImgUrl(chapterId, pageNo))
    
    nextChapterSql = "select chapterId from chapters where comicId = %s and chapterOrder = %s"
    rowCount, rows = db.execute(nextChapterSql, [comicId, order + 1])
    nextChapterUrl = None
    if rowCount > 0:
        nextChapterUrl = _toChapterUrl(rows[0][0], comicId)
    
    chapter = {}
    chapter['comicId'] = comicId
    chapter['name'] = chapterName
    chapter['status'] = status
    chapter['imgUrls'] = imgUrls
    chapter['nextChapterUrl'] = nextChapterUrl 
    
    return chapter

def mark_comic(comicId):
    return _changeComicStatus(comicId, 'marked')

def unmark_comic(comicId):
    return _changeComicStatus(comicId, 'new')

def block_comic(comicId):
    return _changeComicStatus(comicId, 'blocked')

def crawl_comic_info(comicId):
    spider = _getComicSpider()
    spider.addTasks([comicId])
    return
    

if __name__ == '__main__':
    # test only!!
    print "running test."
    
    print get_chapter(1001572)
    
    # spider.change2Stop()
    