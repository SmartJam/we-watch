# -*- coding: utf-8 -*-
'''
Created on Jan 17, 2017

@author: Jam
'''

import flask
from flask import Flask, send_from_directory, request
from flask.templating import render_template
from fileinput import filename
from db.datasource import DataSource
import time
from web import apis
from util.common import Logger

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/comic/<int:comicId>')
def show_comic(comicId):
    comicInfo, chapters = apis.get_comic_info(comicId)
    if comicInfo == None:
        return render_template('404.html'), 404
    
    return render_template('comic.html', comic = comicInfo, chapters = chapters)

@app.route('/chapter/<int:chapterId>')
def show_chapter(chapterId):
    comicId = int(request.args.get('from', 0))
    chapter = apis.get_chapter(chapterId, comicId)
    if chapter == None:
        return render_template('404.html')
    
    return render_template('chapter.html', chapter = chapter)


import ConfigParser
conf = ConfigParser.ConfigParser()
conf.read("../spider.ini")        
ImgRepoDir = conf.get("baseconf", "imgRepoDir")
@app.route('/imgs/<path:path>')
def send_img(path):
    return send_from_directory(ImgRepoDir, path)

@app.route('/favicon.ico')
def send_favicon():
    return send_from_directory("static", "favicon.ico")

@app.route('/api/comics', methods = ['GET'])
def get_comics_info():
    '''
    url params: page, size, mark=true|false
    return: {totalCount, pageNo, pageSize, comics:[{id,name,author,description,status,updatedAt}]}
    '''
    onlyMarked = request.args.get('marked') == "true"
    pageNo = int(request.args.get('page', 1))
    pageSize = int(request.args.get('size', 10))

    result = apis.get_comics_info(pageNo, pageSize, onlyMarked)
    return flask.jsonify(**result)

@app.route('/api/comic/<int:comicId>', methods = ['POST'] )
def mark_comic(comicId):
    result =  apis.mark_comic(comicId)
    print result
    return flask.jsonify(**result)

if __name__ == '__main__':
    # test only!!
    print "running test.112111"
    
    app.run("0.0.0.0", 8187, True)
    
    # spider.change2Stop()
    
    
        