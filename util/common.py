# -*- coding: utf-8 -*-

import os
import sys
from threading import current_thread

import time

class TimeUtils(object):
    
    @staticmethod
    def getCurrentTimeStr():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# 此文件用于定于各种常用工具、方法
# 如单例、日志
class Logger(object):
    @staticmethod
    def log(log):
        caller = sys._getframe(1)
        filename = os.path.basename(caller.f_code.co_filename)
        line = caller.f_lineno
        func = caller.f_code.co_name
        print "[%s][%s] %s:%s in %s() - %s" % (TimeUtils.getCurrentTimeStr(), current_thread().getName(), filename, line, func, log)

def singleton(cls, *args, **kw):
    instances = {}
    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton

class StringUtils(object):
    @staticmethod
    def unicode(data):
        if data == None:
            return None
        
        return unicode(data)

if __name__ == '__main__':
    Logger.log('asfas')