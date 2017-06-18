# -*- coding: utf-8 -*-
import ConfigParser
from util.common import singleton
from util.common import Logger

import MySQLdb
from threading import current_thread

@singleton
class DataSource(object):
    def __init__(self):
        cf = ConfigParser.ConfigParser()
        cf.read("../db_config.ini")
        # Logger.log("conf:%s" % cf.items("baseconf"))
        
        self._db_host = cf.get("baseconf", "host")
        self._db_port = cf.getint("baseconf", "port")
        self._db_user = cf.get("baseconf", "user")
        self._db_passwd = cf.get("baseconf", "password")
        self._db_dbname = cf.get("baseconf", "dbname")
        self._db_charset = 'utf8'
        self._db_conns = {}
    
    def __get_connetion(self):
        thread_name = current_thread().getName()
        
        conns = self._db_conns
        if thread_name in conns and conns[thread_name].open:
            return conns[thread_name]
        
        # 注意，每个线程都应该有独立的mysql连接，否则可能会报错
        # http://dev.mysql.com/doc/refman/5.7/en/gone-away.html
        Logger.log("create new sql connection for thread:{}".format(thread_name))
        conn =  MySQLdb.Connect(
            host = self._db_host,
            port = self._db_port,
            user = self._db_user,
            passwd = self._db_passwd,
            db = self._db_dbname,
            charset = self._db_charset
        )
        conn.autocommit(True)
        conn.ping(True)
        self._db_conns[thread_name] = conn
        return conn
    
    def execute(self, sql, args = None):
        '''
                        自动commit
                       返回: rowCount, rows
        '''
        cursor = self.__get_connetion().cursor()
        rowCount = cursor.execute(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rowCount, rows
    
    def inert_or_update_batch(self, sql, rows):
        '''
                        自动commit
                        返回: affected_count
        '''
        cursor = self.__get_connetion().cursor()
        affected_count = cursor.executemany(sql, rows)
        cursor.close()
        return affected_count
    