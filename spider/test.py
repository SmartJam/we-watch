# -*- coding: utf-8 -*-
'''
Created on Apr 18, 2016

@author: Jam
'''

import os.path

if __name__ == '__main__':
    # test only!!
    print "running test."
    
    num1 = 12
    id1 = '%03d' % 1
    id2 = '%03d' % 123
    id3 = '%05d' % num1 
    print id1, id2, id3
    
    
    filepath = "F:/ACGs/yoedge/abcde12/cba/Photo20170104151502.jpg"
    targetDir = os.path.dirname(filepath)
    print targetDir
    if not os.path.exists(targetDir):
        os.makedirs(targetDir)
    
    # spider.change2Stop()
    
    
        