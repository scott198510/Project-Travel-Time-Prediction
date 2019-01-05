# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 15:40:55 2017

@author: Administrator
"""
import pandas as pd
import pymssql
import pandas.io.sql as sql
from os import getenv 
conn=pymssql.connect(host='localhost',user='sa',password='123456',database='chengdubus')
cur=conn.cursor()

ssql='select * from businfoo'
a=sql.read_sql_query('select * from businfoo',conn)

if not cur:
    raise Exception