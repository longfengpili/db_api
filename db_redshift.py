#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import psycopg2
from datetime import date,timedelta,datetime
import re
import sys

import pandas as pd
from pandas import DataFrame

import logging

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
logging.basicConfig(filename='my.log', level=logging.WARNING, format=LOG_FORMAT)
console = logging.StreamHandler(sys.stderr)
console.setLevel(logging.ERROR)
console.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger('').addHandler(console)

class db_redshift():
    def __init__(self,database,user,password,host,port='5439'):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def __redshift_connect(self):
        redshift_connection = psycopg2.connect(database=self.database,user=self.user,password=self.password,host=self.host,port=self.port)
        return redshift_connection

    def __find_sql_for_red(self,sql):
        sql_for_redshift_list = re.findall('--#redshift(.*?)--redshift#',sql,re.S)
        return sql_for_redshift_list

    def change_sql(self,sql,**kw):
        # print(sql)
        dict = {}
        for k,v in kw.items():
            dict['${}'.format(k)] = v
        for i in dict.keys():
            sql = re.sub('\{}'.format(i),dict[i],sql)

        return sql

    def redshift_execute(self,change_sql):
        rows = 0
        result = None
        conn = self.__redshift_connect()
        cur = conn.cursor()
        try:
            cur.execute(change_sql)
            rows = cur.rowcount
            if rows == -1:
                result = None
            else:
                result = cur.fetchall()
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(e)
            logging.debug(change_sql)
            result = e
        conn.close()

        return rows,result

    def result_df(self,change_sql):
        df = DataFrame([])
        try:
            df = pd.read_sql(sql=change_sql,con=self.__redshift_connect())
        except Exception as e:
            logging.error(e)
            logging.debug(change_sql)

        return df
    
    def multiple_sql_execute(self,sql,k=None,**kw):
        sql_for_redshift_list = self.__find_sql_for_red(sql)
        df_dict = {}
        if k is None:
            k = []
        elif not isinstance(k,list):
            raise '[k] must be a list'            

        if len(k) != 0:
            for i,sql in enumerate(sql_for_redshift_list):
                for j in k:
                    if i == j - 1:
                        change_sql = self.change_sql(sql,**kw)
                        df = self.result_df(change_sql)
                        # rows,_ = self.redshift_execute(change_sql)
                        df_dict[i] = df
        else:
            for i,sql in enumerate(sql_for_redshift_list):
                change_sql = self.change_sql(sql,**kw)
                df = self.result_df(change_sql)
                # rows,_ = self.redshift_execute(change_sql)
                df_dict[i] = df
        return df_dict


if __name__ == '__main__':
    redshift = db_redshift(database='',user='',password='',host='',port='5439')
    with open('./sql.sql','r',encoding='utf-8') as f:
        sql = f.read()
    df = redshift.multiple_sql_execute(sql,k=[1],temp_user_info_t='temp_user_info')
    print(df[0])
    print(df[1])