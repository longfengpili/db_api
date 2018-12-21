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
logging.basicConfig(filename='my.log', level=logging.INFO, format=LOG_FORMAT)
console = logging.StreamHandler(sys.stderr)
console.setLevel(logging.WARNING)
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
        try:
            redshift_connection = psycopg2.connect(database=self.database,user=self.user,password=self.password,host=self.host,port=self.port)
        except Exception as e:
            logging.error(e)
        return redshift_connection

    def __find_sql_for_red(self,sql,sql_zone=None):
        find_sql_for_redshift_list = []
        if sql_zone == None:
            find_sql_for_redshift_list = re.findall('--#redshift(.*?)--redshift#',sql,re.S)
        elif isinstance(sql_zone,int):
            find_sql_for_redshift_list = [re.findall('--#redshift(.*?)--redshift#',sql,re.S)[sql_zone]]
        else:
            raise 'sql_zone must be a int or none!'
        
        if len(find_sql_for_redshift_list) == 0:
            find_sql_for_redshift_list = [sql]

        return find_sql_for_redshift_list
    
    def __execut_sql_for_red(self,find_sql_for_redshift_list,sql_position=None):
        if sql_position == None:
            execut_sql_for_redshift_list = find_sql_for_redshift_list
        elif isinstance(sql_position,int):
            # print(find_sql_for_redshift_list)
            execut_sql_for_redshift_list = [[sql_split.strip() for sql in find_sql_for_redshift_list for sql_split in sql.split(';') if sql_split.strip()][sql_position]]
            # execut_sql_for_redshift_list = [[i for i in find_sql_for_redshift_list[0].split(';') if i.strip()][sql_position]]
        elif not isinstance(sql_position,int):
            raise 'sql_position must be a int or none!'
        else:
            raise 'error'
        return execut_sql_for_redshift_list

    def change_sql(self,sql,**kw):
        # print(sql)
        dict = {}
        for k,v in kw.items():
            dict['${}'.format(k)] = v
        for i in dict.keys():
            # logging.warning("\'{}\'".format(dict[i]))
            if 'str' in i:
                sql = re.sub('\{}(?!\w)'.format(i),"{}".format(dict[i]),sql)
            else:
                sql = re.sub('\{}(?!\w)'.format(i),"\'{}\'".format(dict[i]),sql)
        sql = re.sub('\$.*?,','null,',sql)
        sql = re.sub('\$.*?\)','null)',sql)
        sql = re.sub('\$.*? ','null ',sql)

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
            logging.warning(change_sql)

        return df
    
    def multiple_sql_execute(self,sql,sql_zone=None,sql_position=None,df=True,**kw):
        sql_for_redshift_list = self.__find_sql_for_red(sql,sql_zone=sql_zone)
        execut_sql_for_redshift_list = self.__execut_sql_for_red(sql_for_redshift_list,sql_position=sql_position)
        result_dict = {}

        for i,sql in enumerate(execut_sql_for_redshift_list):
            change_sql = self.change_sql(sql,**kw)
            logging.info(change_sql)
            if df:
                result = self.result_df(change_sql)
            else:
                _,result = self.redshift_execute(change_sql)
            result_dict[i] = result

        return result_dict

    def get_table_columns(self,tablename):
        sql = f'select * from {tablename} limit 1;'
        result = self.result_df(sql)
        columns = list(result.columns.values)
        columns = ','.join(columns)
        return columns


if __name__ == '__main__':
    redshift = db_redshift(database='',user='',password='',host='',port='5439')
    with open('./sql.sql','r',encoding='utf-8') as f:
        sql = f.read()
    result = redshift.multiple_sql_execute(sql,sql_zone=-1,sql_position=-1,temp_user_info_t='temp_user_info')
    print(result[0])
    print(result[1])
