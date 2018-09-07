#!/usr/bin/env python3
#-*- coding:utf-8 -*-

from google.cloud.bigquery import dbapi
from google.cloud import bigquery


from datetime import date,timedelta,datetime
import re
import sys
import pandas as pd
from pandas import DataFrame

import logging

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
logging.basicConfig(filename='my.log', level=logging.ERROR, format=LOG_FORMAT)
console = logging.StreamHandler(sys.stderr)
console.setLevel(logging.ERROR)
console.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger('').addHandler(console)

class db_firebase():
    def __init__(self,secret_json_path,project):
        self.secret_json_path = secret_json_path
        self.project = project

    def __client(self):
        client = bigquery.Client.from_service_account_json(self.secret_json_path)
        client.project = self.project
        return client

    def __firebase_connect(self):
        firebase_connection = dbapi.connect(self.__client())
        return firebase_connection

    def change_sql(self,sql,**kw):
        dict = {}
        for k,v in kw.items():
            dict['${}'.format(k)] = v
        for i in dict.keys():
            sql = re.sub('\{}'.format(i),dict[i],sql)

        return sql

    def firebase_execute(self,change_sql):
        rows = 0
        result = None
        conn = self.__firebase_connect()
        cur = conn.cursor()
        try:
            cur.execute(change_sql)
            rows = cur.rowcount
            result = cur.fetchall()
            conn.commit()
        except Exception as e:
            logging.error(e)
            logging.debug(change_sql)
            result = e
        conn.close()
 
        return rows,result

    def result_df(self,change_sql):
        try:
            df = pd.read_sql(sql=change_sql,con=self.__firebase_connect())
        except:
            logging.debug(change_sql)
            pass
            # logging.error(e)
        return df

    def __drop_table(self,tablename):
        dataset_id,table_id = tablename.split('.',2)
        table_ref = self.__client().dataset(dataset_id).table(table_id)
        self.__client().delete_table(table_ref)

    def __find_sql_for_fire(self,sql):
        sql_for_firebase_list = []
        sql_for_firebase_list_temp = re.findall('--#firebase(.*?)--firebase#',sql,re.S)
        # print(sql_for_firebase_list_temp)
        for sql in sql_for_firebase_list_temp:
            for i in sql.split(';'):
                if i.strip() == '':
                    pass
                else:
                    sql_for_firebase_list.append('{}'.format(i.strip()))
        
        return sql_for_firebase_list

    def multiple_sql_execute(self,sql,**kw):
        df_dict = {}
        sql_for_firebase_list = self.__find_sql_for_fire(sql)
        j = 0
        for i,sql in enumerate(sql_for_firebase_list):
            try:
                tablename = re.findall('create table (.*?) as',sql)[0]
                self.__drop_table(tablename)
            except:
                pass
            change_sql = self.change_sql(sql,**kw)
            rows,_ = self.firebase_execute(change_sql)
            try:
                df = self.result_df(change_sql)
                df_dict[j] = [rows,df] 
                j += 1
            except:
                pass
            
        return df_dict


if __name__ == '__main__':
    firebase = db_firebase(secret_json_path='../wordconnect_secret.json',project='word-view')
    with open('./sql.sql','r',encoding='utf-8') as f:
        sql = f.read()

    df_dict = firebase.multiple_sql_execute(sql)
    print(df_dict[0][1])
    print(df_dict[1][1])