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
logging.basicConfig(filename='my.log', level=logging.WARNING, format=LOG_FORMAT)
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

    def __find_sql_for_fire(self,sql,sql_num=None):
        if sql_num == None:
            find_sql_for_firebase_list = re.findall('--#firebase(.*?)--firebase#',sql,re.S)
        elif isinstance(sql_num,int):
            find_sql_for_firebase_list = [re.findall('--#firebase(.*?)--firebase#',sql,re.S)[sql_num]]
        else:
            raise 'sql_num must be a int!'
        return find_sql_for_firebase_list
    
    def __execut_sql_for_fire(self,find_sql_for_firebase_list,k=None):
        # logging.warning(find_sql_for_firebase_list)
        if k == None:
            execut_sql_for_firebase_list = []
            for sql in find_sql_for_firebase_list:
                sql = [i for i in sql.split(';') if i.strip()]
                execut_sql_for_firebase_list.append(sql)
        elif isinstance(k,int) and len(find_sql_for_firebase_list) == 1:
            execut_sql_for_firebase_list = [[[i for i in find_sql_for_firebase_list[0].split(';') if i.strip()][k]]]

        elif not isinstance(k,int):
            raise 'k must be a int!'
        else:
            raise '请设定唯一sql，即sql_num参数必须单独传入一个数字'
        # logging.warning(execut_sql_for_firebase_list)
        return execut_sql_for_firebase_list

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
            logging.error(change_sql)
            result = e
        conn.close()
 
        return rows,result

    def result_df(self,change_sql):
        try:
            df = pd.read_sql(sql=change_sql,con=self.__firebase_connect())
        except Exception as e:
            df = None
            logging.error(e)
        return df

    def __drop_table(self,tablename):
        try:
            dataset_id,table_id = tablename.split('.',2)
            table_ref = self.__client().dataset(dataset_id).table(table_id)
            self.__client().delete_table(table_ref)
            logging.warning('{}已删除'.format(tablename))
        except Exception as e:
            logging.error(e)

    def firebase_execute_sqllist(self,sqllist,**kw):
        df = None
        for sql in sqllist:
            change_sql = self.change_sql(sql,**kw)
            try:
                tablename = re.findall('create table (.*?) as',sql)[0]
            except:
                tablename = None

            if tablename:
                self.__drop_table(tablename)
                self.firebase_execute(change_sql)
            else:
                df = self.result_df(change_sql)
        return df

    def multiple_sql_execute(self,sql,sql_num=None,k=None,**kw):
        df_dict = {}
        sql_for_firebase_list = self.__find_sql_for_fire(sql,sql_num=sql_num)
        execut_sql_for_firebase_list = self.__execut_sql_for_fire(sql_for_firebase_list,k=k)
        logging.error(execut_sql_for_firebase_list)
        j = 0
        for sqllist in execut_sql_for_firebase_list:
            df = self.firebase_execute_sqllist(sqllist)
            df_dict[j] = df
            j += 1
            
        return df_dict


if __name__ == '__main__':
    firebase = db_firebase(secret_json_path='../wordconnect_secret.json',project='word-view')
    with open('./sql.sql','r',encoding='utf-8') as f:
        sql = f.read()

    df_dict = firebase.multiple_sql_execute(sql,sql_num=0,k=None)
    print(df_dict[0])

