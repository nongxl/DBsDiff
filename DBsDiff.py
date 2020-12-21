#!/usr/bin/python
#coding:utf-8
#pip install mysql-connector-python
import mysql.connector,time
#开发环境
config1 = {
             'user': 'dbadmin',
             'password': '123456',
             'host': '127.0.0.1',
             'port':3306,
             #'database': '',
             'charset': 'utf8',
             #'pool_size': 32,
             'pool_name': 'offlineserver1',
            'pool_reset_session': False,
            'connection_timeout': 120,
            'use_pure': True
}
#正式环境
config2 = {
             'user': 'dbadmin',
             'password': '123456',
             'host': '119.911.120.110',
             'port':3306,
             #'database': '',
             'charset': 'utf8',
             #'pool_size': 32,
             'pool_name': 'offlineserver2',
            'pool_reset_session': False,
            'connection_timeout': 120,
            'use_pure': True
}

DBname = '' #数据库名
showDBs = '''show databases;''' #列出所有库
#showTBs = '''select table_name from information_schema.tables where table_schema='\'%s\'';'''% DBname #l、列出所有表
testTBs = '''select table_name from information_schema.tables where table_schema='billsetcenter';'''
showViews = '''''' #列出所有视图
#showColumns = '''SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = \'%s\' AND TABLE_NAME = \'%s\';''' % (DBname,tbn)


def search(sql,config):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(dictionary=True)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    cnx.close()
    time.sleep(0.5)
    return result

db1 = search(showDBs,config1)
db2 = search(showDBs,config2)

#数据库1的所有库名
db1List = []
for i in range(0,len(db1)):
    dbn = db1[i]['Database']
    db1List.append(dbn)

#数据库2的所有库名
db2List = []
for i in range(0,len(db2)):
    dbn = db2[i]['Database']
    db2List.append(dbn)

#获得两个库的差集
dbsDiff = set(db1List).difference(set(db2List))
print('===============================数据库比对=======================================')
print('差集')
if dbsDiff:
    print(dbsDiff)
else:
    pass
print('======================================================================')
#获得两个库的交集
dbs = set(db1List).intersection(set(db2List))
print('交集')
if dbs:
    print(dbs)
else:
    pass
print('=======================================================================')
dbs = list(dbs)
for i in range(0,len(dbs)):
    DBname = dbs[i] #库名
    print('++++++++++++++++++++++++++++++当前库名:%s++++++++++++++++++++++++++++++' % DBname)
    showTBs = '''select table_name from information_schema.tables where table_schema=\'%s\';''' % DBname
    tbs1 = search(showTBs,config1) #查询测试库的表
    tbns1 = []
    #处理查询所有表的结果，处理成表名的列表
    for i in range(0,len(tbs1)):
        tbn = tbs1[i]['TABLE_NAME']
        tbns1.append(tbn) #测试库的表名列表

    tbs2 = search(showTBs,config2)
    tbns2 = []
    for i in range(0,len(tbs1)):
        tbn = tbs1[i]['TABLE_NAME']
        tbns2.append(tbn) #正式库的表名列表

    #表名的差集
    dbsDiff = set(tbns1).difference(set(tbns2))
    print('==============================库：%s差异的表:=========================' % DBname)
    if dbsDiff:
        print(dbsDiff)
    else:
        pass
    print('====================================================================')
    # 获得两个库表名的交集，只对比共有表的列
    setTables = list(set(tbns1).intersection(set(tbns2)))
    for j in range(0,len(setTables)):
        showColumns = '''SELECT COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = \'%s\' AND TABLE_NAME = \'%s\';''' % (DBname,setTables[j])
        #print(setTables[i])
        columns1 = search(showColumns,config1)
        colns1 = []
        for k in range(0,len(columns1)):
            coln = columns1[k]['COLUMN_NAME'] #字段名
            colc = columns1[k]['COLUMN_COMMENT'] #字段备注
            colt = columns1[k]['COLUMN_TYPE'] #字段类型
            col_info = str(coln) + str(colt).replace('b\'',' ').replace('\'','')
            #colns1.append(coln)
            colns1.append(col_info)
        columns2 = search(showColumns,config2)
        colns2 = []
        for m in range(0,len(columns2)):
            coln = columns2[m]['COLUMN_NAME']
            colc = columns2[m]['COLUMN_COMMENT']
            colt = columns2[m]['COLUMN_TYPE']
            col_info = str(coln) + str(colt).replace('b\'',' ').replace('\'','')
            #colns2.append(coln)
            colns2.append(col_info)
        setColns = list(set(colns1).difference(set(colns2)))
        if setColns:
            print('--------------------表：%s差异的列--------------------' % setTables[j])
            for m in range(0,len(setColns)):
                colAddr = DBname+'.'+setTables[j]+'.'+setColns[m]
                print(colAddr)
        else:
            pass

