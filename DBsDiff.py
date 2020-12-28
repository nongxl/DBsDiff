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
print('===============================================================================')
dbs = list(dbs)
for i in range(0,len(dbs)):
    DBname = dbs[i] #库名
    print('++++++++++++++++++++++++++++++当前库名:%s++++++++++++++++++++++++++++++' % DBname)
    showTBs = '''select table_name,table_type from information_schema.tables where table_schema=\'%s\';''' % DBname
    tbs1 = search(showTBs,config1) #查询测试库的表
    tbns1 = []
    vns1 = []
    #处理查询所有表的结果，处理成表名的列表
    for p in range(0,len(tbs1)):
        tbn = tbs1[p]['TABLE_NAME']
        t_type = tbs1[p]['TABLE_TYPE']
        if t_type == 'BASE TABLE':
            tbns1.append(tbn) #测试库的表名列表
        elif t_type == 'VIEW': #区分视图
            vns1.append(tbn)
        else:
            print(str(tbn) + '暂不支持的表类型或未知错误'+str(t_type))

    tbs2 = search(showTBs,config2)
    tbns2 = []
    vns2 = []
    for q in range(0,len(tbs2)):
        tbn = tbs2[q]['TABLE_NAME']
        t_type = tbs2[q]['TABLE_TYPE']
        if t_type == 'BASE TABLE':
            tbns2.append(tbn) #测试库的表名列表
        elif t_type == 'VIEW' or t_type == 'SYSTEM VIEW': #区分视图
            vns2.append(tbn)
        else:
            print(str(tbn) + '暂不支持的表类型或未知错误'+str(t_type))

    #表名的差集
    dbsDiff = set(tbns1).difference(set(tbns2))
    viewDiff = set(vns1).difference(set(vns2))
    if dbsDiff:
        print('==============================库：%s差异的表:=========================' % DBname)
        print(dbsDiff)
        print('====================================================================')
    elif viewDiff:
        print('==============================库：%s差异的视图:=========================' % DBname)
        print(viewDiff)
        print('====================================================================')
    else:
        pass
    # 获得两个库表名的交集，只对比共有表的列
    setTables = list(set(tbns1).intersection(set(tbns2)))
    setViews = list(set(vns1).intersection(set(vns2)))
    #setTables = setTables + setViews #表和视图一起比较差异的列.视图会出现 is not BASE TABLE的错误。

    #比对表中列的差异
    for j in range(0,len(setTables)):
        showColumns = '''SELECT COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = \'%s\' AND TABLE_NAME = \'%s\';''' % (DBname,setTables[j])
        #print(setTables[i])
        columns1 = search(showColumns,config1)
        colns1 = []
        for k in range(0,len(columns1)):
            coln = columns1[k]['COLUMN_NAME'] #字段名
            colc = columns1[k]['COLUMN_COMMENT'] #字段备注
            colt = columns1[k]['COLUMN_TYPE'] #字段类型
            col_info = str(coln) + str(colt).replace('b\'',' ').replace('\'',' ')
            colns1.append(coln) #不比对数据类型
            #colns1.append(col_info) #比对数据类型
        columns2 = search(showColumns,config2)
        colns2 = []
        colt = []
        for m in range(0,len(columns2)):
            coln = columns2[m]['COLUMN_NAME']
            colc = columns2[m]['COLUMN_COMMENT']
            colt = columns2[m]['COLUMN_TYPE']
            col_info = str(coln) + str(colt).replace('b\'',' ').replace('\'',' ')
            colns2.append(coln) #不比对数据类型
            #colns2.append(col_info) #比对数据类型
        setColns = list(set(colns1).difference(set(colns2))) #上面列属性经过编辑，此处比对可能有问题。需要确定检查。此处可直接对比列名和字段类型的差异
        if setColns:
            print('--------------------表：%s差异的列--------------------' % setTables[j])
            for n in range(0,len(setColns)):
                colAddr = 'alter table '+DBname+'.'+setTables[j]+' add '+setColns[n]+' '+str(colt).replace('b\'','').replace('\'',' ')+';'
                print(colAddr)
        else:
            pass
    #比对视图中列的差异。
    print('--------------------有差异的视图：--------------------')
    for j in range(0,len(setViews)):
        viewDefinition = '''SELECT view_definition FROM information_schema.views WHERE TABLE_NAME = \'%s\';''' % setViews[j]
        defSQL1 = search(viewDefinition,config1)
        defSQL2 = search(viewDefinition,config2)
        defSQL1 = str(defSQL1[0]['VIEW_DEFINITION'])
        defSQL2 = str(defSQL2[0]['VIEW_DEFINITION'])
        if defSQL1 != defSQL2:
            #print(defSQL1)
            print(DBname+'.'+setTables[j]+'.'+setViews[j]) #只输出视图名。输出视图设计语句太乱
            #print(defSQL2)
        else:
            pass