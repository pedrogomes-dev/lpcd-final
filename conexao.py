# -*- coding: utf-8 -*-
import sys
import psycopg2 as psy
from   psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

########################
#### DATABASE SETUP ####
########################

host='localhost'
port=5432
user='postgres'
password='toor'
dbname='grupo_p'
schema='sc_covid'

kwargs = dict()

def setup():
    kwargs['host'] = host
    kwargs['port'] = port
    kwargs['user'] = user
    kwargs['password'] = password
    kwargs['dbname'] = dbname
    return kwargs

def conectar(**kwargs):
    host = kwargs.get('host')
    port = kwargs.get('port')
    dbname = kwargs.get('dbname')
    user = kwargs.get('user')
    password = kwargs.get('password')

    try:
        conn = psy.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    except psy.OperationalError:
        print('Base não Existe! Connectando a Base default!')
        conn = psy.connect(host=host, port=port, user=user, password=password)
    finally:
        print('Setando Parâmetros de Configuração!')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def executar(cur,cmd):
    cur.execute(cmd)
    records = cur.fetchall()
    for rec in records:
        print(rec)
    print(str(cur.rowcount) +' row(s) affected(s).')

def sqlParaString(path):
    f = open(path,'r')
    return f.read()