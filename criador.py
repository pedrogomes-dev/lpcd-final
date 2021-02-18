# -*- coding: utf-8 -*-
import sys
import psycopg2 as psy
from   psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

###############
#### SETUP ####
###############

host='localhost'
port=5432
user='postgres'
password='toor'
dbname='grupo_p'
schema='sc_covid'

###############

kwargs = dict()

def setup():
    kwargs['host'] = host
    kwargs['port'] = port
    kwargs['user'] = user
    kwargs['password'] = password
    return kwargs

def sqlParaString(path):
    f = open(path,'r')
    return f.read()

def conectar(**kwargs):
    host = kwargs.get('host')
    port = kwargs.get('port')
    dbname = kwargs.get('dbname')
    user = kwargs.get('user')
    password = kwargs.get('password')

    if dbname is not None:
        conn = psy.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    else:    
        conn = psy.connect(host=host, port=port, user=user, password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def abrirCursor(conn):
    return conn.cursor()

def executar(cur,cmd):
    cur.execute(cmd)
    records = cur.fetchall()
    for rec in records:
        print(rec)
    print(str(cur.rowcount) +' row(s) affected(s).')

def criarBanco(cur,dbname='grupo_p'):
    cur.execute("DROP DATABASE IF EXISTS "+dbname+";")
    cur.execute("CREATE DATABASE "+dbname+";")
    kwargs['dbname'] = dbname

def criarSchema(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS "+schema+";")
    cur.execute("SET search_path TO "+schema+";")
    executar(cur,"SELECT current_schema()")
    return

def reconectar(conn,cur,**kwargs):
    cur.close()
    conn.close()
    conn = conectar(**kwargs)
    cur = abrirCursor(conn)
    return conn,cur

def criarTabelas(cur):
    cria_tabela_municipio = sqlParaString('sql/criaTabela_municipio.sql')
    cria_tabela_report = sqlParaString('sql/criaTabela_report.sql')
    criar_tabela_about_report = sqlParaString('sql/criaTabela_about_report.sql')
    cur.execute(cria_tabela_municipio)
    cur.execute(cria_tabela_report) 
    cur.execute(criar_tabela_about_report)
    executar(cur,"select 'tabelas criadas'")

def preparacao():
    kwargs = setup()
    conn = conectar(**kwargs)
    cur = abrirCursor(conn)
    return kwargs, conn, cur

def definicao(conn, cur, kwargs):
    criarBanco(cur,dbname='grupo_p')
    conn, cur = reconectar(conn,cur,**kwargs)
    criarSchema(cur)
    criarTabelas(cur)
    return conn, cur

def encerramento(conn, cur):
    cur.close()
    conn.close()

def main():
    kwargs,conn,cur = preparacao()
    conn, cur = definicao(conn, cur, kwargs)
    encerramento(conn, cur)
    return

if __name__ == '__main__':
    main()