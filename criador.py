# -*- coding: utf-8 -*-
import sys
import psycopg2 as psy
from   psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 
import conexao as c

###############



def abrirCursor(conn):
    return conn.cursor()

def executar(cur,cmd):
    cur.execute(cmd)
    records = cur.fetchall()
    for rec in records:
        print(rec)
    print(str(cur.rowcount) +' row(s) affected(s).')

def criarBanco(cur):
    try:
        cur.execute("DROP DATABASE IF EXISTS "+c.dbname+";")
        cur.execute("CREATE DATABASE "+c.dbname+";")
    except psy.errors.ObjectInUse:
        print('Database em uso!')
    except psy.errors.DuplicateDatabase:
        print("Database "+c.dbname+" já existe!")

def criarSchema(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS "+c.schema+";")
    cur.execute("SET search_path TO "+c.schema+";")
    executar(cur,"SELECT current_schema()")
    return

def reconectar(conn,cur,**kwargs):
    cur.close()
    conn.close()
    conn = c.conectar(**kwargs)
    cur = abrirCursor(conn)
    return conn,cur

def criarTabelas(cur):
    cria_tabela_municipio = c.sqlParaString('sql/criaTabela_municipio.sql')
    cria_tabela_report = c.sqlParaString('sql/criaTabela_report.sql')
    criar_tabela_about_report = c.sqlParaString('sql/criaTabela_about_report.sql')
    cur.execute(cria_tabela_municipio)
    cur.execute(cria_tabela_report) 
    cur.execute(criar_tabela_about_report)
    executar(cur,"select 'tabelas criadas'")

def preparacao():
    kwargs = c.setup()
    conn = c.conectar(**kwargs)
    cur = abrirCursor(conn)
    return kwargs, conn, cur

def definicao(conn, cur, kwargs):
    criarBanco(cur)
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