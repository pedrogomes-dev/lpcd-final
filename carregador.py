# -*- coding: utf-8 -*-
import pandas as pd
import psycopg2 as psy
from   psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

######################
### DATABASE SETUP ###
######################

host='localhost'
port=5432
user='postgres'
password='toor'

dbname='grupo_p'
schema='sc_covid'

######################
### DATASET SETUP ###
######################

path= 'database/covid19_casos_brasil.csv'
municipios = ['Presidente Prudente', 'Rondonópolis', 'Campo Novo do Parecis', 'Araraquara', 'Porto Velho']
######################

kwargs = dict()


def setup():
    kwargs['host'] = host
    kwargs['port'] = port
    kwargs['user'] = user
    kwargs['password'] = password
    return kwargs

def mapearDataframe(df):
    municipioDF = df.filter(['city_ibge_code','city','state','estimated_population_2019'])
    municipioDF = municipioDF[municipioDF.city.notnull()] #removendo estados

    reportDF = df.filter(['city_ibge_code','date','epidemiological_week','new_confirmed','new_deaths'])    
    reportDF = reportDF.rename(columns={"date": "report_date",})

    aboutReportDF = df.filter(['city_ibge_code','date','is_last','is_repeated','last_available_confirmed' ,'last_available_confirmed_per_100k_inhabitants' ,'last_available_date','last_available_death_rate','last_available_deaths','order_for_place'])
    aboutReportDF = aboutReportDF.rename(columns={"date": "report_date",})

    return municipioDF, reportDF, aboutReportDF

    
def carregaDataSet(path=path):
    df = pd.read_csv(path,parse_dates=['date','last_available_date'])
    return df

def conectar(**kwargs):
    host = kwargs.get('host')
    port = kwargs.get('port')
    dbname = kwargs.get('dbname')
    user = kwargs.get('user')
    password = kwargs.get('password')

    conn = psy.connect(host=host, port=port, dbname=dbname, user=user, password=password)
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

def preparacao(df):
    kwargs = setup()
    conn = conectar(**kwargs)
    cur = abrirCursor(conn)

    mapearDataframe(df)
    return kwargs, conn, cur

def main():
    df = carregaDataSet()
    kwargs,conn,cur = preparacao(df)
    executar(cur,"select 1")

    print(df.shape)
    return

if __name__ == '__main__':
    main()