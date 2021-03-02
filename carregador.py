# -*- coding: utf-8 -*-
import pandas as pd
import psycopg2 as psy
from   psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import conexao as c

######################
### DATASET SETUP ###
######################

path = 'database/covid19_casos_brasil.csv'
municipios = ['Presidente Prudente', 
    'Rondonópolis', 'Campo Novo do Parecis', 
    'Araraquara', 'Porto Velho']
######################

kwargs = dict()

def execute_many(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    print("Persistindo tabela: "+table)    
    print("Colunas: "+cols)
    query  = geraInsert(df) % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psy.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def mapearDataframe(df):
    municipioDF = df.filter(['city_ibge_code','city','state','estimated_population_2019'])
    municipioDF = municipioDF.drop_duplicates(subset=['city_ibge_code'], keep='first')

    reportDF = df.filter(['city_ibge_code','date','epidemiological_week','new_confirmed','new_deaths'])    
    reportDF = reportDF.rename(columns={"date": "report_date",})

    aboutReportDF = df.filter(['city_ibge_code','date','is_last','is_repeated','last_available_confirmed' ,'last_available_confirmed_per_100k_inhabitants' ,'last_available_date','last_available_death_rate','last_available_deaths','order_for_place'])
    aboutReportDF = aboutReportDF.rename(columns={"date": "report_date",})

    return municipioDF, reportDF, aboutReportDF

def geraInsert(df):
    insert = {}
    insert[1] = "INSERT INTO %s(%s) VALUES(%%s)"
    insert[2] = "INSERT INTO %s(%s) VALUES(%%s,%%s)"
    insert[3] = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)"
    insert[4] = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s)"
    insert[5] = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)"
    insert[6] = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s)"
    insert[7] = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s)"
    insert[8] = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"
    insert[9] = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"
    insert[10] = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"
    return insert.get(len(df.columns))   

def carregaDataSet(path=path):
    df = pd.read_csv(path,parse_dates=['date','last_available_date'])
    return df.dropna(axis=0)

def preparacao(df):
    kwargs = c.setup()
    conn = c.conectar(**kwargs)

    municipioDF, reportDF, aboutReportDF = mapearDataframe(df)
    return conn, municipioDF, reportDF, aboutReportDF

def persiste(conn,  municipioDF, reportDF, aboutReportDF):
    execute_many(conn,municipioDF,c.schema+'.municipio')
    execute_many(conn,reportDF,c.schema+'.report')
    execute_many(conn,aboutReportDF,c.schema+'.about_report')

def recupera(conn):
    consulta = c.sqlParaString('sql/selecaoParaDF.sql')
    df = pd.read_sql_query(consulta,conn)
    df = df.loc[df['city'].isin(municipios)]
    print(df.dtypes)
    print(df.shape)
    return df

def main():
    df = carregaDataSet()
    conn, municipioDF, reportDF, aboutReportDF = preparacao(df)
    #persiste(conn,  municipioDF, reportDF, aboutReportDF)
    df_filtrado = recupera(conn)
    return df_filtrado

if __name__ == '__main__':
    main()