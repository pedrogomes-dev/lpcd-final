# -*- coding: utf-8 -*-
import pandas as pd

def loadDatabase(path):
    return pd.read_csv(path)

def main():
    db_covid = loadDatabase('./database/covid19_casos_brasil.csv')
    print(db_covid)
    print()

if __name__ == '__main__':
    # execute only if run as the entry point into the program
    main()