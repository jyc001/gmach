from datetime import datetime

import pandas as pd
import fuzzywuzzy.process as proc
from multiprocessing import Pool
from peewee import *
import time
import csv

db = SqliteDatabase('self.db')


class base(Model):
    code = CharField()
    Title = CharField()
    Alternate_Title = CharField()
    Short_Title = CharField()
    Source = CharField()

    class Meta:
        database = db


def func(year, employer, name, title, annual_wages, source, predictedgender, nameclean):
    if nameclean is None or nameclean == '':
        return year, employer, name, title, annual_wages, source, predictedgender, nameclean, '', 0, '', 0
    ar = base.select().where(SQL(f"Title like '%{nameclean}%'"))
    if ar.exists():
        ll = []
        for i in ar:
            ll.append(i.id)
        _res = proc.extractBests(nameclean, ll, score_cutoff=80, limit=1)
        if not _res:
            res1 = ""
            soc1 = 0
        else:
            res1 = _res[0][0]
            soc1 = _res[0][1]
    else:
        res1 = ""
        soc1 = 0
    ar = base.select().where(SQL(f"Alternate_Title like '%{nameclean}%'"))
    if ar.exists():
        l2 = []
        for i in ar:
            l2.append(i.id)
        _res = proc.extractBests(nameclean, l2, score_cutoff=80, limit=1)
        if not _res:
            res2 = ""
            soc2 = 0
        else:
            res2 = _res[0][0]
            soc2 = _res[0][1]
    else:
        res2 = ""
        soc2 = 0
    return year, employer, name, title, annual_wages, source, predictedgender, nameclean, res1, soc1, res2, soc2


if __name__ == '__main__':
    print("starting...")
    db.connect()
    # db.create_tables([base])

    # print(t_list)
    # print(ta_list)

    print("reading...")
    # df1 = pd.read_csv("./tst.csv")
    df1 = pd.read_csv("./New_York_17-19_Full.csv")
    df1 = df1.fillna('')
    print("zipping...")
    name_list = zip(df1['year'].to_list(), df1['employer'].to_list(), df1['name'].to_list(), df1['title'].to_list(),
                    df1['annual_wages'].to_list(), df1['source'].to_list(), df1['predictedgender'].to_list(),
                    df1['nameclean'].to_list())
    print("maching...")
    with Pool(15) as p:
        start = datetime.now()
        res = p.starmap_async(func, name_list)
        result = res.get()
        print('matching data cost %s s.' % str(datetime.now() - start))
        print(result)
    print("outputting...")
    with open("out.csv", "w+") as file:
        f_csv = csv.writer(file)
        f_csv.writerow(
            ["year", "employer", "name", "title", "annual_wages", "source", "predictedgender", "nameclean", "res1",
             "soc1", "res2", "soc2"])
        f_csv.writerows(result)
