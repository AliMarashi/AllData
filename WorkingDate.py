import warnings

warnings.filterwarnings("ignore")
import re

import requests
import json
from threading import Thread
import pandas as pd
from tqdm import tqdm

# from bs4 import BeautifulSoup

from persiantools.jdatetime import JalaliDate
import time
import pandas as pd
import datetime

import urllib.request





def getclientType(id):
    url = "http://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{}".format(id)
    r = requests.get(url, timeout=15, headers = {
        'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        } )
    r = json.loads(r.text)
    result = r
    return result

def GetClosingPriceDaily(url):
    r = requests.get(url, timeout=0.2, headers = {
        'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        } )
    r = json.loads(r.text)
    result = r
    return result


def price_df_maker(ls1,a,b) :
    ls2 = []
    for j in range(a):
        l = ls1[j]
        for i in range(b) :
            x = l[i]
            result = json.loads(x.cont)
            df = pd.DataFrame(result)
            ls2.append(df)
    df = pd.concat(ls2 , axis = 1 , ignore_index = False)
    return df


##
stock_id = pd.read_excel(r'E:\RA_AliMarashi\pycharm\TSE-prices\stock_id_v2.xlsx')

stock_id = stock_id[stock_id["chek"]==1]
# stock_id = stock_id[stock_id["Ticker"].str[-1] != 'Í'].reset_index(drop=True)
##
j = 0
k = 0
ls1 = []
lsm1 = []
# stock_id["chek"] = 1
for i in tqdm(range(len(stock_id))):
    for retry in range(150) :
        try:
            id = stock_id.iloc[i,0]
            result = getclientType(id)
            # time.sleep(0.1)
            result = result["clientType"]
            result = pd.DataFrame(result)
            # if result.shape[1]<3:
            #     k = k + 1
            #     print(k)
            #     stock_id.iloc[i,2]=0
            #     j = 0
            result['Ticker'] = stock_id.iloc[i,1]
            ls1.append(result)
            j = 0
            break
        except :
        if j == 15 or j == 50 :
            time.sleep(0.25)
            pass
        if j == 75 or j == 100 :
            time.sleep(0.5)
            pass
        if j == 120 :
            time.sleep(0.75)
            pass
        j = j + 1
        print(j , i)
        if j == 150 :
            j = 0
            print("er#######")
            lsm1.append(id)
            break
        continue
# stock_id.to_excel(r'E:\RA_AliMarashi\pycharm\TSE-prices\stock_id_v2.xlsx')
df = pd.concat(ls1 , axis = 0 , ignore_index = False)
df1 =df
##
df = df[['recDate', 'insCode']]
df.recDate = df.recDate.astype('int')
df = df[df["recDate"] >= 20190325]
df['PriceUrl'] = "http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDaily/" + df['insCode'].astype(str) + "/" + df['recDate'].astype(str)
df['InstrumentUrl'] = "http://cdn.tsetmc.com/api/Instrument/GetInstrumentHistory/" + df['insCode'].astype(str) + "/" + df['recDate'].astype(str)
df['clientTypeUrl'] = "http://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/" + df['insCode'].astype(str) + "/" + df['recDate'].astype(str)


df.to_excel('WorkingDateStock.xlsx', index=False)
WorkingDateStock = df
WorkingDateStock.shape
##

WorkingDateStock = pd.read_excel('WorkingDateStock.xlsx')
##
# from mirutil.async_req import get_resps_async_sync
from mirutil.async_req import get_reqs_and_save_async_sync
b = 100
len(df)/b//1
list = []
for k in tqdm(range(1200,1202)):
    k1 = (k)*b
    k2 = (k+1)*b
    ls1 = get_resps_async_sync(WorkingDateStock.iloc[k1:k2]['PriceUrl'])
    list.append(ls1)
# df2 = pd.concat(list , axis = 0 , ignore_index = False)
df3 = price_df_maker(list,5,b)
##
