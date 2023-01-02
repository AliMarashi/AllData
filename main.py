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

def price_df_maker2(ls1,b) :
    ls2 = []
    for i in range(b) :
        x = ls1[i]
        result = json.loads(x.cont)
        df = pd.DataFrame(result)
        ls2.append(df)
    df = pd.concat(ls2 , axis = 1 , ignore_index = False)
    return df

##
stock_id = pd.read_excel(r'E:\RA_AliMarashi\pycharm\TSE-prices\stock_id_v2.xlsx')

stock_id = stock_id[stock_id["chek"]==1].reset_index(drop=True)
# stock_id = stock_id[stock_id["Ticker"].str[-1] != 'Í'].reset_index(drop=True)
##

WorkingDateStock = pd.read_excel('WorkingDateStock.xlsx')
##
df = pd.DataFrame()
dd = pd.DataFrame()
dj = pd.DataFrame()
dfm = pd.DataFrame()
##
df = pd.read_excel('PriceData.xlsx')
dd = pd.read_excel('InstrumentData.xlsx')
dfm = pd.read_excel('MistakeUrlPrice.xlsx')
##
part= 1000
lenght = WorkingDateStock.shape[0]//part

i = 0
for k in range(part):
    j = 0
    ls1 = []
    ls2 = []
    ls3 = []
    lsm1 = []

    print(k+1,'##############################################')
    for i in tqdm(range(lenght*k,lenght*(k+1))):
        for retry in range(150) :
            try:
                url = WorkingDateStock.iloc[i]['PriceUrl']
                result = GetClosingPriceDaily(url)
                result = pd.DataFrame(result)

                url0 = WorkingDateStock.iloc[i]['InstrumentUrl']
                result0 = GetClosingPriceDaily(url0)
                # result0['instrumentHistory']['Code'] = result0['instrumentHistory'].pop('insCode')
                del result0['instrumentHistory']['insCode']
                result0['instrumentHistory']['Date'] = WorkingDateStock.iloc[i]['recDate']
                result0['instrumentHistory']['Code'] = WorkingDateStock.iloc[i]['insCode']
                result0 = pd.DataFrame(result0)

                url1 = WorkingDateStock.iloc[i]['clientTypeUrl']
                result1 = GetClosingPriceDaily(url1)
                del result1['clientType']['insCode']
                del result1['clientType']['recDate']
                result1['clientType']['Date1'] = WorkingDateStock.iloc[i]['recDate']
                result1['clientType']['Code1'] = WorkingDateStock.iloc[i]['insCode']
                result1 = pd.DataFrame(result1)



                ls1.append(result)
                ls2.append(result0)
                ls3.append(result1)
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
                print(j , i, k)
                if j == 150 :
                    j = 0
                    print("er#######")
                    lsm1.append(i)
                    break
                continue


    df1 = pd.concat(ls1 , axis = 1 , ignore_index = False).T.reset_index(drop = True)
    df = pd.concat([df,df1], axis = 0 , ignore_index = False)

    dd1 = pd.concat(ls2 , axis = 1 , ignore_index = False).T.reset_index(drop = True)
    dd = pd.concat([dd,dd1], axis = 0 , ignore_index = False)

    dj1 = pd.concat(ls3 ,axis = 1 ,ignore_index = False).T.reset_index(drop = True)
    dj = pd.concat([dj , dj1] , axis = 0 , ignore_index = False)

    result = pd.concat([dj,dd,df], axis = 1 , ignore_index = False)

    dfm1 = pd.DataFrame({'col':lsm1})
    dfm = pd.concat([dfm,dfm1])


    df.to_parquet('PriceData.parquet', index=False)
    dd.to_parquet('InstrumentData.parquet', index=False)
    dj.to_parquet('ClientTypeData.parquet' , index = False)
    result.to_parquet('AllData.parquet', index=False)
    dfm.to_parquet('MistakeUrlPrice.parquet', index=False)

# df.to_excel('PriceData.xlsx', index=False)
# dd.to_excel('InstrumentData.xlsx', index=False)
# result.to_excel('AllData.xlsx', index=False)
# dfm.to_excel('MistakeUrlPrice.xlsx', index=False)
##
reault2.columns
result = result[['buy_I_Count', 'buy_I_Value', 'buy_I_Volume',
       'buy_N_Count', 'buy_N_Value', 'buy_N_Volume', 'sell_I_Count',
       'sell_I_Value', 'sell_I_Volume', 'sell_N_Count', 'sell_N_Value',
       'sell_N_Volume','insCode', 'dEven','lVal18AFC','zTitad','pClosing']]
result2 = result1.rename(columns = {
            "buy_I_Count" : "IndBuyCount" ,
            "buy_I_Value" : "IndBuyVal",
            "buy_I_Volume" : "IndBuyVol" ,
            "buy_N_Count" : "InsBuyCount",
            "buy_N_Value" : "InsBuyVal" ,
            "buy_N_Volume" : "InsBuyVol",
            "sell_I_Count" : "IndSellCount" ,
            "sell_I_Value" : "IndSellVal",
            "sell_I_Volume" : "IndSellVol" ,
            "sell_N_Count" : "InsSellCount",
            "sell_N_Value" : "InsSellVal" ,
            "sell_N_Volume" : "InsSellVol",
            "insCode" : "insCode" ,
            "dEven" : "JDate",
            "lVal18AFC" : "Ticker" ,
            "zTitad" : "shrout",
            "pClosing" : "close_price"
    })
##
result2 = result2[['JDate', 'Ticker', 'IndBuyVol', 'InsBuyVol', 'IndBuyVal', 'InsBuyVal',
       'InsBuyCount', 'IndBuyCount', 'IndSellVol', 'InsSellVol', 'IndSellVal',
       'InsSellVal', 'InsSellCount', 'IndSellCount','shrout', 'close_price','insCode']]

result2.to_parquet(r'E:\RA_AliMarashi\pycharm\IndIns\CleanedmaregedData_v3.parquet' , index = False)
