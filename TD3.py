# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 15:38:18 2020

@author: Matthieu
"""

import requests,json
import time

def currencies_list():
    currencies = requests.get('https://api.pro.coinbase.com/products') #on recupere l'API
    r_json = json.loads(currencies.text)
    array_currency = []
    for i in r_json:
        if i['base_currency'] not in array_currency: #on retire les doublons
            array_currency.append(i['base_currency'])
    print(array_currency)

#currencies_list()


def GetDepth(direction = "ask",pair = "BTC-USD"):
    if direction == "bid":
        price = requests.get("https://api.coinbase.com/v2/prices/"+pair+"/sell")
        r_json = json.loads(price.text)
        ammount = r_json['data']['amount']
    if direction == "ask":
        price = requests.get("https://api.coinbase.com/v2/prices/"+pair+"/buy")
        r_json = json.loads(price.text)
        ammount = r_json['data']['amount']
    print(ammount)
    
#GetDepth("ask","ETH-USD")

def OrderBook(pair = "BTC-USD"):
    data = requests.get("https://api-public.sandbox.pro.coinbase.com/products/"+pair+"/book?level=2")
    r_json = json.loads(data.text)
    bids_price = []
    bids_size = []
    print("BIDS")
    for i in r_json['bids']: #On commence par les bids  en stockant leur price et size
        bids_price.append(i[0])
        bids_size.append(i[1])
    for i in range(len(bids_price)):
        print("price: ",bids_price[i]," ","size: ",bids_size[i]) # on les affiche ensuite
    
    asks_price = []
    asks_size = []
    print("ASKS")
    for i in r_json['asks']: #On fait pareil pour les aks
        asks_price.append(i[0])
        asks_size.append(i[1])
    for i in range(len(asks_price)):
        print("price: ",asks_price[i]," ","size: ",asks_size[i])

#OrderBook("BTC-USD")

def candle(pair = "BTC-USD",duration = 300):


    data = requests.get("https://api-public.sandbox.pro.coinbase.com/products/"+pair+"/candles?granularity="+str(duration))
    r_json = json.loads(data.text)
    for i in r_json:
        print(i)
    
#candle("BTC-USD",300)



import sqlite3
import datetime

def sqlite_table(pair = "BTC-USD",duration = 300):
    conn = sqlite3.connect('candles.db',timeout = 10)
    c = conn.cursor()

    # Create table 
    setTableName = str("CoinBase" + "_" + pair[:3] +pair[4:]+ "_"+ str(duration))
    c.execute("""CREATE TABLE """ + setTableName + """ (Id INTEGER PRIMARY KEY , date INT, high REAL, low REAL, open REAL, close REAL, volume REAL, quotevolume REAL, weightedaverage REAL, sma_7 REAL, ema_7 REAL, sma_30 REAL, ema_30 REAL, sma_200 REAL, ema_200 REAL);""")
    conn.commit()
    start = datetime.datetime(2020,1, 25,9,1).isoformat()
    end = datetime.datetime.utcfromtimestamp(time.time()).isoformat()
    data = requests.get("https://api-public.sandbox.pro.coinbase.com/products/"+str(pair)+"/candles?start="+str(start)+"&end="+end+"&granularity="+str(duration))
    r_json = json.loads(data.text)
    print(r_json)
    primary_key = 0
    for i in r_json:
        request = str("INSERT INTO " +setTableName+ " VALUES(" +str(primary_key)+ "," +str(i[0])+ "," +str(i[2])+ "," +str(i[1])+ "," +str(i[3]) +"," +str(i[4])+ ","+ str(i[5])+ ",NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);")
        c.execute(request) #Fill table
        conn.commit()
        primary_key +=1
    for row in c.execute('SELECT * FROM '+ setTableName):
        print (row)
    conn.close()
   
#sqlite_table("BTC-USD",3600)



def update_database(pair = "BTC-USD",duration = 300):
    conn = sqlite3.connect('candles.db',timeout = 10)
    c = conn.cursor()
    setTableName = str("CoinBase" + "_" + pair[:3] +pair[4:]+ "_"+ str(duration))
    start = datetime.datetime(2020,1, 25,9,1).isoformat()
    end = datetime.datetime.utcfromtimestamp(time.time()).isoformat()
    data = requests.get("https://api-public.sandbox.pro.coinbase.com/products/"+str(pair)+"/candles?start="+str(start)+"&end="+str(end)+"&granularity="+str(duration))
    r_json = json.loads(data.text)
    time_list = []
    for row in c.execute('SELECT date FROM '+ setTableName): 
        time_list.append(row[0]) #store the times in the database
    primary_key = len(time_list)+2 # get where the primary key should start
    for i in r_json:
        if(i[0] not in time_list):
            request = str("INSERT INTO " +setTableName+ " VALUES(" +str(primary_key)+ "," +str(i[0])+ "," +str(i[2])+ "," +str(i[1])+ "," +str(i[3]) +"," +str(i[4])+ ","+ str(i[5])+ ",NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);")
            c.execute(request)
            conn.commit()
            primary_key +=1
            
#update_database("BTC-USD",86400)

def trades(pair="BTC-USD"):
    data = requests.get("https://api-public.sandbox.pro.coinbase.com/products/"+pair+"/trades?level=2")
    r_json = json.loads(data.text)
    for i in r_json:
        print(i)

#trades()

def store_trades(pair = "BTC-USD"):
    data = requests.get("https://api-public.sandbox.pro.coinbase.com/products/"+pair+"/trades?level=2")
    r_json = json.loads(data.text)
    conn = sqlite3.connect('trades.db',timeout = 10)
    c = conn.cursor()

    # Create table
    
    setTableName = str("CoinBase" + "_" + pair[:3] +pair[4:])
    c.execute("""CREATE TABLE """ +setTableName + """(Id INTEGER PRIMARY KEY, uuid TEXT, traded_btc REAL, price REAL, created_at_int INT, side TEXT)""")
    conn.commit()
    primary_key = 0
    for i in r_json:
        a = i['time']
        time_i = datetime.datetime(int(a[:4]),int(a[5:7]),int(a[8:10]), int(a[11:13]),int(a[14:16]),int(a[17:19]))
        date = int((time_i - datetime.datetime(1970, 1, 1)).total_seconds()) # convert the time to epoch
        side = "'"+i['side']+"'"
        request = str("INSERT INTO " +setTableName+ " VALUES(" +str(primary_key)+ "," +str(i['trade_id'])+ "," +str(i['size'])+ "," +str(i['price'])+ "," +str(date) +"," +str(side)+");")
        c.execute(request)
        conn.commit()
        primary_key +=1
    
#trades("BTC-USD")

def menu():
    print("1: currencies list")
    print("2: get depth")
    print("3: order book")
    print("4: candles list")
    print("5: create table with candles")
    print("6: update candle table")
    print("7: trades list")
    print("8: store trade data")
    choix = int(input("choix : "))
    if(choix ==1):
        currencies_list()
    elif choix ==2:
        direction = input("choix duration, au format ask ou bid : ")
        print("la pair doit etre saisie au format monnaie1-monnaie2. Si juste entrée est pressee ce sera BTC-USD")
        pair = input("choix pair : ")
        if(pair == ""): pair = "BTC-USD"
        GetDepth(direction,pair)
    elif choix == 3:
        print("la pair doit etre saisie au format monnaie1-monnaie2. Si juste entrée est pressee ce sera BTC-USD")
        pair = input("choix pair : ")
        if(pair == ""): pair = "BTC-USD"
        OrderBook(pair)
    elif  choix == 4:
        print("la pair doit etre saisie au format monnaie1-monnaie2. Si juste entrée est pressee ce sera BTC-USD")
        pair = input("choix pair : ")
        if(pair == ""): pair = "BTC-USD"
        duration = input("choix duration (300 secondes par defaut) : ")
        if(duration == "") : duration = 300
        else : duration = int(duration)
        candle(pair,duration)
    elif choix == 5:
        print("la pair doit etre saisie au format monnaie1-monnaie2. Si juste entrée est pressee ce sera BTC-USD")
        pair = input("choix pair : ")
        if(pair == ""): pair = "BTC-USD"
        duration = input("choix duration (300 secondes par defaut) : ")
        if(duration == "") : duration = 300
        else : duration = int(duration)
        sqlite_table(pair, duration)
    elif choix == 6:
        print("la pair doit etre saisie au format monnaie1-monnaie2. Si juste entrée est pressee ce sera BTC-USD")
        pair = input("choix pair : ")
        if(pair == ""): pair = "BTC-USD"
        duration = input("choix duration (300 secondes par defaut) : ")
        if(duration == "") : duration = 300
        else : duration = int(duration)
        update_database(pair, duration)
    elif choix == 7:
        print("la pair doit etre saisie au format monnaie1-monnaie2. Si juste entrée est pressee ce sera BTC-USD")
        pair = input("choix pair : ")
        if(pair == ""): pair = "BTC-USD"
        trades(pair)
    elif choix == 8:
        print("la pair doit etre saisie au format monnaie1-monnaie2. Si juste entrée est pressee ce sera BTC-USD")
        pair = input("choix pair : ")
        if(pair == ""): pair = "BTC-USD"
        store_trades(pair)
        
menu()