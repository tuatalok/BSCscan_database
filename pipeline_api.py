import requests
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
from sqlalchemy import create_engine
import time

#Establishing the connection
conn = psycopg2.connect(
                        database="postgres", 
                        user='postgres', 
                        password='', 
                        host='', 
                        port= ''
                        )  


def create_table():
   #Creating a cursor object using the cursor() method
   cursor = conn.cursor()
   #Creating table as per requirement
   cursor.execute("DROP TABLE IF EXISTS THB_BTC")
   sql = '''CREATE TABLE cryptomine(blockNumber int,
                                    timeStamp int,
                                    hash char,
                                    nonce char,
                                    blockHash char,
                                    from_ad char,
                                    contractAddress char,
                                    to_ad char,
                                    value char,
                                    tokenName char,
                                    tokenSymbol char,
                                    tokenDecimal char,
                                    transactionIndex char,
                                    gas char,
                                    gasPrice char,
                                    gasUsed char,
                                    cumulativeGasUsed char,
                                    input char,
                                    confirmations char
   )'''
   cursor.execute(sql)
   print("Table created successfully........")
   conn.commit()
   #Closing the connection
   conn.close()

param_dic = {
    "host"      : "localhost",
    "database"  : "postgres",
    "user"      : "postgres",
    "password"  : "Teerapong29"
}
connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    param_dic['user'],
    param_dic['password'],
    param_dic['host'],
    param_dic['database']
)

def insert_data(df):
    """
    Using a dummy table to test this call library
    """
    engine = create_engine(connect)
    df.to_sql(
        'cryptomine', 
        con=engine, 
        index=False, 
        if_exists='replace'
    )
    print("Push Data To Database Done!!")

df_r = pd.DataFrame(columns=["blockNumber","timeStamp","hash","nonce","blockHash",
                             "from_ad","contractAddress","to_ad","tokenName","tokenSymbol",
                             "tokenDecimal","transactionIndex","gas","gasPrice","gasUsed",
                             "cumulativeGasUsed","input","confirmations"
                             ])

while True :
      game_address = '0xd44fd09d74cd13838f137b590497595d6b3feea4'
      api = 'https://api.bscscan.com/api?module=account&action=tokentx&contractaddress='+game_address+\
            '&page=1&offset=10000&startblock=0&endblock=999999999&sort=desc&apikey=YourApiKeyToken'

      print('Get Data From API')

      response = requests.get(api)
      data = response.json()
      result = data.get('result')

      print('Get Data Done!!')

      df = pd.DataFrame(result)
      df.rename(columns={'from':'from_ad','to':'to_ad'}, inplace=True)
      df_r = pd.concat([df_r,df])
      df_r.drop_duplicates(["blockNumber","timeStamp","hash","nonce","blockHash",
                            "from_ad","contractAddress","to_ad","tokenName","tokenSymbol",
                            "tokenDecimal","transactionIndex","gas","gasPrice","gasUsed",
                            "cumulativeGasUsed","input"],inplace = True)

      insert_data(df_r)
      time.sleep(30)
      print('------------------')