import datetime
import finnhub
import pandas as pd
from google.cloud import storage
import os
import time
import sqlalchemy as db
from sqlalchemy.engine import make_url

def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp)
def timestamp_to_date(timestamp):
    return datetime.date.fromtimestamp(timestamp)
def date_to_timestamp(date:str):
    return int(datetime.datetime.strptime(date, '%Y-%m-%d').timestamp())
def datetime_to_timestamp(d:str):
    return int(datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S').timestamp())
class Crypto:
    def __init__(self,name,start_date,api_key="#############",sql_server_address="10.54.160.2",database_name="cryptodb"):
        self.name=name
        self.start_date=start_date
        self.end_date=date_to_timestamp("2100-01-01")
        self.latest_date=None
        self.finnhub_client = finnhub.Client(api_key=api_key)
        self.day_in_seconds=86400
        self.finnhub_record_limit=499
        self.daily_increment_by=self.day_in_seconds*self.finnhub_record_limit
        self.table_name="crypto_"+self.name.lower().replace(":","_")
        self.engine=None
        self.df=None
        self.sql_server_address=sql_server_address
        self.database_name=database_name

        self.database_username="######"
        self.database_password="######"
        self.indicators={
            'rsi':14,
            'macd':{'fast_period':12, 'slow_period':26, 'signal_period':9},
            'stoch':{'fastk_period':5, 'slowk_period':3, 'slowd_period':3},
            'sma':5,
            'mom':3,
            'ema':5,
            'adx':14,
            'mfi':14,
            'obv':14
        }
    def connect_to_sql(self):
        sqlUrl = make_url(f"mysql+mysqlconnector://{self.database_username}:{self.database_password}@{self.sql_server_address}/{self.database_name}")
        self.engine = db.create_engine(sqlUrl,echo=False)
        self.engine.connect()
        print("Connected to SQL")
        return self.engine

    def retrive_data_from_finnhub(self,symbol,interval,start_date,end_date):
        candles=self.finnhub_client.crypto_candles(symbol, interval, start_date, end_date)
        if len(candles)==1 and candles['s']=='no_data':
            return None
        candles=pd.DataFrame.from_dict(candles)

        for key,value in self.indicators.items():
            if type(value)!=dict:
                df2= self.finnhub_client.technical_indicator(symbol=symbol, resolution='D', _from=start_date, to=end_date, indicator=key,indicator_fields={f"timeperiod":{value}})
            else:
                df2=self.finnhub_client.technical_indicator(symbol=symbol, resolution='D', _from=start_date, to=end_date, indicator=key,indicator_fields=value)

            df2=pd.DataFrame.from_dict(df2)
            df2.drop(columns=['t','s','o','c','h','l','v'],inplace=True)

            candles=pd.concat([candles,df2],axis=1)
        return candles
        
    def fetch_all_from_finnhub(self):
        
        df=pd.DataFrame()
        while (True):
            daily_rows=self.retrive_data_from_finnhub(self.name, 'D', self.start_date, self.start_date+self.daily_increment_by)
            time.sleep(0.5)
            #print(daily_rows)
            if type(daily_rows)!=pd.DataFrame:
                #No more data
                df.rename(columns={'c':'close', 'h':'high', 'l':'low', 'o':'open', 's':'status', 't':'timestamp', 'v':'volume'}, inplace=True)
                df.drop(columns='status', inplace=True)
                          
                df['target_open']=df['open'].shift(-1)
                df['target_close']=df['close'].shift(-1)
                df['target_high']=df['high'].shift(-1)
                df['target_low']=df['low'].shift(-1)
                df=df[33:-1]
                self.latest_date=df['timestamp'].max()
                self.df=df
                break
            print(f"{len(daily_rows['t'])} rows retrieved")
            if df.shape[0]==0:
                df=daily_rows.copy(True)
            else:
                df=df.append(daily_rows, ignore_index=True)
            print(df.shape,"total rows.")
            self.start_date+=self.daily_increment_by
        print("Total rows:",df.shape[0])
    def return_df(self):
        return self.df
    def to_sql(self,cnxn=None,if_exists="replace"):
        if cnxn is None:
            cnxn=self.connect_to_sql()
        self.connect_to_sql()
        self.df.to_sql(self.table_name, cnxn, if_exists=if_exists, method=None,index=False)
        cnxn.execute(f"ALTER TABLE {self.database_name}.{self.table_name} ADD PRIMARY KEY (timestamp)")
        print("Dataframe saved to SQL")
    def fetch_from_sql(self,cnxn=None):
        try:
            if cnxn is None:
                cnxn=self.connect_to_sql()
            self.df=pd.read_sql(f"SELECT * FROM {self.database_name}.{self.table_name}", cnxn)
        except Exception as e:
            print(e)
            self.df=None
    def fetch(self,cnxn=None,store_to_sql=True):
        if cnxn is None:
            cnxn=self.connect_to_sql()
        self.fetch_from_sql(cnxn)
        if self.df is None:
            self.fetch_all_from_finnhub()
            if store_to_sql:
                self.to_sql(cnxn)
    def update(self,cnxn=None,store_to_sql=True,store_to_gcs=False):
        if cnxn is None:
            cnxn=self.connect_to_sql()
        if self.latest_date is None:
            print("No data to update")
            return
        df2=self.retrive_data_from_finnhub(self.name, 'D', self.latest_date, self.latest_date+self.daily_increment_by)
        self.df=self.df.append(df2, ignore_index=True)
        if store_to_sql:
            self.to_sql(cnxn,method='append')
        if store_to_gcs:
            self.to_gcs()
        print("Updated dataframe with",df2.shape[0],"rows")
    def to_gcs(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']='serviceacc.json'
        storage_client = storage.Client()
        bucket_name="smart-trader-training-dataset"
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(f"{self.table_name}.csv")
        blob.upload_from_string(self.df.to_csv(index=False))
        print("Uploaded to GCS successfully")
        
       

