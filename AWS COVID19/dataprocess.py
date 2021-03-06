import requests, os
import logging
import boto3
import pandas as pd
from io import StringIO
import json

os.environ.setdefault('AWS_PROFILE', 'aws-free-denise')
s3_var = boto3.client('s3')
logging.basicConfig(filename='output/python_output.log', level=logging.INFO)

def age_group(x):
    if x in ['0','1','2','3','4', '5-9', '10-14', '15-19']:
        return '0-19'
    elif x in ['20-24', '25-29', '30-34']:
        return '20-34'
    elif x in ['35-39', '40-44', '45-49']:
        return '35-49'
    elif x in ['50-54', '55-59', '60-64', '65-69']:
        return '50-69'
    else:
        return '>70'

def change_toint(x):
    if type(x)==str:
        string = x.replace(',','')
        return int(string)
    else:
        return None

def uscountry_proc(uscountry_content, date):
    # update uscountry data
    uscountry_df = pd.read_csv(uscountry_content)
    uscountry_df1 =uscountry_df.loc[uscountry_df['date']>date]
    uscountry_df1['date'] = pd.to_datetime(uscountry_df1['date'])
    uscountry_df1['month'] = uscountry_df1['date'].dt.month
    uscountry_df1['month_dt'] = 202203
    file_name = 'uscountry_dataproc.csv'
    uscountry_df1.to_csv(f'output/processdata/{file_name}', header=True, index=False, sep=',')
    logging.info(f'complete to download {file_name}')

def usrecovery_proc(us_recovery_content, date):
    # update usrecovery data
    us_recovery_df = pd.read_csv(us_recovery_content)
    us_recovery_df1 = us_recovery_df.loc[
        us_recovery_df['Country/Region'] == 'US', ['Date', 'Country/Region', 'Confirmed', 'Recovered', 'Deaths']]
    us_recovery_df1['Date'] = pd.to_datetime(us_recovery_df1['Date'])
    us_recovery_df1['Month'] = us_recovery_df1['Date'].dt.month
    us_recovery_df2 = us_recovery_df1.loc[us_recovery_df1['Date']>date]
    us_recovery_df2 = us_recovery_df2.reset_index().drop(['index'], axis=1)
    us_recovery_df2 = us_recovery_df2.rename(columns={'Country/Region': 'Country'})
    file_name = 'us_recovery_dataproc1.csv'
    us_recovery_df2.to_csv(f'output/processdata/{file_name}', index=False, header=True, sep=',')
    logging.info(f'complete to download {file_name}')

def taiwan_proc(taiwan_content, date):
    # update taiwan data
    taiwan_df = pd.read_csv(taiwan_content)
    taiwan_df['???????????????'] = pd.to_datetime(taiwan_df['???????????????'])
    taiwan_df1 = taiwan_df.loc[taiwan_df['???????????????']>date]
    taiwan_df1['??????'] = taiwan_df1['???????????????'].dt.month
    taiwan_df1['?????????'] = taiwan_df1['?????????'].apply(age_group)
    taiwan_df1 = taiwan_df1.drop(['????????????'], axis=1)
    taiwan_df1.columns = ['date', 'county', 'town', 'gender', 'Foreign_access', 'agegroup', 'cases', 'month']
    taiwan_df1.index.name = 'id'
    file_name = 'taiwan_dataproc1.csv'
    taiwan_df1.to_csv(f'output/processdata/{file_name}', index=True, header=True, sep=',')
    logging.info(f'complete to download {file_name}')

def vaccine_proc(file_path):
    # update vaccine data
    vaccine_df = pd.read_csv(file_path, encoding='utf-8')
    vaccine_df = vaccine_df.dropna(subset=['??????'])
    vaccine_df = vaccine_df.loc[vaccine_df['??????'] != '???????????????']
    vaccine_df = vaccine_df.reset_index()
    vaccine_df1 = vaccine_df.loc[(vaccine_df['??????'].str.contains(r'[0-9]+???[0-9]+???'))]
    row, col = vaccine_df.shape
    monthday_dict = pd.Series(vaccine_df1['??????'].values, index=vaccine_df1.index).to_dict()
    row_index = [i for i in range(1, max(monthday_dict.keys()) + 2)]
    vaccine_df['???_???'] = row_index
    vaccine_df['???_???'] = vaccine_df['???_???'].map(monthday_dict)
    vaccine_df = vaccine_df.dropna(subset=['???_???'])
    vaccine_df['??????_1'] = vaccine_df['??????'].str.split('(').str[0]
    vaccine_df['??????'] = vaccine_df['??????_1'].str.strip().str.cat(vaccine_df['???_???'])
    vaccine_df['??????'] = pd.to_datetime(vaccine_df['??????'], format="%Y???%m???%d???")
    vaccine_df['??????'] = vaccine_df['??????'].dt.month
    vaccine_df = vaccine_df.drop(['??????', '???_???', '??????_1', 'index'], axis=1)
    for row in ['??????????????????', 'AZ????????????', '?????????????????????', 'BNT????????????', '??????????????????', 'AZ????????????', '?????????????????????', 'BNT????????????', '??????????????????']:
        vaccine_df[row] = vaccine_df[row].apply(change_toint)
    vaccine_df.columns = ['date', 'totalpeoplevac', 'totalazvac', 'totalmodernavac', \
                          'totalbntvac', 'totalmvcvac', 'azvacstorage', 'modernavacstorage', \
                          'bntvacstorage', 'mvcvacstorage', 'month']
    file_name ='twnvaccine_dataproc1.csv'
    vaccine_df.to_csv(f'output/processdata/{file_name}', header=True, index=False, sep=',')
    logging.info(f'complete to download {file_name}')

def vaccinetwn_proc(vaccine_json, date):
    # update vaccine data1
    # vaccinetwn_json = json.load(vaccine_json)
    vaccinetaw_df = pd.read_json(vaccine_json)
    vaccinetaw_df.columns = ['ID', 'date', 'County', 'agegroup', '1dose_rate', '2dose_rate', 'extradose_rate',
                             '3dose_rate']
    vaccinetaw_df['date'] = pd.to_datetime(vaccinetaw_df['date'])
    vaccinetaw_df1 = vaccinetaw_df.loc[vaccinetaw_df['date']>date]
    file_name = 'twnvaccineregion_dataproc1.csv'
    vaccinetaw_df1.to_csv(f'output/processdata/{file_name}', header=True, index=False, sep=',')
    logging.info(f'complete to download {file_name}')