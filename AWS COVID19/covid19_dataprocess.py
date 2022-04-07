import requests, os
import logging
import boto3
import pandas as pd
import csv
from io import StringIO

os.environ.setdefault('AWS_PROFILE', 'aws-free-denise')
s3_var = boto3.client('s3')
logging.basicConfig(filename='output/python_output.log', level=logging.INFO)

def lambda_handler(event, context):
    '''
    uscountry data: 設日期格式、加月份
    usrecovery data: 設日期格式、加月份、新增索引欄位
    taiwan_df: 設日期格式、加月份、更新年齡群
    vaccine_df : 將日期資料修改字串，並把日期對應到每一個欄位，改疫苗接種人次和疫苗存量欄位為數值資料
    vaccinetaw_df: 設日期格式、增加欄位名稱
    '''
    # uscountry data
    uscountry_content = download_s3(Bucket='aws-covid19-proj', file='data/raw/us.csv')
    uscountry_df = pd.read_csv(uscountry_content)
    uscountry_df['date'] = pd.to_datetime(uscountry_df['date'])
    uscountry_df['month'] = uscountry_df['date'].dt.month
    uscountry_df['month_dt'] = 202202
    # usrecovery data
    us_recovery_content = download_s3(Bucket='aws-covid19-proj', file='data/raw/us_recovery.csv')
    us_recovery_df = pd.read_csv(us_recovery_content)
    us_recovery_df1 = us_recovery_df.loc[
        us_recovery_df['Country/Region'] == 'US', ['Date', 'Country/Region', 'Confirmed', 'Recovered', 'Deaths']]
    us_recovery_df1['Date'] = pd.to_datetime(us_recovery_df1['Date'])
    us_recovery_df1['Month'] = us_recovery_df1['Date'].dt.month
    us_recovery_df1 = us_recovery_df1.reset_index().drop(['index'], axis=1)
    us_recovery_df1 = us_recovery_df1.rename(columns={'Country/Region': 'Country'})
    # taiwan COVID19確診數 data
    taiwan_content = download_s3(Bucket='aws-covid19-proj', file='data/raw/taiwan_data.csv')
    taiwan_df = pd.read_csv(taiwan_content)
    taiwan_df['個案研判日'] = pd.to_datetime(taiwan_df['個案研判日'])
    taiwan_df['月份'] = taiwan_df['個案研判日'].dt.month
    taiwan_df['年齡層'] = taiwan_df['年齡層'].apply(age_group)
    taiwan_df = taiwan_df.drop(['確定病名'], axis=1)
    taiwan_df.columns = ['date', 'county', 'town', 'gender', 'Foreign_access', 'agegroup', 'cases', 'month']
    taiwan_df.index.name = 'id'
    print(taiwan_df.head(10))
    # vaccine df
    vaccine_content = download_s3(Bucket='aws-covid19-proj', file='data/raw/COVID19_vaccine.csv')
    vaccine_df = pd.read_csv(vaccine_content, encoding='utf-8')
    vaccine_df = vaccine_df.dropna(subset=['日期'])
    vaccine_df = vaccine_df.loc[vaccine_df['日期'] != '疫苗日報表']
    vaccine_df = vaccine_df.reset_index()
    vaccine_df1 = vaccine_df.loc[(vaccine_df['日期'].str.contains(r'[0-9]+月[0-9]+日'))]
    row, col = vaccine_df.shape
    monthday_dict = pd.Series(vaccine_df1['日期'].values, index=vaccine_df1.index).to_dict()
    row_index = [i for i in range(1, max(monthday_dict.keys()) + 2)]
    vaccine_df['月_日'] = row_index
    vaccine_df['月_日'] = vaccine_df['月_日'].map(monthday_dict)
    vaccine_df = vaccine_df.dropna(subset=['月_日'])
    vaccine_df['日期_1'] = vaccine_df['日期'].str.split('(').str[0]
    vaccine_df['日期'] = vaccine_df['日期_1'].str.strip().str.cat(vaccine_df['月_日'])
    vaccine_df['日期'] = pd.to_datetime(vaccine_df['日期'], format="%Y年%m月%d日")
    vaccine_df['月份'] = vaccine_df['日期'].dt.month
    vaccine_df = vaccine_df.drop(['記事', '月_日', '日期_1', 'index'], axis=1)
    for row in ['累計接種人次', 'AZ累計接種', '莫德納累計接種', 'BNT累計接種', '高端累計接種', 'AZ推估存量', '莫德納推估存量', 'BNT推估存量', '高端推估存量']:
        vaccine_df[row] = vaccine_df[row].apply(change_toint)
    vaccine_df.columns = ['date', 'totalpeoplevac', 'totalazvac', 'totalmodernavac', \
                          'totalbntvac', 'totalmvcvac', 'azvacstorage', 'modernavacstorage', \
                          'bntvacstorage', 'mvcvacstorage', 'month']
    # 各縣市疫苗資料
    vaccinetaw_s3 = download_s3('aws-covid19-proj', 'data/raw/vaccine_taiwan.json')
    vaccinetaw_df = pd.read_json(vaccinetaw_s3)
    vaccinetaw_df.columns = ['ID', 'date', 'County', 'agegroup', '1dose_rate', '2dose_rate', 'extradose_rate',
                             '3dose_rate']
    vaccinetaw_df['date'] = pd.to_datetime(vaccinetaw_df['date'])

    # upload to s3
    csv_buff = StringIO()
    uscountry_df.to_csv(csv_buff, index=False, header=True, sep=',')
    uscountry_res = upload_s3(Bucket='aws-covid19-proj', file='data/process/uscountry/file/uscountry_dataproc.csv',
                              body=csv_buff.getvalue())
    csv_buff = StringIO()
    us_recovery_df1.to_csv(csv_buff, index=False, header=True, sep=',')
    usrecovery_res = upload_s3(Bucket='aws-covid19-proj', file='data/process/usrecovery/us_recovery_dataproc.csv',
                               body=csv_buff.getvalue())
    csv_buff = StringIO()
    taiwan_df.to_csv(csv_buff, index=True, header=True, sep=',')
    taiwan_res = upload_s3(Bucket='aws-covid19-proj', file='data/process/taiwan_proc.csv', body=csv_buff.getvalue())
    csv_buff = StringIO()
    vaccine_df.to_csv(csv_buff, index=False, header=True, sep=',')
    vaccine_res = upload_s3(Bucket='aws-covid19-proj', file='data/process/vaccine_dataproc.csv',
                            body=csv_buff.getvalue())
    csv_buff = StringIO()
    vaccinetaw_df.to_csv(csv_buff, index=False, header=True, sep=',')
    vaccinetaw_res = upload_s3(Bucket='aws-covid19-proj', file='data/process/台灣疫苗接種率/taiwanvaccine1_proc.csv',
                               body=csv_buff.getvalue())
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
def upload_s3(Bucket, body, file):
    upload_res = s3_var.put_object(Bucket=Bucket, Key=file, Body = body)
    if upload_res['ResponseMetadata'].get('HTTPStatusCode') ==200:
        logging.info('f{file} complete to load to s3')
        return upload_res
    else:
        logging.warning('fail to load to s3')

def download_s3(Bucket, file):
    s3_obj = s3_var.get_object(Bucket=Bucket, Key=file)
    content = StringIO(s3_obj['Body'].read().decode('utf-8'))
    return content

if __name__=='__main__':
    #main()
    lambda_handler(None, None)