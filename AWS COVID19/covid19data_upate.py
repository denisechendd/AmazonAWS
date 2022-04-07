import requests, os
import boto3
import logging
import pandas as pd
from io import StringIO
from dataprocess import uscountry_proc
from dataprocess import usrecovery_proc
from dataprocess import taiwan_proc, vaccine_proc, vaccinetwn_proc

os.environ.setdefault('AWS_PROFILE', 'aws-free-denise')
s3_var = boto3.client('s3')
logging.basicConfig(filename='output/python_output.log', level=logging.INFO)

def download_s3(Bucket, file):
    s3_obj = s3_var.get_object(Bucket=Bucket, Key=file)
    content = StringIO(s3_obj['Body'].read().decode('utf-8'))
    return content

def download(url, filename):
    if filename:
        file_req = requests.get(f'{url}/{filename}')
    else:
        file_req = requests.get(f'{url}')
    file_content = StringIO(file_req.content.decode('utf-8'))
    if file_req.status_code ==200:
        logging.info(f'complete to download {filename}')
        return file_content
    else:
        logging.warning('fail to get the website data')

def upload_s3(body, Bucket, file):
    upload_res = s3_var.put_object(Bucket=Bucket, Key=file, Body=body)
    if upload_res['ResponseMetadata'].get('HTTPStatusCode') == 200:
        logging.info(f'{file} complete to load to s3')
        return upload_res
    else:
        logging.warning('fail to load to s3')

def main():
    file_name = 'us.csv'
    file_name1 = 'time-series-19-covid-combined.csv'
    file_name2 = 'Day_Confirmation_Age_County_Gender_19CoV.csv'
    date = '2022-03-19'
    # update uscountry data
    uscountry_content = download('https://raw.githubusercontent.com/nytimes/covid-19-data/master', file_name)
    uscountry_proc(uscountry_content, date)
    # update usrecovery data
    usrecovery_content = download('https://raw.githubusercontent.com/datasets/covid-19/master/data', file_name1)
    usrecovery_proc(usrecovery_content, date)
    # update taiwan data
    taiwan_content = download('https://od.cdc.gov.tw/eic', file_name2)
    taiwan_proc(taiwan_content, date)
    # update vaccine data
    vaccine_proc('data/twnvaccine.csv')
    # update vaccine data1
    twnvaccine_json = download('https://covid-19.nchc.org.tw/api/covid19?CK=covid-19@nchc.org.tw&querydata=2006', filename='')
    vaccinetwn_proc(twnvaccine_json, date)

if __name__ =='__main__':
    main()