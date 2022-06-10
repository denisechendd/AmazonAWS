import json
import requests, os
import logging
import boto3
import pandas as pd
import csv
from io import StringIO

logging.basicConfig(filename= 'output/python_output.log', level=logging.INFO)
s3_var = boto3.client('s3')
os.environ.setdefault('AWS_PROFILE', 'aws_free_denise')

def lambda_handler(event, context):
    file_name = 'us.csv'
    file_name1 = 'time-series-19-covid-combined.csv'
    file_name2 = 'Day_Confirmation_Age_County_Gender_19CoV.csv'
    file_name3 = 'covid19_global_cases_and_deaths.csv'
    # download data
    uscountry = download('https://raw.githubusercontent.com/nytimes/covid-19-data/master', file_name)
    uscountry_recovery = download('https://raw.githubusercontent.com/datasets/covid-19/master/data', file_name1)
    taiwan_data = download('https://od.cdc.gov.tw/eic', file_name2)
    world_data = download('https://od.cdc.gov.tw/eic/covid19', file_name3)
    vaccine_content =download('https://covid-19.nchc.org.tw/api/covid19?CK=covid-19@nchc.org.tw&querydata=2006', file_name='')
    vaccine_json = json.load(vaccine_content)
    with open('data/vaccine_taiwan.json', 'w', encoding='utf-8') as file:
        json.dump(vaccine_json, file, ensure_ascii=False)
        file.close()
    # upload data
    uscountry_s3 = upload_s3(uscountry.getvalue(), 'aws-covid19-proj', 'data/raw/us.csv')
    uscountry_recovery_s3 = upload_s3(uscountry_recovery.getvalue(), 'aws-covid19-proj', 'data/raw/us_recovery.csv')
    taiwan_data_s3 = upload_s3(taiwan_data.getvalue(), 'aws-covid19-proj', 'data/raw/taiwan_data.csv')
    world_data_s3 = upload_s3(world_data.getvalue(), 'aws-covid19-proj', 'data/raw/world_data.csv')


def download(url, file_name):
    if file_name:
        file_req = requests.get(f'{url}/{file_name}')
    else:
        file_req = requests.get(f'{url}')
    file_content = StringIO(file_req.content.decode('utf-8'))
    if file_req.status_code ==200:
        logging.info(f'complete to download {file_name}')
        return file_content
    else:
        logging.warning('fail to download the file')

def upload_s3(body, bucket, file):
    '''upload data to s3
    body: content
    bucket: bucket name
    file: file name
    '''
    upload_res = s3_var.put_object(
    Bucket=bucket,
    Key=file,
    Body=body
    )
    if upload_res['ResponseMetadata'].get('HTTPStatusCode') == 200:
        logging.info(f'{file} complete to load to s3')
        return upload_res
    else:
        logging.warning('fail to load to s3')

if __name__ == '__main__':
    lambda_handler(None, None)