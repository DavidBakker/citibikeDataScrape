# python 3
#citibikeDataPull.py - pulls data from https://s3.amazonaws.com/tripdata/index.html.
#S3 bucket uses page scripts to load data, which is why we're using Selenium to scrape.


import os
import requests
from functools import wraps
from time import time
from io import StringIO

import pandas as pd
from selenium import webdriver
import boto3
import settings



def get_links_from_page(source):
    browser = webdriver.Firefox()
    browser.get(source)
    print('Opening page %s' % (source))
    time.sleep(10)    #allow time for page scripts to execute
    elem = browser.find_elements_by_tag_name('a')
    links = [i.get_attribute('href') for i in elem]
    links.remove(source)
    return links


def timing(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time()
        result = f(*args, **kwargs)
        end = time()
        print('Elapsed time: {}'.format(end-start))
        return result
    return wrapper


class CsvObject(object):
    
    def __init__(self, link):
        self.link = link
        self.filename = os.path.basename(self.link).replace('.zip', '')
        self.dataframe = pd.DataFrame()


    @timing
    def import_data(self):
        try:
            print('Opening ' + self.filename + '...')
            self.dataframe = pd.read_csv(self.link, index_col=False)
            print('Importing data...')
        except Exception as err:
            print('***ERROR*** ' +str(err))


    @timing
    def clean_data(self):
        print('Cleaning ' + self.filename)
        self.dataframe.dropna()
        self.dataframe.columns = self.dataframe.columns.str.replace(' ', '_')
        self.dataframe[['usertype', 'gender']].apply(lambda x: x.astype('category'))
        self.dataframe[['starttime', 'stoptime']].apply(pd.to_datetime)


    @timing
    def quick_facts(self):
        print('Quick facts:')
        print('\n\nTop 5\n')
        print(self.dataframe.head())
        print('\n\nTypes\n')
        print(self.dataframe.info())
        print('\n\nDescribe \n')
        print(self.dataframe.describe())


    @timing
    def upload_to_S3(self):
        session = boto3.Session(
            aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY,
            aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY,
            )
        csv_buffer = StringIO()
        self.dataframe.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object('dbpersonalstorage', self.filename).put(Body=csv_buffer.getvalue())



if __name__ == '__main__':
    sourceURL = 'https://s3.amazonaws.com/tripdata/index.html' 
    allLinks = get_links_from_page(sourceURL)
    recentLinks = [i for i in allLinks if os.path.basename(i).startswith('2018')]
    
    for u in recentLinks:
        u = CsvObject(u)
        u.import_data()
        u.clean_data()
        u.upload_to_S3()





