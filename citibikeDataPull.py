# python 3
#citibikeDataPull.py - pulls data from https://s3.amazonaws.com/tripdata/index.html.
#S3 bucket uses page scripts to load data, which is why we're using Selenium to scrape.


import os
import requests
import time

import pandas as pd
import numpy as np
import shelve
from selenium import webdriver
from sqlalchemy import create_engine
#from settings import DB_CONNECTION


def get_links_from_page(source):
    browser = webdriver.Firefox()
    browser.get(source)
    print('Opening page %s' % (source))
    time.sleep(10)    #allow time for page scripts to execute
    elem = browser.find_elements_by_tag_name('a')
    links = [i.get_attribute('href') for i in elem]
    links.remove(source)              
    return links


sourceURL = 'https://s3.amazonaws.com/tripdata/index.html' 
allLinks = get_links_from_page(sourceURL)
recentLinks = [i for i in allLinks if os.path.basename(i).startswith('2018')]


def import_data_to_dataframe(files):
    dataFrame = pd.DataFrame()
    for i in files:
        try:
            print('Opening ' + i + '...')
            newDf = pd.read_csv(i, index_col=0)
            print('Appending data...')
            dataFrame = pd.concat([dataFrame, newDf])
        except Exception as err:
            print('***ERROR*** ' +str(err))
    return dataFrame


df = import_data_to_dataframe(recentLinks)
print('Import complete.')


df.columns = df.columns.str.replace(' ', '_')
df[['usertype', 'gender']].apply(lambda x: x.astype('category'))
df[['starttime', 'stoptime']] = df[['starttime', 'stoptime']].apply(pd.to_datetime)
df[['start_station_id', 'end_station_id']].apply(lambda x: x.fillna(0).astype(int))



print(df.head())
print('\n\n')
print(df.info())
print('\n\n')
print(df.describe())

df.to_csv('.\\citibikedata.csv')

#conn = create_engine('postgresql://bakkerrs1:hfF(*$1j23@bakker-rscluster-1.cfxnz230wsra.us-east-2.redshift.amazonaws.com:7401/dev')
#df.to_sql('citibike_data', conn, index=False, if_exists='append', chunksize=1000)


print('Done')