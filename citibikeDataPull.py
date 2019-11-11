# python 3
#citibikeDataPull.py - pulls data from https://s3.amazonaws.com/tripdata/index.html.
#S3 bucket uses page scripts to load data, which is why we're using Selenium to scrape.


import os
import requests
import time

import pandas as pd
from selenium import webdriver


def get_links_from_page(source):
    browser = webdriver.Firefox()
    browser.get(source)
    print('Opening page %s' % (source))
    time.sleep(10)    #allow time for page scripts to execute
    elem = browser.find_elements_by_tag_name('a')
    links = [i.get_attribute('href') for i in elem]
    links.remove(source)
    return links


def import_data_to_dataframe(files):
    dataFrame = pd.DataFrame()
    for i in files:
        try:
            print('Opening ' + os.path.basename(i) + '...')
            newDf = pd.read_csv(i, index_col=False)
            print('Appending data...')
            dataFrame = pd.concat([dataFrame, newDf])
        except Exception as err:
            print('***ERROR*** ' +str(err))
    print('Import Complete.')
    return dataFrame


def clean_data(dataframe):
    print('Cleaning data.')
    dataframe = dataframe.dropna()
    dataframe = dataframe.drop_duplicates()
    dataframe.columns = dataframe.columns.str.replace(' ', '_')
    dataframe[['usertype', 'gender']].apply(lambda x: x.astype('category'))
    dataframe[['starttime', 'stoptime']] = df[['starttime', 'stoptime']].apply(pd.to_datetime)
    assert dataframe.notnull().all().all()
    return dataframe


def quick_df_facts(dataframe):
    print('Quick facts:')
    print('\n\nTop 5\n')
    print(dataframe.head())
    print('\n\nTypes\n')
    print(dataframe.info())
    print('\n\nDescribe \n')
    print(dataframe.describe())


if __name__ == '__main__':
    sourceURL = 'https://s3.amazonaws.com/tripdata/index.html' 
    allLinks = get_links_from_page(sourceURL)
    recentLinks = [i for i in allLinks if os.path.basename(i).startswith('2018')]
    df = import_data_to_dataframe(recentLinks)
    df = clean_data(df)
    quick_df_facts(df)
    