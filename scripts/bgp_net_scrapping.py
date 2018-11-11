#!/usr/bin/env python
"""
Purpose     : Extract data for Netflix from bgp.net

        #####################     Change log   ###############################
        ##------------------------------------------------------------------##
        ##  Author              ##Date                ##Current Version     ##
        ##------------------------------------------------------------------##
        ## Moody's Analytics    ##15th July,2018      ##V1.0                ##
        ##------------------------------------------------------------------##
        ######################################################################
        Date                   Version     Author      Description
        18th July,2018       v 0.1       Deepak        Initial Code
"""

from bs4 import BeautifulSoup
import pandas as pd
import time
#from tabulate import tabulate
from selenium import webdriver
import datetime
import re
import logging
from os import path
from psycopg2 import IntegrityError

import hashlib
import psycopg2
import warnings

warnings.simplefilter(action='ignore')
now = datetime.datetime.now()

def log_config(logpath,change_log):
    """
    Purpose: This function will Configure Log file

    Args: Absolute path of 'logfile'

    Returns: Log File Path
    You can refer following examples for logging the context.
     logging.debug('This message will get printed in log file')
     logging.info('This message will get printed in log file')
     logging.warning('This message will get printed in log file')
     Logging.error('This message will get printed in log file')
     Logging.critical('This message will get printed in log file')

    """

    now = datetime.datetime.now()
    today = now.strftime("%d-%m-%Y")
    time_stamp = today + "_" + now.strftime("%H-%M-%S")
    logfile = logpath + "_" + time_stamp + ".log"
    #Logging the Doc String
    with open(logfile,mode= 'a') as ftr:
        ftr.write(change_log+"\n\n")
    #Log Configuration
    logging.basicConfig(filename=logfile,level=logging.DEBUG,
                        format='%(asctime)s- %(name)-12s - %(levelname)-8s - %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')
    logging.info('Starting the program execution\n')
    return logfile


def configuration():
    logpath = path.join(path.dirname(path.dirname(path.realpath(__file__))), "logs\Log_Netflix_Data_Bgp_net")
    output_path = path.join(path.dirname(path.dirname(path.realpath(__file__))), "data\output\\")
    return logpath, output_path


def save_data(output_path):
    driver = webdriver.Firefox()
    driver.maximize_window()
    driver.get('https://bgp.he.net/AS2906')
    time.sleep(5)
    jsoup = BeautifulSoup(driver.page_source)
    date = jsoup.find('div', attrs={'id': 'footer'})
    if date is not None:
        updated_date = date.text
    #print('updated_date = ', updated_date)
    # print(jsoup)



    prefixes = jsoup.find('div', attrs={'id': 'prefixes'})
    prefixes6 = jsoup.find('div', attrs={'id': 'prefixes6'})
    peers = jsoup.find('div', attrs={'id': 'peers'})
    peers6 = jsoup.find('div', attrs={'id': 'peers6'})
    ix = jsoup.find('div', attrs={'id': 'ix'})
    # print(prefixes)


    _prefixes_v4 = []
    if prefixes is not None:
        for pf in [['prefixes', prefixes]]:
            for tr in pf[1].find('tbody').find_all('tr'):
                tds = tr.find_all('td')
                a = tds[0].find('a')
                t = [pf[0], a['href'], a.text, tds[1].text.strip()]
                _prefixes_v4.append(t)
    #print(tabulate(_prefixes_v4))
    df_PrefixV4 = pd.DataFrame.from_records(_prefixes_v4,columns=["temp1", "temp2","Prefix", "Description"])
    df_PrefixV4.drop(columns=["temp1", "temp2"], inplace=True)
    output = output_path + "NETFLIX_Data_BGPNet_PrefixV4_" + datetime.datetime.now().strftime(
        "%d_%b_%Y") + ".csv"
    df_PrefixV4["Date"] = datetime.datetime.now().strftime("%Y-%m-%d")
    df_PrefixV4 = df_PrefixV4.reindex(columns=["Date","Prefix", "Description"])
    df_PrefixV4.to_csv(output, index=False)

    ###################Writing above to DB############
    msg = "Data Saved to Local Drive for Public Peering Data"
    logging.info(msg)
    print(msg)

    msg = "Creating Unique Key"
    logging.info(msg)
    print(msg)
    ###DB INSERT-- for df_PrefixV4---
    conn_str = "host={} dbname={} user={} password={}".format("35.194.230.117", "naresh", "naresh", "naresh")
    msg = "Inserting Data to DB"
    logging.info(msg)
    print(msg)
    # BUFFER - Facility,Country,City
    for idx in range(len(df_PrefixV4.index)):
        buf3 = str(df_PrefixV4['Date'].iloc[idx]) + str(
            df_PrefixV4['Prefix'].iloc[idx]) \
               + str(df_PrefixV4['Description'].iloc[idx])
        hasher3 = hashlib.md5()
        hasher3.update(buf3.encode())
        temp = hasher3.hexdigest()
        print("Hash key Created")

        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        print("Insertion Started")
        try:
            insert_query = "INSERT INTO public.netflix_bgpnet_prefix_v4data VALUES (%s,%s,%s,%s)"
            row_data = (str(df_PrefixV4['Date'].iloc[idx]),str(df_PrefixV4['Prefix'].iloc[idx]),
                        str(df_PrefixV4['Description'].iloc[idx]),str(temp))
            cur.execute(insert_query, row_data)
            msg = "Data Insertion Query Executed for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        except IntegrityError:
            msg = "Found Duplicate Record comparing to Database Content for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        conn.commit()
        cur.close()
        conn.close()
    msg = "Data uploaded to DB"
    logging.info(msg)
    

    _prefixes_v6 = []
    if prefixes is not None:
        for pf in [['prefixes6', prefixes6]]:
            for tr in pf[1].find('tbody').find_all('tr'):
                tds = tr.find_all('td')
                a = tds[0].find('a')
                t = [pf[0], a['href'], a.text, tds[1].text.strip()]
                _prefixes_v6.append(t)
    #print(tabulate(_prefixes_v6))


    df_PrefixV6 = pd.DataFrame.from_records(_prefixes_v6, columns=["temp1", "temp2", "Prefix", "Description"])
    df_PrefixV6.drop(columns=["temp1", "temp2"], inplace=True)
    output = output_path + "NETFLIX_Data_BGPNet_PrefixV4_" + datetime.datetime.now().strftime(
        "%d_%b_%Y") + ".csv"
    df_PrefixV6["Date"] = datetime.datetime.now().strftime("%Y-%m-%d")
    df_PrefixV6 = df_PrefixV6.reindex(columns=["Date", "Prefix", "Description"])
    df_PrefixV6.to_csv(output, index=False)

    ###################Writing above to DB############
    msg = "Data Saved to Local Drive for Public Peering Data"
    logging.info(msg)
    print(msg)

    msg = "Creating Unique Key"
    logging.info(msg)
    print(msg)
    ###DB INSERT-- for df_PrefixV4---
    conn_str = "host={} dbname={} user={} password={}".format("35.194.230.117", "naresh", "naresh", "naresh")
    msg = "Inserting Data to DB"
    logging.info(msg)
    print(msg)
    # BUFFER - Facility,Country,City
    for idx in range(len(df_PrefixV6.index)):
        buf3 = str(df_PrefixV6['Date'].iloc[idx]) + str(
            df_PrefixV6['Prefix'].iloc[idx]) \
               + str(df_PrefixV6['Description'].iloc[idx])
        hasher3 = hashlib.md5()
        hasher3.update(buf3.encode())
        temp = hasher3.hexdigest()
        print("Hash key Created")

        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        print("Insertion Started")
        try:
            insert_query = "INSERT INTO public.netflix_bgpnet_prefix_v6data VALUES (%s,%s,%s,%s)"
            row_data = (str(df_PrefixV6['Date'].iloc[idx]), str(df_PrefixV6['Prefix'].iloc[idx]),
                        str(df_PrefixV6['Description'].iloc[idx]), str(temp))
            cur.execute(insert_query, row_data)
            msg = "Data Insertion Query Executed for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        except IntegrityError:
            msg = "Found Duplicate Record comparing to Database Content for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        conn.commit()
        cur.close()
        conn.close()
    msg = "Data uploaded to DB"
    logging.info(msg)


    _peers4 = []
    if peers is not None:
        for peer in [['peers', peers]]:
            for tr in peer[1].find('tbody').find_all('tr'):
                tds = tr.find_all('td')
                a = tds[3].find('a')
                t = [peer[0], tds[1].text.strip(), tds[2].text.strip(), a['href'], a.text.strip()]
                _peers4.append(t)
    #print(tabulate(_peers4))
    df_PeersV4 = pd.DataFrame.from_records(_peers4, columns=["temp1", "Description","IPV6","temp2", "Peer" ])
    df_PeersV4.drop(columns=["temp1", "temp2"], inplace=True)
    output = output_path + "NETFLIX_Data_BGPNet_PeerV4_" + datetime.datetime.now().strftime(
        "%d_%b_%Y") + ".csv"
    df_PeersV4["Date"] = datetime.datetime.now().strftime("%Y-%m-%d")
    df_PeersV4 = df_PeersV4.reindex(columns=["Date", "Description","IPV6","Peer"])
    df_PeersV4.to_csv(output, index=False)

    ###################Writing above to DB############
    msg = "Data Saved to Local Drive for Public Peering Data"
    logging.info(msg)
    print(msg)

    msg = "Creating Unique Key"
    logging.info(msg)
    print(msg)
    ###DB INSERT-- for df_PrefixV4---
    conn_str = "host={} dbname={} user={} password={}".format("35.194.230.117", "naresh", "naresh", "naresh")
    msg = "Inserting Data to DB"
    logging.info(msg)
    print(msg)
    # BUFFER - Facility,Country,City
    for idx in range(len(df_PeersV4.index)):
        buf3 = str(df_PeersV4['Date'].iloc[idx])+ str(df_PeersV4['Description'].iloc[idx]) + str(df_PeersV4["IPV6"].iloc[idx]) \
               + str(df_PeersV4["Peer"].iloc[idx])
        hasher3 = hashlib.md5()
        hasher3.update(buf3.encode())
        temp = hasher3.hexdigest()
        print("Hash key Created")

        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        print("Insertion Started")
        try:
            insert_query = "INSERT INTO public.netflix_bgpnet_peer_v4data VALUES (%s,%s,%s,%s,%s)"
            row_data = (str(df_PeersV4['Date'].iloc[idx]),str(df_PeersV4['Description'].iloc[idx]),str(df_PeersV4["IPV6"].iloc[idx]),\
               str(df_PeersV4["Peer"].iloc[idx]), str(temp))
            cur.execute(insert_query, row_data)
            msg = "Data Insertion Query Executed for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        except IntegrityError:
            msg = "Found Duplicate Record comparing to Database Content for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        conn.commit()
        cur.close()
        conn.close()
    msg = "Data uploaded to DB"
    logging.info(msg)

    _peers6 = []
    if peers is not None:
        for peer in [['peers6', peers6]]:
            for tr in peer[1].find('tbody').find_all('tr'):
                tds = tr.find_all('td')
                a = tds[3].find('a')
                t = [peer[0], tds[1].text.strip(), tds[2].text.strip(), a['href'], a.text.strip()]
                _peers6.append(t)
    #print(tabulate(_peers6))
    df_PeersV6 = pd.DataFrame.from_records(_peers6, columns=["temp1", "Description","IPV4","temp2", "Peer" ])
    df_PeersV6.drop(columns=["temp1", "temp2"], inplace=True)
    output = output_path + "NETFLIX_Data_BGPNet_PeerV6_" + datetime.datetime.now().strftime(
        "%d_%b_%Y") + ".csv"
    df_PeersV6["Date"] = datetime.datetime.now().strftime("%Y-%m-%d")
    df_PeersV6 = df_PeersV6.reindex(columns=["Date", "Description","IPV4","Peer"])
    df_PeersV6.to_csv(output, index=False)

    ###################Writing above to DB############
    msg = "Data Saved to Local Drive for Public Peering Data"
    logging.info(msg)
    print(msg)

    msg = "Creating Unique Key"
    logging.info(msg)
    print(msg)
    ###DB INSERT-- for df_PrefixV4---
    conn_str = "host={} dbname={} user={} password={}".format("35.194.230.117", "naresh", "naresh", "naresh")
    msg = "Inserting Data to DB"
    logging.info(msg)
    print(msg)
    # BUFFER - Facility,Country,City
    for idx in range(len(df_PeersV6.index)):
        buf3 = str(df_PeersV6['Date'].iloc[idx])+ str(df_PeersV6['Description'].iloc[idx]) + str(df_PeersV6["IPV4"].iloc[idx]) \
               + str(df_PeersV6["Peer"].iloc[idx])
        hasher3 = hashlib.md5()
        hasher3.update(buf3.encode())
        temp = hasher3.hexdigest()
        print("Hash key Created")

        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        print("Insertion Started")
        try:
            insert_query = "INSERT INTO public.netflix_bgpnet_peer_v6data VALUES (%s,%s,%s,%s,%s)"
            row_data = (str(df_PeersV6['Date'].iloc[idx]),str(df_PeersV6['Description'].iloc[idx]),str(df_PeersV6["IPV4"].iloc[idx]),\
               str(df_PeersV6["Peer"].iloc[idx]), str(temp))
            cur.execute(insert_query, row_data)
            msg = "Data Insertion Query Executed for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        except IntegrityError:
            msg = "Found Duplicate Record comparing to Database Content for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        conn.commit()
        cur.close()
        conn.close()
    msg = "Data uploaded to DB"
    logging.info(msg)


    _ix = []
    if ix is not None:
        for tr in ix.find('tbody').find_all('tr'):
            tds = tr.find_all('td')
            t1 = tds[0].find('a')
            a = ['ix', t1.text, t1['href'], tds[1].text, tds[2].text,
                 str(tds[3]).replace('<td class="font-small">', '').replace('</td>', '').replace("<br/>", ','),
                 str(tds[4]).replace('<td class="font-small">', '').replace('</td>', '').replace("<br/>", ',')]
            _ix.append(a)

    #print(tabulate(_ix))

    df_exchange = pd.DataFrame.from_records(_ix, columns=["temp1", "Exchange", "temp2","Country_Code", "City", "IPV4","IPV6"])
    df_exchange.drop(columns=["temp1", "temp2"], inplace=True)
    output = output_path + "NETFLIX_Data_BGPNet_ExchangeData_" + datetime.datetime.now().strftime(
        "%d_%b_%Y") + ".csv"
    df_exchange["Date"] = datetime.datetime.now().strftime("%Y-%m-%d")
    df_exchange = df_exchange.reindex(columns=["Date", "Exchange", "Country_Code", "City", "IPV4","IPV6"])
    df_exchange = df_exchange.applymap(lambda x: str(x).replace("\n", "").strip())
    df_exchange.to_csv(output, index=False)

    ###################Writing above to DB############
    msg = "Data Saved to Local Drive for Public Peering Data"
    logging.info(msg)
    print(msg)

    msg = "Creating Unique Key"
    logging.info(msg)
    print(msg)
    ###DB INSERT-- for df_PrefixV4---
    conn_str = "host={} dbname={} user={} password={}".format("35.194.230.117", "naresh", "naresh", "naresh")
    msg = "Inserting Data to DB"
    logging.info(msg)
    print(msg)
    # BUFFER - Facility,Country,City
    for idx in range(len(df_exchange.index)):
        buf3 = str(df_exchange['Date'].iloc[idx]) + str(df_exchange['Exchange'].iloc[idx]) + str(df_exchange["Country_Code"].iloc[idx]) \
               + str(df_exchange["City"].iloc[idx])+ str(df_exchange["IPV4"].iloc[idx])+ str(df_exchange["IPV6"].iloc[idx])
        hasher3 = hashlib.md5()
        hasher3.update(buf3.encode())
        temp = hasher3.hexdigest()
        print("Hash key Created")

        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        print("Insertion Started")
        try:
            insert_query = "INSERT INTO public.netflix_bgpnet_exchange_data VALUES (%s,%s,%s,%s,%s,%s,%s)"
            row_data = (str(df_exchange['Date'].iloc[idx]), str(df_exchange['Exchange'].iloc[idx]),
                        str(df_exchange["Country_Code"].iloc[idx]),str(df_exchange["City"].iloc[idx]),
                        str(df_exchange["IPV4"].iloc[idx]), str(df_exchange["IPV6"].iloc[idx]),str(temp))
            cur.execute(insert_query, row_data)
            msg = "Data Insertion Query Executed for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        except IntegrityError:
            msg = "Found Duplicate Record comparing to Database Content for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        conn.commit()
        cur.close()
        conn.close()
    msg = "Data uploaded to DB"
    logging.info(msg)

    text = driver.find_element_by_tag_name('body').text
    pov4 = re.search('Prefixes Originated \(v4\): ([0-9]*)', text)
    if pov4 is not None:
        pov4 = pov4.group(1)
    pov6 = re.search('Prefixes Originated \(v6\): ([0-9]*)', text)
    if pov6 is not None:
        pov6 = pov6.group(1)
    Prefixes_Originated_v4 = pov4
    Prefixes_Originated_v6 = pov6
    #print(Prefixes_Originated_v4, Prefixes_Originated_v6)
    driver.close()
    now = datetime.datetime.now()


    msg = updated_date
    datetime_object = datetime.datetime.strptime(" ".join(msg.split()[1:4]), "%d %b %Y")
    updated_date = datetime_object.strftime("%Y-%m-%d")
    ip_prefix_count = {'IPv4_Prefixes': Prefixes_Originated_v4, 'IPv6_Prefixes': Prefixes_Originated_v6,
                       'Last_Updated': updated_date, 'Date': now.strftime("%Y-%m-%d")}
    ip_prefix_count = {key: [str(val)] for key, val in ip_prefix_count.items()}
    df_ip_prefix_count = pd.DataFrame.from_dict(ip_prefix_count)
    output = output_path + "NETFLIX_Data_BGPNet_IP_Prefix_Count" + datetime.datetime.now().strftime("%d_%b_%Y") + ".csv"
    df_ip_prefix_count.to_csv(output, index = False)

    ############fieldsDict#Save this to DB
    msg = "Data Saved to Local Drive for IP Prefix Count Data"
    logging.info(msg)
    print(msg)
    msg = "Creating Unique Key"
    logging.info(msg)
    print(msg)
    ###DB INSERT-- for df_extracted_monthly---
    conn_str = "host={} dbname={} user={} password={}".format("35.194.230.117", "naresh", "naresh", "naresh")

    msg = "Inserting Data to DB"
    logging.info(msg)
    print(msg)
    #BUFFER - Facility,Country,City
    for idx in range(len(df_ip_prefix_count.index)):
        buf3 = str(df_ip_prefix_count['Date'].iloc[idx]) + str(df_ip_prefix_count['IPv4_Prefixes'].iloc[idx])\
               + str(df_ip_prefix_count['IPv6_Prefixes'].iloc[idx]) \
               + str(df_ip_prefix_count['Last_Updated'].iloc[idx])
        hasher3 = hashlib.md5()
        hasher3.update(buf3.encode())
        temp = hasher3.hexdigest()
        print("Hash key Created")

        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        print("Insertion Started")
        try:
            insert_query = "INSERT INTO public.netflix_bgpnet_ip_prefix_count VALUES (%s,%s,%s,%s,%s)"
            row_data = (str(df_ip_prefix_count['Date'].iloc[idx]), str(df_ip_prefix_count['IPv4_Prefixes'].iloc[idx]),
               str(df_ip_prefix_count['IPv6_Prefixes'].iloc[idx]),
               str(df_ip_prefix_count['Last_Updated'].iloc[idx]), str(temp) )

            cur.execute(insert_query, row_data)
            msg = "Data Insertion Query Executed for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        except IntegrityError:
            msg = "Found Duplicate Record comparing to Database Content for CSV line no: {}".format(idx)
            logging.info(msg)
            print(msg)
        conn.commit()
        cur.close()
        conn.close()
    msg = "Data uploaded to DB"
    logging.info(msg)


if __name__== "__main__":
    print("Please wait while we execute the code and generate the Log file for you...")
    logpath, output_path = configuration()
    log_config(logpath, change_log=__doc__)
    save_data(output_path)