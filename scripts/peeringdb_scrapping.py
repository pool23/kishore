#!/usr/bin/env python
"""
Purpose     : Extract data for Netflix

        #####################     Change log   ###############################
        ##------------------------------------------------------------------##
        ##  Author              ##Date                ##Current Version     ##
        ##------------------------------------------------------------------##
        ## Moody's Analytics    ##15th July,2018      ##V1.0                ##
        ##------------------------------------------------------------------##
        ######################################################################
        Date                   Version     Author      Description
        15th July,2018       v 0.1       Deepak        Initial Code
"""


import pandas as pd
import logging
from os import path
import numpy as np
from psycopg2 import IntegrityError
import hashlib
import psycopg2
import requests
from bs4 import BeautifulSoup
import datetime
import warnings
import re
#from tabulate import tabulate

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
    logpath = path.join(path.dirname(path.dirname(path.realpath(__file__))), "logs\Log_Netflix_Data")
    output_path = path.join(path.dirname(path.dirname(path.realpath(__file__))), "data\output\\")
    return logpath, output_path


def save_data(output_path):
    result = requests.get('https://www.peeringdb.com/net/457')
    pageSource = result.content
    soup = BeautifulSoup(pageSource)


    view_fields = soup.find('div', attrs={'class':'view_fields'}).find_all('div', attrs={'class':'row view_row'})
    fieldsDict = {'IPv4 Prefixes':'info_prefixes4', 'IPv6 Prefixes':'info_prefixes6', 'Last Updated':'updated'}
    for key in fieldsDict.keys():
        for row in view_fields:
            if key in row.text:
                fieldsDict[key] = row.find('div', attrs={'data-edit-name':fieldsDict[key]})
                if fieldsDict[key] is not None:
                    fieldsDict[key] = fieldsDict[key].text
                    break

    #print(fieldsDict)

    fieldsDict["Date"] = now.strftime("%Y-%m-%d")
    fieldsDict['Last Updated'] = fieldsDict['Last Updated'].split("T")[0]
    fieldsDict = {key: [val] for key, val in fieldsDict.items()}
    df_ip_prefix_count = pd.DataFrame.from_dict(fieldsDict)
    output = output_path + "NETFLIX_Data_IP_Prefix_Count" + datetime.datetime.now().strftime("%d_%b_%Y") + ".csv"
    df_ip_prefix_count.rename(
        columns={'IPv4 Prefixes': 'IPv4_Prefixes', 'IPv6 Prefixes': 'IPv6_Prefixes', 'Last Updated': 'Last_Updated'},
        inplace=True)
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
            insert_query = "INSERT INTO public.netflix_ip_prefix_count VALUES (%s,%s,%s,%s,%s)"
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




    tableData = []
    tableHeaders = ['Exchange', 'ASN', 'ipaddr4', 'ipaddr6', 'speed', 'is_rs_peer', 'url']
    tableData.append(tableHeaders)
    #Public Peering Exchange Points
    PPEP = soup.find('div', attrs={'id':'list-exchanges'}).find_all('div', attrs={'class':'row item'})
    for row in PPEP:
        columns = row.find_all('div', attrs={'class':re.compile('col')})
        Exchange = columns[0].find('a')
        ExchangeUrl = 'https://www.peeringdb.com'+Exchange['href'] if Exchange is not None else None
        Exchange = Exchange.text if Exchange is not None else None

        ASN = columns[0].find('div', attrs={'class':'asn'})
        ASN = ASN.text if ASN is not None else None

        ipaddr4 = columns[1].find('div', attrs={'data-edit-name':'ipaddr4'})
        ipaddr4 = ipaddr4.text if ipaddr4 is not None else None

        ipaddr6 = columns[1].find('div', attrs={'data-edit-name':'ipaddr6'})
        ipaddr6 = ipaddr6.text if ipaddr6 is not None else None

        speed = columns[2].find('div', attrs={'class':'speed'})
        speed = speed.text if speed is not None else None

        is_rs_peer = columns[2].find('div', attrs={'data-edit-type':'bool'})
        is_rs_peer = is_rs_peer.text if is_rs_peer is not None else None

        tableData.append([Exchange, ASN, ipaddr4, ipaddr6, speed, is_rs_peer, ExchangeUrl])

    #print(tabulate(tableData))

    df_Public_Peering_Exchange_Points = pd.DataFrame.from_records(tableData[1:], columns =["Exchange","ASN","Ipaddr4","Ipaddr6","Speed","is_rs_peer","Url"])
    df_Public_Peering_Exchange_Points.drop(columns=["is_rs_peer","ASN"],inplace= True)
    df_Public_Peering_Exchange_Points = df_Public_Peering_Exchange_Points.applymap(lambda x: str(x).replace("\n",""))
    #df_Public_Peering_Exchange_Points.to_csv("Public_Peering_Exchange_Points.csv", index =False)
    output = output_path + "NETFLIX_Data_Public_Peering_Exchange_Points" + datetime.datetime.now().strftime("%d_%b_%Y") + ".csv"
    df_Public_Peering_Exchange_Points["Date"] = now.strftime("%Y-%m-%d")
    df_Public_Peering_Exchange_Points.to_csv(output, index=False)
    ###################df_Public_Peering_Exchange_Points#Write this to DB

    msg = "Data Saved to Local Drive for Public Peering Data"
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
    for idx in range(len(df_Public_Peering_Exchange_Points.index)):
        buf3 = str(df_Public_Peering_Exchange_Points['Date'].iloc[idx]) + str(df_Public_Peering_Exchange_Points['Exchange'].iloc[idx])\
               + str(df_Public_Peering_Exchange_Points['Ipaddr4'].iloc[idx]) \
               + str(df_Public_Peering_Exchange_Points['Ipaddr6'].iloc[idx])
        hasher3 = hashlib.md5()
        hasher3.update(buf3.encode())
        temp = hasher3.hexdigest()
        print("Hash key Created")

        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        print("Insertion Started")
        try:
            insert_query = "INSERT INTO public.netflix_public_peering VALUES (%s,%s,%s,%s,%s,%s,%s)"
            row_data = (str(df_Public_Peering_Exchange_Points['Date'].iloc[idx]), str(df_Public_Peering_Exchange_Points['Exchange'].iloc[idx]),
               str(df_Public_Peering_Exchange_Points['Ipaddr4'].iloc[idx]),
               str(df_Public_Peering_Exchange_Points['Ipaddr6'].iloc[idx]), str(df_Public_Peering_Exchange_Points['Speed'].iloc[idx]),
               str(df_Public_Peering_Exchange_Points['Url'].iloc[idx]),str(temp) )

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


    # Private Peering Facilities
    PPFData = []
    headers = ['facility', 'Value', 'country', 'city', 'url']
    PPFData.append(headers)
    PPF = soup.find('div', attrs={'id':'list-facilities'}).find_all('div', attrs={'class':'row item'})
    for row in PPF:
        columns = row.find_all('div', attrs={'class': re.compile('col')})
        facility = columns[0].find('a')
        facility_url = 'https://www.peeringdb.com'+facility['href'] if facility is not None else None
        facility = facility.text if facility is not None else None

        Value = columns[0].find('div', text = re.compile('[0-9]*'))
        Value = Value.text if Value is not None else None

        country = columns[1].find('div', attrs={'class':'country'})
        country = country.text if country is not None else None

        city = columns[1].find('div', attrs={'class':'city'})
        city = city.text if city is not None else None

        PPFData.append([facility, Value, country, city, facility_url])

    #print(tabulate(PPFData))

    df_Private_Peering_Facilities = pd.DataFrame.from_records(PPFData[1:], columns =["Facility","Value","Country","City","Url"])
    df_Private_Peering_Facilities.drop(columns=["Value"],inplace= True)
    #df_Private_Peering_Facilities.to_csv("Private_Peering_Facilities.csv", index =False)
    output = output_path + "NETFLIX_Data_Private_Peering_Facilities" + datetime.datetime.now().strftime(
        "%d_%b_%Y") + ".csv"
    df_Private_Peering_Facilities["Date"] = now.strftime("%Y-%m-%d")
    df_Private_Peering_Facilities.to_csv(output, index=False)
    ########################df_Private_Peering_Facilities#Write this to DB

    msg = "Data Saved to Local Drive for Private Peering Data"
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
    for idx in range(len(df_Private_Peering_Facilities.index)):
        buf3 = str(df_Private_Peering_Facilities['Date'].iloc[idx]) + str(df_Private_Peering_Facilities['Facility'].iloc[idx])\
               + str(df_Private_Peering_Facilities['Country'].iloc[idx]) \
               + str(df_Private_Peering_Facilities['City'].iloc[idx])
        hasher3 = hashlib.md5()
        hasher3.update(buf3.encode())
        temp = hasher3.hexdigest()
        print("Hash key Created")

        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        print("Insertion Started")
        try:
            insert_query = "INSERT INTO public.netflix_private_peering VALUES (%s,%s,%s,%s,%s,%s)"
            row_data = (str(df_Private_Peering_Facilities['Date'].iloc[idx]), str(df_Private_Peering_Facilities['Facility'].iloc[idx]),
               str(df_Private_Peering_Facilities['Country'].iloc[idx]),
               str(df_Private_Peering_Facilities['City'].iloc[idx]), str(df_Private_Peering_Facilities['Url'].iloc[idx]),str(temp) )

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