
print ("--------------------------- TikTok Live Dashboard: FIRST MILE, SORT AND MIDDLE MILE -------------------")
# from google.colab import files
from datetime import datetime,timedelta
import os
import requests
import time
from pprint import pprint
import json
import pandas as pd
import sys
import numpy as np
import calendar
import gspread as gs
import gspread_dataframe as gd
import xlsxwriter
import string
import warnings
warnings.filterwarnings("ignore")

credentials = {
}
gc = gs.service_account_from_dict(credentials)

def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(5)

    if job['status'] == 3:
        return job['query_result_id']

    return None


def get_fresh_query_result(redash_url, query_id, api_key, params):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})
    payload = dict(max_age=0, parameters=params)
    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload))

    if response.status_code != 200:
        return 'Refresh failed'
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        while True:
            try:
                response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
                break
            except:
                print('retry')

        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    return response.json()['query_result']['data']['rows']

def get_fresh_query_result_no_params(redash_url, query_id, api_key):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})
    payload = dict(max_age=0)
    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload))

    if response.status_code != 200:
        return 'Refresh failed'
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        while True:
            try:
                response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
                break
            except:
                print('retry')

        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    return response.json()['query_result']['data']['rows']


# from google.colab import drive
# drive.mount('/content/drive')

print('>> Pulling data from Redash...')
loopcounter=0
while True:
    try:
        api_key = 'yourkey'
        result = get_fresh_query_result_no_params('https://redash-id.ninjavan.co/',2249, api_key)
        print('Pulling Data: Success.')
        break
    except:
        print('>> Pulling failed, retrying...')
        loopcounter = loopcounter +1
        if loopcounter >=5:
            break

raw_data = pd.DataFrame(result)
print('Total Active Order:',len(raw_data))
# raw_data.head()

dataframes = []

chunk_size = 10000  # specifying the chunk size
num_chunks = len(raw_data) // chunk_size + 1

# insert your files
# for chunk in pd.read_csv(r"C:\Users\Harits\Downloads\TikTok_Orders_P95_2024_03_01.csv", chunksize=chunk_size):
for i in range(num_chunks):
    chunk = raw_data[i * chunk_size: (i+1)*chunk_size]
    list_order_id = chunk['order_id'].unique().tolist()
    listToStr_orderid = ",".join(map(str, list_order_id))

    print('>> Pulling data from Redash...')
    loopcounter = 0
    while True:
        try:
            params = {'order_id': listToStr_orderid}
            api_key = 'yourkey'
            result1 = get_fresh_query_result('https://redash-id.ninjavan.co/',2250, api_key, params)
            current = pd.DataFrame(result1)
            break
        except:
            print('Failed. Retrying..')
            loopcounter = loopcounter +1
            if loopcounter >=5:
                break
    dataframes.append(current)

raw_data = pd.concat(dataframes)

# aging category by hours
conditions = [(raw_data['aging'] >= 0) & (raw_data['aging'] <= 12),
              (raw_data['aging'] > 12) & (raw_data['aging'] <= 24),
              (raw_data['aging'] > 24) & (raw_data['aging'] <= 36),
              (raw_data['aging'] > 36) & (raw_data['aging'] <= 48),
              (raw_data['aging'] > 48) & (raw_data['aging'] <= 60),
              (raw_data['aging'] > 60) & (raw_data['aging'] <= 72),
              (raw_data['aging'] > 72)]

values = ['0-12','12-24','24-36','36-48','48-60','60-72','>72']

raw_data['aging_category'] = np.select(conditions,values)

raw_data = raw_data[['order_id','tracking_id','global_shipper_id','shipper_name','granular_status','creation_datetime','aging','aging_category','origin_hub_region','origin_hub_name',
                     'dest_hub_region','dest_hub_area','dest_hub_name','last_scan_datetime','last_scan_type','last_scan_hub','last_scan_area','last_scan_region','shipment_status',
                     'shipment_type','shipment_event','department','refresh_at']]
# raw_data.head()
############### Export to Google Sheets
def export_to_sheets(file_name,sheet_name,df,mode='r'):
    ws = gc.open(file_name).worksheet(sheet_name)
    if(mode=='w'):
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)
        return True
    elif(mode=='a'):
        #ws.add_rows(4)
        old = gd.get_as_dataframe(worksheet=ws)
        updated = pd.concat([old,df])
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=updated,include_index=False,include_column_header=True,resize=True)
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)

# Middle Mile -------------------------------------------------------------------------------------------------------------------------------
middle_mile = raw_data[(raw_data['department'] == 'Middle Mile') & (~raw_data['granular_status'].isin(['Completed','Cancelled','Returned to Sender']))]

# Upload Raw Data
print('MM: Dump to Google Sheets...')
countinject = 0 
while True:
    try:
        inject = export_to_sheets("Tiktok Live Monitoring Data", 'Middle Mile', middle_mile, mode='w')
        print('DONE')
        break
    except:
        print('Failed. Retrying...')


# Sort -------------------------------------------------------------------------------------------------------------------------------
sort = raw_data[(raw_data['department'] == 'Sort') & (~raw_data['granular_status'].isin(['Completed','Cancelled','Returned to Sender','On Vehicle for Delivery','Pending Reschedule']))]

# Upload Raw Data
print('SORT: Dump to Google Sheets...')
countinject = 0    
while True:
    try:
        inject = export_to_sheets("Tiktok Live Monitoring Data", 'Sort', sort, mode='w')
        print('DONE')
        break
    except:
        print('Failed. Retrying...')
        
# First Mile -------------------------------------------------------------------------------------------------------------------------------
first_mile = raw_data[(raw_data['department'] == 'First Mile') & (~raw_data['granular_status'].isin(['Completed','Cancelled','Returned to Sender','On Vehicle for Delivery','Pending Reschedule']))]
shipper_historical = pd.read_csv(r'C:\Users\Ninja Xpress\Desktop\real time tiktok monitoring\shipper_historical.csv')
# shipper_historical = pd.read_csv(r"C:\Users\Harits\Downloads\id_ops___fm___shipper_historical_pick_up_type_2024-03-07T04_01_49.838624Z.csv")
first_mile = pd.merge(first_mile,shipper_historical[['global_shipper_id','last_inbound']],on='global_shipper_id', how='left')

    # Region
print('>> Pulling Data from Redash Hub Facilities Information....')
counterloop=0
while True:
    try:
        api_key = 'yourkey'
        result = get_fresh_query_result_no_params('https://redash-id.ninjavan.co/',2170, api_key)
        region = pd.DataFrame(result)
        print('Pulling Data: Success.')
        break
    except:
        print('>> Pulling failed, retrying...')

# region = pd.DataFrame(pd.read_csv(r'C:\Users\Ninja Xpress\Desktop\real time tiktok monitoring\hubs.csv'))
first_mile = pd.merge(first_mile, region[['name','region_name']], left_on='last_inbound',right_on='name', how='left')

# Last Historical Hub and Region (for Pending Pickup Orders)
first_mile.rename(columns={'last_inbound':'last_historical_inbound_hub','region_name':'last_historical_inbound_region'}, inplace = True)
first_mile['last_historical_inbound_hub'] = np.where(first_mile['last_historical_inbound_hub'].isnull(),'No Historical Pickup/Inbound', first_mile['last_historical_inbound_hub'])
first_mile['last_historical_inbound_region'] = np.where(first_mile['last_historical_inbound_region'].isnull(),'No Historical Pickup/Inbound', first_mile['last_historical_inbound_region'])

first_mile.drop(columns=['name'], inplace=True)
first_mile = first_mile[['order_id', 'tracking_id', 'global_shipper_id','shipper_name', 'granular_status','creation_datetime', 'aging', 'aging_category', 'origin_hub_region',
       'origin_hub_name', 'dest_hub_region', 'dest_hub_area', 'dest_hub_name','last_scan_datetime', 'last_scan_type', 'last_scan_hub','last_scan_area', 'last_scan_region',
       'department', 'refresh_at', 'last_historical_inbound_hub', 'last_historical_inbound_region']]

# Upload Raw Data
print('FM: Dump to Google Sheets...')
countinject = 0    
while True:
    try:
        inject = export_to_sheets("Tiktok Live Monitoring Data: First Mile", 'First Mile', first_mile, mode='w')
        print('DONE')
        break
    except:
        print('Failed. Retrying...')