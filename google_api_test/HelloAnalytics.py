"""A simple example of how to access the Google Analytics API."""
# GOOGLE API 
import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from googleapiclient.errors import HttpError
# END GOOGLE API

import csv
import os
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime, timedelta
from contextlib import closing

def get_service(api_name, api_version, scope, client_secrets_path):
    """Get a service that communicates to a Google API.
    
    Args:
      api_name: string The name of the api to connect to.
      api_version: string The api version to connect to.
      scope: A list of strings representing the auth scopes to authorize for the
        connection.
      client_secrets_path: string A path to a valid client secrets file.
    
    Returns:
      A service that is connected to the specified API.
    """
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])
    
    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        client_secrets_path, scope=scope,
        message=tools.message_if_missing(client_secrets_path))
    
    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage(api_name + '.dat')
    credentials = storage.get()
    http=httplib2.Http(ca_certs='cacerts.txt')
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags, http)
    http = credentials.authorize(http)
    
    # Build the service object.
    service = build(api_name, api_version, http=http)
    
    return service


def get_profile_id(service):
    # Use the Analytics service object to get the first profile id.
    # Get a list of all Google Analytics accounts for the authorized user.
    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        # Get the first Google Analytics account.
        account = accounts.get('items')[0].get('id')
        
        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
            accountId=account).execute()
        
        if properties.get('items'):
            # Get the first property id.
            #property = properties.get('items')[0].get('id')
            props = properties.get('items')
            for i in range(len(props)):
                if props[i].get('name') == 'InkPixi.com':
                    property =  props[i].get('id')
            # Get a list of all views (profiles) for the first property.
            profiles = service.management().profiles().list(accountId=account, webPropertyId=property).execute()
         
            if profiles.get('items'):
                # return the first view (profile) id.
                return profiles.get('items')[0].get('id')
                
    return None


def get_data(service, profileID, start_dt, end_dt):
    try:
        api_query = service.data().ga().get(
                ids='ga:' + profileID,
                start_date=start_dt,
                end_date=end_dt,
                metrics='ga:transactions, ga:transactionRevenue',
                dimensions='ga:transactionId, ga:source, ga:medium, ga:campaign, ga:date',
                sort='ga:campaign, ga:transactions, ga:transactionId',
                filters='ga:source==exacttarget',
                max_results='5000')
    except TypeError as e:
        print(str(e))
    except HttpError as e:
        print(str(e))
    
    try:
        results = api_query.execute()
        return results
    except HttpError as err:
        print(str(err))


def import_data(results):
    headers = results.get('columnHeaders')
    lst_headers = []
    for header in headers:
        nm = header.get('name')
        lst_headers.append(nm.replace('ga:', ''))
    
    with open('googlecsv.csv', 'w', newline='') as csv_file:
        wr = csv.writer(csv_file)
        wr.writerow(lst_headers)
        for row in results.get('rows'):
            wr.writerow([row[0], row[1], row[2], row[3], row[4], row[5]])
    
    os.startfile('googlecsv.csv')
    
    con = mysql.connector.connect(user = 'root', password = 'rowsby01', host = '192.168.1.196', database = 'inkpixi_reporting', raise_on_warnings = True) 
    cur = con.cursor()

    with closing(cur) as db:
        columns = lst_headers
        query = 'insert into google_order_analysis({0}) values({1})'
        query = query.format(','.join(columns), ','.join(['%s'] * len(columns)))
        print(query)
        try:
            for row in results['rows']:
                content = list(row[i] for i in range(len(columns)))
                print(content)
                db.execute(query, content)
            con.commit()
            db.close()
            con.close()
        except mysql.connector.Error as err:
            print(str(err))

if __name__ == '__main__':
    start_dt = '2016-01-01'
    end_dt = datetime.now()
    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    # Authenticate and construct service.
    service = get_service('analytics', 'v3', scope, 'client_secrets.json')
    profile = get_profile_id(service)
    
    results = get_data(service, profile, start_dt, end_dt.strftime('%Y-%m-%d'))
    import_data(results)   