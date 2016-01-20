import os
import csv
import pyodbc
import shutil
from ftplib import FTP, error_temp
from datetime import datetime, timedelta
from pyodbc import IntegrityError

def connect_ftp():
    try:
        #Connect to Exact Target FTP site.
        host = 'ftp.s4.exacttarget.com'
        user = '1005090'
        password = 'Rx7.k2.F'
            
        ftp = FTP(host)
        ftp.login(user, password)
        #change the default directory for Exact Target.
        ftp.cwd('reports')
        return ftp
    except error_temp as e:
        print(str(e), ' retrying')
        connect_ftp()
        #QMessageBox.critical(self,'Cannot Connect', 'Could not connect to the FTP server, please try again. \n %s' % str(e), QMessageBox.Ok)
        
def get_file_list(ftp):
    #grabs the list of files in the currently connected ftp server.
    filenames = []
    try:
        ftp.retrlines('NLST', filenames.append)
    except error_temp as e:
        #QMessageBox.critical(self,'Cannot Connect', 'Could not connect to the FTP server, please try again. \n %s' % str(e), QMessageBox.Ok)
        print(str(e))
    
    return filenames

def download_report(rpt_name, ftp):
        try:
            #send a dummy operation to the ftp server to make sure we are still connected            
            ftp.voidcmd("NOOP")
        except IOError:
            print('reconnect')
            #if not reconnect
            ftp = connect_ftp() 
        
        #Set up filename and path's
        local_filename = os.path.join(r'download', rpt_name)
        try:
            lf = open(local_filename, "wb")
            #Download file
            ftp.retrbinary("RETR %s" % rpt_name, lf.write)
            lf.close() 
        except PermissionError as e:
            print('this', str(e))
        
def get_rpt_name(reports):
    for i in reports:
        rpt = i.split('_')
        if rpt[0] == 'recent-email-sent':
            return i        

def import_csv(rpt_name):
    rpt_path = os.path.join('download', rpt_name)
    db = connect_database()
    
    with open(rpt_path, 'r') as f:
        r = csv.reader(f)
        headers = next(r)

        query = 'insert into dbo.tblRecentEmailSendingSummary({0}) values ({1})'
        query = query.format(','.join(headers), ','.join('?' * len(headers)))
        
        try:
            for data in r:
                if data:
                    content = list(data[i] for i in range(len(data)))
                    parsed = parse_csv_data_types(content)
                    db.execute(query, parsed)
            db.commit()
        except IntegrityError as e:
            print(str(e))
            
        f.close()

def parse_csv_data_types(content):
    parsed = (int(content[0]),
              content[1],
              content[2],
              datetime.strptime(content[3], '%m/%d/%Y %I:%M:%S %p'),
              datetime.strptime(content[4], '%m/%d/%Y %I:%M:%S %p'),
              int(content[5]),
              int(content[6]),
              int(content[7]),
              int(content[8]),
              int(content[9]),
              int(content[10]),
              int(content[11]),
              int(content[12]),
              int(content[13]),
              int(content[14]),
              int(content[15]),
              float(content[16].replace('%', '')),
              float(content[17].replace('%', '')),
              float(content[18].replace('%', '')),
              float(content[19].replace('%', '')),
              float(content[20].replace('%', '')),
              int(content[21]),
              int(content[22]))
    return parsed

def remove_files(rpt_name, ftp):
    archive_dir = r"\\esmserver\InkPixi\ONLINE MARKETING\archive\recent_email_sending_summary"
    now = datetime.strftime(datetime.now(), '%Y-%m-%d')
    new_name = now + '-recent-email-sent-summary.csv' 
    
    #os.remove(os.path.join('download', rpt_name))
    shutil.move(os.path.join('download', rpt_name), archive_dir)
    os.rename(os.path.join(archive_dir, rpt_name), os.path.join(archive_dir, new_name))
    ftp.delete(rpt_name)

def connect_database():
    con = pyodbc.connect('DRIVER={SQL Server}; SERVER=SQLRPTSERVER\SQLREPORTS; DATABASE=OnlineMarketing; Trusted_Connection=yes')
    cur = con.cursor()
    
    return cur
        
if __name__ == '__main__':
    ftp = connect_ftp() 
    reports = get_file_list(ftp)
    rpt_name = get_rpt_name(reports)
    if rpt_name:     
        download_report(rpt_name, ftp)
        import_csv(rpt_name)
        remove_files(rpt_name, ftp)
    else:
        print('no report to download.')