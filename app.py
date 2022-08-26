from flask import Flask, render_template, request, redirect, url_for, send_file
from requests.structures import CaseInsensitiveDict

from datetime import datetime
from datetime import timedelta
import json, pandas as pd
from pandas.io.json import json_normalize
from requests.auth import HTTPBasicAuth
import requests, zipfile,os
import snowflake.connector
from os import listdir
from tag_names import taglist


def call_api(tag,tc,str, end, interval=6000):    
    if interval=="1min":
        interval_value = "60000"
    elif interval== "5min":
        interval_value = "300000"
    
    url = "http://10.255.110.209:8080/historian-rest-api/v1/datapoints/trend?tagNames="+tag+"&start="+str+"&end="+end+"&samplingMode=6&calculationMode=1&direction=0&intervalMs="+interval_value
    print(url)
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"  
    headers["Authorization"] = "Bearer "+tc
    print(tag,str, end)
    resp = requests.get(url, headers=headers)
    #print("response is "+resp.text)
    with open(f'{tag}.csv', "a+") as write_file:
        df=pd.json_normalize(json.loads(resp.text),record_path=['Data', 'Trend'])        
        print("inside file open")
        df1 = pd.Series([ x for x in df.to_dict().values()])
        for i in (df1[0]):
            pass
            #print((df1[0][i]['Timestamp']))
                #if df1[0][i]['Value']!=0:
                #print(f"{tag},{df1[0][i]['Timestamp']},{df1[0][i]['Value']}\n")
            write_file.write(f"{tag},{df1[0][i]['Timestamp']},{df1[0][i]['Value']}\n")
                #write_file.flush()
    #write_file.flush()

def call_historian(tag,interval,start_date,end_date):
    print('called historian url')
    #tag_list=['845_BLN_AI_P_FE10_02_01']
    #tag_list=tag.split(",")
    
    n = 3 # Days to increment. This will pull less than 5000 data points.
    total_days = 365 # Default total days to download if end_date is not set(min 2)
    response = requests.get("http://10.255.110.209:8080/uaa/oauth/token?grant_type=client_credentials", auth = HTTPBasicAuth('admin', 'plcAdmin_845'))
    oauth_token = response.json()["access_token"]
    tc = oauth_token
    # tc= tc.token
    #tc ='ds'
    date_format = '%Y-%m-%dT%H:%M:%S'
    #start_date = '2015-10-01T00:00:00'
    #end_date = '2015-10-05T00:00:00'
    dtObj = datetime.strptime(start_date, date_format)
    ftObj= datetime.strptime(end_date, date_format)
    future_date = dtObj + timedelta(days=n)
    base = start_date
    total_days = (ftObj - dtObj).days
    date_list = [dtObj + timedelta(days=x) for x in range(0,total_days,n)]
    date_list.append(ftObj)
    #print((date_list))
    
    if tc:
        print("token created"+ tc)
    #for tag in tag_list:
        for x in range(0,len(date_list)-1):
            print("Running:",tag,date_list[x], date_list[x+1])
            call_api(tag,tc,str(date_list[x].strftime(date_format)),str(date_list[x+1].strftime(date_format)), interval)


app = Flask(__name__)
@app.route('/')
def index():
   #print('Request for index page received')
   return render_template('index.html', taglist = taglist)

#@app.route('/favicon.ico')
#def favicon():
#    return send_from_directory(os.path.join(app.root_path, 'static'),
#                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/status', methods=['POST'])
def status():
   tag = request.form.get('tag')
   interval = request.form.get('interval')
   start_date  = request.form.get('start_date')
   end_date  = request.form.get('end_date')
   
  

   if tag:
       for i in tag.split(','):
           print('Request for hello page received with name=%s' % i)
           print('Request for hello page received with name=%s' % interval)
           print('Request for hello page received with name=%s' % start_date)
           print('Request for hello page received with name=%s' % end_date)
           print(i)
           call_historian(i,interval,start_date,end_date)
       return render_template('status.html', tag = tag, interval=interval, start_date=start_date, end_date=end_date )
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))

@app.route('/snowflake', methods=['POST'])
def upload():
    print('calling snowflake upload')
    # Gets the version
    ctx = snowflake.connector.connect(
   # authenticator='externalbrowser',
    user='SRV_ADF',
    password= 'hMCp6}@;/FbJ\^R6kbVDE3P.',
    account='cmc.west-us-2.azure'
    )
    cs = ctx.cursor()
    cs.execute("use database raw")
    cs.execute("use schema HISTORIAN_DEV")
    WAREHOUSE = 'LOADING_WH'
    DATABASE = 'RAW'
    SCHEMA = 'historian_dev'
    path_to_dir ="."
    filenames = listdir(path_to_dir)     
    try:
        for filename in filenames:
            if filename.endswith( ".csv" ):
                pass
                cs.execute("put file://"+filename+"  @%tag_data")
                cs.execute("copy into tag_data from @%tag_data FILE_FORMAT = ( TYPE = CSV )");
                #cs.execute("@%tag_data/"+filename)
    finally:
        cs.close()
    ctx.close()
    return redirect(url_for('index'))


@app.route('/download', methods=['GET'])
def download():
    zipf = zipfile.ZipFile('Name.zip','w', zipfile.ZIP_DEFLATED)
    for root,dirs, files in os.walk('.'):
         for file in files:
             if file.endswith( "test.csv" ):
                  zipf.write(file)
    zipf.close()
    return send_file('Name.zip',
            mimetype = 'zip',
            attachment_filename= 'Name.zip',
            as_attachment = True)

if __name__ == '__main__':
    app.run()