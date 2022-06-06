from flask import Flask, render_template, request, redirect, url_for, send_from_directory
from requests.structures import CaseInsensitiveDict

from datetime import datetime
from datetime import timedelta
import json, pandas as pd
from pandas.io.json import json_normalize
from requests.auth import HTTPBasicAuth
import requests, os



def call_api(tag,tc,str, end):    
    url = "http://10.255.110.209:8080/historian-rest-api/v1/datapoints/trend?tagNames="+tag+"&start="+str+"&end="+end+"&samplingMode=6&calculationMode=1&direction=0&intervalMs=60000"
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"  
    headers["Authorization"] = "Bearer "+tc
    resp = requests.get(url, headers=headers)
    #print("response is "+resp.text)
    with open(f'{tag}_test.csv', "a+") as write_file:
        df=pd.json_normalize(json.loads(resp.text),record_path=['Data', 'Trend'])        
        print("inside file open")
        df1 = pd.Series([ x for x in df.to_dict().values()])
        for i in (df1[0]):
            print((df1[0][i]['Timestamp']))
            #if df1[0][i]['Value']!=0:
            #print(f"{tag},{df1[0][i]['Timestamp']},{df1[0][i]['Value']}\n")
            write_file.write(f"{tag},{df1[0][i]['Timestamp']},{df1[0][i]['Value']}\n")
            #write_file.flush()
    #write_file.flush()

def call_historian(tag,interval,start_date,end_date):
    print('called historian url')
    #tag_list=['845_BLN_AI_P_FE10_02_01']
    tag_list=tag.split(",")
    
    n = 3 # Days to increment. This will pull less than 5000 data points.
    total_days = 365 # Default total days to download if end_date is not set(min 2)
    #response = requests.get("http://10.255.110.209:8080/uaa/oauth/token?grant_type=client_credentials", auth = HTTPBasicAuth('admin', 'plcAdmin_845'))
    #oauth_token = response.json()["access_token"]
    #tc = oauth_token
    # tc= tc.token
    tc =''
    date_format = '%Y-%m-%dT%H:%M:%S'
    #start_date = '2015-10-01T00:00:00'
    #end_date = '2015-10-05T00:00:00'
    dtObj = datetime.strptime(start_date, date_format)
    ftObj= datetime.strptime(end_date, date_format)
    future_date = dtObj + timedelta(days=n)
    base = start_date
    total_days = (ftObj - dtObj).days
    date_list = [dtObj + timedelta(days=x) for x in range(0,total_days,n)]
    if tc:
        print("token created")
    for tag in tag_list:
        for x in range(0,len(date_list)-1):
            print("Running:",tag,date_list[x], date_list[x+1])
            #call_api(tag,tc,str(date_list[x].strftime(date_format)),str(date_list[x+1].strftime(date_format)))


app = Flask(__name__)
@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/hello', methods=['POST'])
def hello():
   tag = request.form.get('tag')
   interval = request.form.get('interval')
   start_date  = request.form.get('start_date')
   end_date  = request.form.get('end_date')
   print(tag)
   #call_historian(tag,interval,start_date,end_date)

   if tag:
       for i in tag.split(','):
           print('Request for hello page received with name=%s' % i)
           print('Request for hello page received with name=%s' % interval)
           print('Request for hello page received with name=%s' % start_date)
           print('Request for hello page received with name=%s' % end_date)
           call_historian(i,interval,start_date,end_date)
       return render_template('hello.html', tag = tag, interval=interval, start_date=start_date, end_date=end_date )
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))



if __name__ == '__main__':
    app.run()