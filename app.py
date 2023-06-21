#Programming quiz 3
#Jordan James 1001879608
#CSE 6332-002

import math
import pandas as pd
import os, uuid
from sqlalchemy import create_engine
import pymysql
import time
import redis
import random
import pyarrow as pa
from sqlalchemy import create_engine, update, MetaData, Table
from flask import (Flask, redirect, render_template, request,
                   send_from_directory, url_for)

app = Flask(__name__)


r = redis.Redis(
    host='6332cache.redis.cache.windows.net',
    port=6379, 
    password='O03L4nySbOtQY2Qfr7qiRIBi8EwmtQ7D5AzCaJZK7ZA=')


# Setting up database connection parameters
server = 'q2server.mysql.database.azure.com'
database = 'testdb'
username = 'servadmin'
password = '#hackme123'
port = '3306'

engine = create_engine(
    f"mysql+pymysql://{username}:{password}@{server}:{port}/{database}",
    connect_args={"ssl": {"ssl_ca": "DigiCertGlobalRootCA.crt.pem"}},
)


@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


@app.route('/count', methods=['POST'])
def count():
    # SQL query to retrieve earthquakes of a specific type and magnitude greater than a threshold
    query = '''SELECT *
    FROM cities
    WHERE idx = {}
    '''
    result = None
    arg = request.form.get('name')
    if not arg.isnumeric():
        return redirect(url_for('index'))
    arg = int(arg)
    rands = []
    times = []
    tot = 0
    for i in range(arg):
        rands.append(random.randint(0,10000))
    for i in rands:
        start = time.time()
        result = pd.read_sql_query(query.format(i), engine)
        finish = time.time()
        times.append(str(i)+": "+str(finish-start))
        tot += (finish-start)
    # Rendering the template with the query result
    
    if result is not None:
       return render_template('count.html',total = "Total Time: "+str(tot), name = times)
    else:
       print('Request for count page received with no name or blank name -- redirecting')
       
@app.route('/reg_small', methods=['POST'])
def reg_small():
    # SQL query to retrieve earthquakes of a specific type and magnitude greater than a threshold
    query = '''SELECT *
    FROM cities
    WHERE idx = {}
    '''
    result = None
    arg = request.form.get('name')
    if not arg.isnumeric():
        return redirect(url_for('index'))
    arg = int(arg)
    rands = []
    times = []
    tot = 0
    for i in range(arg):
        rands.append(random.randint(0,10))
    for i in rands:
        start = time.time()
        result = pd.read_sql_query(query.format(i), engine)
        finish = time.time()
        times.append(str(i)+": "+str(finish-start))
        tot += (finish-start)
    # Rendering the template with the query result
    
    if result is not None:
       return render_template('count.html',total = "Total Time: "+str(tot), name = times)
    else:
       print('Request for count page received with no name or blank name -- redirecting')

@app.route('/cache_all', methods=['POST'])
def cache_all():
    # SQL query to retrieve earthquakes of a specific type and magnitude greater than a threshold
    query = '''SELECT *
    FROM cities
    WHERE idx = {}
    '''
    result = None
    arg = request.form.get('name')
    if not arg.isnumeric():
        return redirect(url_for('index'))
    arg = int(arg)
    rands = []
    times = []
    tot = 0
    for i in range(arg):
        rands.append(random.randint(0,10000))
    for i in rands:
        start = time.time()
        result = pa.default_serialization_context()
        res = r.get(i)
        if res is not None:
            result.deserialize(res)
        else:
            result = pd.read_sql_query(query.format(i), engine)
            r.set(i,pa.default_serialization_context().serialize(result).to_buffer().to_pybytes())
        finish = time.time()
        times.append(str(i)+": "+str(finish-start))
        tot += (finish-start)
    # Rendering the template with the query result
    
    if result is not None:
       return render_template('count.html',total = "Total Time: "+str(tot), name = times)
    else:
       print('Request for count page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))
       
@app.route('/cache_small', methods=['POST'])
def cache_small():
    # SQL query to retrieve earthquakes of a specific type and magnitude greater than a threshold
    query = '''SELECT *
    FROM cities
    WHERE idx = {}
    '''
    result = None
    arg = request.form.get('name')
    if not arg.isnumeric():
        return redirect(url_for('index'))
    arg = int(arg)
    rands = []
    times = []
    tot = 0
    for i in range(arg):
        rands.append(random.randint(0,10))
    for i in rands:
        start = time.time()
        result = pa.default_serialization_context()
        res = r.get(i)
        if res is not None:
            result.deserialize(res)
        else:
            result = pd.read_sql_query(query.format(i), engine)
            r.set(i,pa.default_serialization_context().serialize(result).to_buffer().to_pybytes())
        finish = time.time()
        times.append(str(i)+": "+str(finish-start))
        tot += (finish-start)
    # Rendering the template with the query result
    
    if result is not None:
       return render_template('count.html',total = "Total Time: "+str(tot), name = times)
    else:
       print('Request for count page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))

if __name__ == '__main__':
   app.run()
