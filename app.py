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
    # Retrieving data from the request form
    arg = request.form.get('name')
    args = arg.split(',')
    
    sf = float(args[0])
    nf = float(args[2])
    wl = float(args[1])
    el = float(args[3])
    
    # SQL query to retrieve earthquakes within the specified boundaries
    query = '''SELECT *
    FROM cities
    WHERE lat BETWEEN {} AND {}
    AND
    lon BETWEEN {} AND {}
    '''
        
    start = time.time()
    # Executing the SQL query based on different boundary scenarios
    if sf < nf and el < wl:
        result = pd.read_sql_query(query.format(sf,nf,el,wl), engine)
    elif sf < nf and wl < el:
        result = pd.read_sql_query(query.format(sf,nf,wl,el), engine)
    elif nf < sf and wl < el:
        result = pd.read_sql_query(query.format(nf,sf,wl,el), engine)
    elif nf < sf and el < wl:
        result = pd.read_sql_query(query.format(nf,sf,el,wl), engine)          
        
    finish = time.time()   
    tot = finish - start
    # Rendering the template with the query result
    if result is not None:
       return render_template('count.html', total = tot, tables=[result.to_html(classes='data', header="true")])
    else:
       print('Request for count page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))
       
@app.route('/reg_small', methods=['POST'])
def reg_small():
    # Retrieving data from the request form
    arg = request.form.get('name')
    args = arg.split(',')
    
    sf = float(args[0])
    nf = float(args[2])
    wl = float(args[1])
    el = float(args[3])
    
    # SQL query to retrieve earthquakes within the specified boundaries
    query = '''SELECT *
    FROM cities
    WHERE lat BETWEEN {} AND {}
    AND
    lon BETWEEN {} AND {}
    '''
    
    
    
    start = time.time()
    result = pa.default_serialization_context()
    res = r.get(arg)
    if res is not None:
        result.deserialize(res)
    else:
        # Executing the SQL query based on different boundary scenarios
        if sf < nf and el < wl:
            result = pd.read_sql_query(query.format(sf,nf,el,wl), engine)
        elif sf < nf and wl < el:
            result = pd.read_sql_query(query.format(sf,nf,wl,el), engine)
        elif nf < sf and wl < el:
            result = pd.read_sql_query(query.format(nf,sf,wl,el), engine)
        elif nf < sf and el < wl:
            result = pd.read_sql_query(query.format(nf,sf,el,wl), engine)          
        r.set(arg,pa.default_serialization_context().serialize(result).to_buffer().to_pybytes())

    finish = time.time()   
    tot = finish - start
    if sf < nf and el < wl:
        result = pd.read_sql_query(query.format(sf,nf,el,wl), engine)
    elif sf < nf and wl < el:
        result = pd.read_sql_query(query.format(sf,nf,wl,el), engine)
    elif nf < sf and wl < el:
        result = pd.read_sql_query(query.format(nf,sf,wl,el), engine)
    elif nf < sf and el < wl:
        result = pd.read_sql_query(query.format(nf,sf,el,wl), engine)
    # Rendering the template with the query result
    if result is not None:
       return render_template('count.html', total = tot, tables=[result.to_html(classes='data', header="true")])
    else:
       print('Request for count page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))

@app.route('/increment', methods=['POST'])
def increment():
    # Retrieving data from the request form
    result = None
    arg = request.form.get('name')
    args = arg.split(',')
    args[1] = int(args[1])
    args[2] = int(args[2])
    args[3] = int(args[3])
    query = '''SELECT *
    FROM cities
    WHERE State = {}
    AND Population BETWEEN {} AND {}
    '''
    
    # Table name, field to increment, and population range
    metadata = MetaData(bind=engine)
    table_name = 'cities'
    table = Table(table_name, metadata, autoload=True)
    field_to_increment = 'Population'
    min_population = args[1]
    max_population = args[2]

    
    # Construct the update query
    stmt = (
        update(table)
        .where(table.c.Population.between(args[0],min_population, max_population))
        .values({field_to_increment: table.c.Population + args[3]})  
    )
    start = time.time()

    # Execute the update query
    with engine.begin() as connection:
        connection.execute(stmt)
    finish = time.time()
    time = finish-start
    result = pd.read_sql_query(query.format(args[0],min_population,max_population), engine)
    
    # Rendering the template with the query result
    if result is not None:
       return render_template('count.html', total = time, tables=[result.to_html(classes='data', header="true")])
    else:
       print('Request for count page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))

@app.route('/cache_all', methods=['POST'])
def cache_all():
    # Retrieving data from the request form
    arg = request.form.get('name')
    args = arg.split(',')
    
    sf = float(args[0])
    nf = float(args[2])
    wl = float(args[1])
    el = float(args[3])
    
    # SQL query to retrieve earthquakes within the specified boundaries
    query = '''SELECT *
    FROM cities
    WHERE lat BETWEEN {} AND {}
    AND
    lon BETWEEN {} AND {}
    '''
        
    # Executing the SQL query based on different boundary scenarios
    if sf < nf and el < wl:
        result = pd.read_sql_query(query.format(sf,nf,el,wl), engine)
    elif sf < nf and wl < el:
        result = pd.read_sql_query(query.format(sf,nf,wl,el), engine)
    elif nf < sf and wl < el:
        result = pd.read_sql_query(query.format(nf,sf,wl,el), engine)
    elif nf < sf and el < wl:
        result = pd.read_sql_query(query.format(nf,sf,el,wl), engine)          

    rands = []
    times = []
    tot = 0
    idxs = []
    res = None
    query = '''SELECT *
    FROM cities
    WHERE idx = {}
    '''
    print(int(args[4]))
    for ro in result['idx']:
        idxs.append(ro) 
    for i in range(int(args[4])):
        rands.append(random.randint(min(idxs),max(idxs)))
    for j,i in enumerate(rands):
        start = time.time()
        res = pd.read_sql_query(query.format(i), engine)
        finish = time.time()
        tot += (finish-start)
        times.append(str(finish-start))
    # Rendering the template with the query result
    red = None
    for i in rands:
        if red is None:
            red = pd.read_sql_query(query.format(i), engine)
        else:
            rett = pd.read_sql_query(query.format(i), engine)
            red = pd.concat([red,rett], ignore_index=True)
            
    # Rendering the template with the query result
    #result = res
    result = red
    
    if result is not None:
       return render_template('count.html',total = "Total Time: "+str(tot), times = times, tables=[result.to_html(classes='data', header="true")])
    else:
       print('Request for count page received with no name or blank name -- redirecting')
       
@app.route('/cache_small', methods=['POST'])
def cache_small():
    # Retrieving data from the request form
    arg = request.form.get('name')
    args = arg.split(',')
    
    sf = float(args[0])
    nf = float(args[2])
    wl = float(args[1])
    el = float(args[3])
    
    # SQL query to retrieve earthquakes within the specified boundaries
    query = '''SELECT *
    FROM cities
    WHERE lat BETWEEN {} AND {}
    AND
    lon BETWEEN {} AND {}
    '''
        
    # Executing the SQL query based on different boundary scenarios
    if sf < nf and el < wl:
        result = pd.read_sql_query(query.format(sf,nf,el,wl), engine)
    elif sf < nf and wl < el:
        result = pd.read_sql_query(query.format(sf,nf,wl,el), engine)
    elif nf < sf and wl < el:
        result = pd.read_sql_query(query.format(nf,sf,wl,el), engine)
    elif nf < sf and el < wl:
        result = pd.read_sql_query(query.format(nf,sf,el,wl), engine)          

    rands = []
    times = []
    tot = 0
    idxs = []
    res = None
    query = '''SELECT *
    FROM cities
    WHERE idx = {}
    '''
    print(int(args[4]))
    for ro in result['idx']:
        idxs.append(ro) 
    for i in range(int(args[4])):
        rands.append(random.randint(min(idxs),max(idxs)))
    for j,i in enumerate(rands):
        start = time.time()
        if j > 0:
            results = pa.default_serialization_context()
            res = r.get(i)
            if res is not None:
                results.deserialize(res)
            else:
                res = pd.read_sql_query(query.format(i), engine)
                r.set(i,pa.default_serialization_context().serialize(res).to_buffer().to_pybytes())
        else:
            results = pa.default_serialization_context()
            res = r.get(i)
            if res is not None:
                results.deserialize(res)
            else:
                ret = pd.read_sql_query(query.format(i), engine)
                r.set(i,pa.default_serialization_context().serialize(ret).to_buffer().to_pybytes())
            #res = pd.concat([res,ret], ignore_index=True)
        finish = time.time()
        tot += (finish-start)
        times.append(str(finish-start))
    # Rendering the template with the query result
    red = None
    for i in rands:
        if red is None:
            red = pd.read_sql_query(query.format(i), engine)
        else:
            rett = pd.read_sql_query(query.format(i), engine)
            red = pd.concat([red,rett], ignore_index=True)
            
    # Rendering the template with the query result
    #result = res
    result = red
    
    if result is not None:
       return render_template('count.html',total = "Total Time: "+str(tot), times = times, tables=[result.to_html(classes='data', header="true")])
    else:
       print('Request for count page received with no name or blank name -- redirecting')
if __name__ == '__main__':
   app.run()
