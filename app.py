import decimal
import json
import time
from datetime import datetime
from uuid import UUID

import mongotl
import dataframetl
import sparktl
import bson
from flask import Flask, render_template, jsonify, request, url_for
from config import configs
from sqlalchemy import create_engine
from SQLTool import *

app = Flask(__name__)
app.config.from_object(configs.get('development'))
engine = create_engine(app.config.get('SQLALCHEMY_DATABASE_URI'))

columns_type_dict = {}

JSONEncoder_olddefault = json.JSONEncoder.default
def JSONEncoder_newdefault(self, o):
    if isinstance(o, UUID): return str(o)
    if isinstance(o, datetime): return str(o)
    if isinstance(o, time.struct_time): return datetime.fromtimestamp(time.mktime(o))
    if isinstance(o, decimal.Decimal): return str(o)
    return JSONEncoder_olddefault(self, o)
json.JSONEncoder.default = JSONEncoder_newdefault


@app.route('/queryTL', methods=['GET', 'POST'])
def queryTL():
    type = request.form['type']
    sql_dict = json.loads(request.form['sql_dict'])
    columns_type_dict = getColumnsType(engine, sql_dict)
    if type == 'MongoDB':
        result = mongotl.query(columns_type_dict, sql_dict)
        result = json.dumps(result)
    elif type == 'Dataframe':
        result = dataframetl.query(sql_dict)
    elif type == 'Spark':
        result = sparktl.query(sql_dict)
    elif type == 'MapView':
        result = getwhere(engine, sql_dict)
    else:
        result = None
    return jsonify({'tl_result': result}) #, default=str


@app.route('/translate', methods=['GET', 'POST'])
def translate():
    tl_type = request.form['type']
    sql_dict = json.loads(request.form['sql_dict'])
    if tl_type == 'MongoDB':
        columns_type_dict = getColumnsType(engine, sql_dict)
        pipeline = mongotl.translate(columns_type_dict, sql_dict)
        if len(pipeline) == 0:
            result = 'db.{}.aggregate('.format(sql_dict['table']) + ')'
        else:
            result = 'db.{}.aggregate('.format(sql_dict['table']) + json.dumps(pipeline) + ')'
    elif tl_type == 'Dataframe':
        result = dataframetl.translate(sql_dict)
    elif tl_type == 'Spark':
        result = sparktl.translate(sql_dict)
    elif tl_type == 'MapView':
        result = getwhere(engine, sql_dict)
    else:
        result = ''

    return jsonify({'tl': result})


@app.route('/query', methods=['GET', 'POST'])
def query():
    db = request.form['db']
    sql = request.form['sql']
    engine.execute('use '+db)
    # print({'data': getQueryResult(engine, sql)})
    # return jsonify({'data': getQueryResult(engine, sql)})
    # result = json.dumps(getQueryResult(engine, db, sql), default=str, use_decimal=True)
    # return jsonify(json.loads(result))
    return jsonify(getQueryResult(engine, db, sql))


@app.route('/getSql', methods=['GET', 'POST'])
def getSql():
    # db = request.form['db']
    sql_dict = request.form['sql_dict']
    print(sql_dict)
    # return jsonify({'data': getQueryResult(engine, sql)})
    return jsonify({'sql': concatSQL(engine, json.loads(sql_dict))})


@app.route('/getDbs')
def getDbs():
    return jsonify({'dbs': getMySQLDatabases(engine)})


@app.route('/getTables', methods=['GET', 'POST'])
def getTables():
    db = request.form['db']
    return jsonify({'tables': getAllTables(engine, db)})


@app.route('/getAttrs', methods=['GET', 'POST'])
def getAttrs():
    db = request.form['db']
    tables = request.form.getlist('tables[]')
    # print('getattributes')
    # print(getAttributes(engine, db, tables))
    return jsonify({'attributes': getAttributes(engine, db, tables)})


@app.route('/getFkTables', methods=['GET', 'POST'])
def getFkTables():
    db = request.form['db']
    table = request.form['table']
    return jsonify({'fk_tables': getAllFKTables(engine, db, table)})


@app.route('/getFks', methods=['GET', 'POST'])
def getFks():
    db = request.form['db']
    return jsonify({'fks': getAllFK(engine, db)})

# @app.oute('/getMapUrl', methods=['GET', 'POST'])
# def mapUrl():


@app.route('/')
def index():
    dbs = getMySQLDatabases(engine)
    return render_template('index.html', dbs=dbs)


if __name__ == '__main__':
    app.run()