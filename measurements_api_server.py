#!flask/bin/python


from flask import Flask, jsonify
from pprint import pprint
from io import BytesIO
from flask import request
import json
from flask.ext.mysqldb import MySQL
import copy

app = Flask(__name__)
mysql = MySQL()
app.config['MYSQL_USER'] = ''
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'metaviz_t'
app.config['MYSQL_HOST'] = 'localhost'

mysql = MySQL(app)

providers = {"serverType": "mySQL", "url": "http://localhost:5000", "version": "1"} 
sources = {"name": 'metaviz', "description": 'hmp 1 data', "version": '2'} 

#annotations that are available with this dataset
filter_contains = {"name": "contains", "operator": "contains",  "valueType": "string", "description": "string is contained within field value", "supportsNegate": "false" }
filter_equals = {"name": "equals",  "operator": "equals", "valueType": "int", "description": "string equals field value", "supportsNegate": "false"}
filter_range = {"name": "range",  "operator": "range", "valueType": "int", "description": "field values are in this range", "supportsNegate": "true"}

field_id = {"field": "id", "type": "string", "description": "id of sample", "label": "id", "stats": {"rowCount": 1, "distinctValues": " "}, "filter": [filter_contains]}
field_sample_id = {"field": "xsampleid", "type": "string", "description": "id of sample", "label": "id", "stats": { "rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}
field_barcode = {"field": "barcodesequence", "type": "string", "description": "barcode of sample", "label": "barcodesequence", "stats": {"rowCount": 0, "distinctValues": " " }, "filter": [filter_contains]}
field_primer = {"field": "linkerprimersequence", "type": "string", "description": "primer sequence used", "label": "linkerprimersequence", "stats": {"rowCount": 0, "distinctValues": " " }, "filter": [filter_contains]}
field_title = {"field": "title", "type": "string", "description": "title of experiment", "label": "title", "stats": {"rowCount": 0, "distinctValues": " " }, "filter": [filter_contains]}
field_experiment = {"field": "experimentaccession", "type": "string", "description": "accession number of experiment", "label": "experimentaccession", "stats": {"rowCount": 0,"distinctValues": " " }, "filter": [filter_contains]}
field_sex = {"field": "sex", "type": "string", "description": "sex of the host", "label": "sex", "stats": {"rowCount": 0, "distinctValues": ["male", "female"]}, "filter": [filter_equals]}
field_country  = {"field": "country", "type": "string", "description": "country of the host", "label": "country", "stats": {"rowCount": 0, "distinctValues": " " }, "filter": [filter_contains]}
field_visitno = {"field": "visitno", "type": "int", "description": "visit number of sample", "label": "visitno", "stats": { "rowCount": 0, "distinctValues": " " }, "filter": [filter_range]}
field_bodysite = { "field": "bodysite", "type": "string", "description": "host body site of sample","label":"bodysite","stats": {"rowCount": 0, "distinctValues": " " }, "filter": [filter_contains]}
field_index = { "field": "index", "type": "int", "description": "index for hmp database", "label":"index","stats": {"rowCount": 0, "distinctValues": " " }, "filter": [filter_range]}


annotations = []
annotations.append(field_id)
annotations.append(field_sample_id)
annotations.append(field_barcode)
annotations.append(field_primer)
annotations.append(field_title)
annotations.append(field_experiment)
annotations.append(field_sex)
annotations.append(field_country)
annotations.append(field_visitno)
annotations.append(field_bodysite)

# Function for displaying data providers available with data sources
@app.route('/dataProviders', methods=['GET'])
def get_providers():
    res = jsonify({"dataProviders": providers})
    res.headers['Access-Control-Allow-Origin'] = '*'
    res.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
    return res

# Function for displaying all data sources served by this provider
@app.route('/dataSources', methods=['GET'])
def get_sources():
    res = jsonify({"dataSources": [sources]})
    res.headers['Access-Control-Allow-Origin'] = '*'
    res.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
    return res

# Function for performing SQL query to retrieve database attributes
@app.route('/annotations/<dsName>', methods=['GET'])
def get_annotations(dsName):
    dataSource = dsName
    formatRes = request.args.get('format')
    cur = mysql.connection.cursor()
    for i in range(0, len(annotations)):
        cur.execute('''SELECT COUNT(''' + annotations[i]['field'] + ''') FROM col_data''')
        rv = cur.fetchall()
        annotations[i]['stats']['rowCount'] = rv[0]
    res = jsonify({"dataSource": dataSource, "dataAnnotations": annotations})
    res.headers['Access-Control-Allow-Origin'] = '*'
    res.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
    return res

# Function for performing SQL query to retrieve measurements with filters and pagination
@app.route('/measurements/<dsName>', methods=['POST', 'OPTIONS'])
def post_measurements(dsName):
    if request.method=='OPTIONS':
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
        return res

    request.data = request.get_json()
    reqId = request.args.get('requestId')
    formatRes = request.args.get('format')
    cur = mysql.connection.cursor()
    cur.execute('''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME="col_data"''')
    rv = cur.fetchall()
    keys = (rv)

    measurements = []
    dictionary = {}
    for i in range(0, len(keys)):
       dictionary[rv[i][0]] = " "

    pageSize = str(10)
    queryStr = '''SELECT * FROM col_data '''
    whereClause = []
    whereClauseStr = ''
    if len(request.data['filter']) > 0:
        queryStr += ''' WHERE '''
        for i in range(0, len(request.data['filter'])):
            if request.data['filter'][i]['filterOperator']=="contains":
                whereClause.append(''' ''' + request.data['filter'][i]['filterField'] + ''' LIKE '%''' + request.data['filter'][i]['filterValue'] + '''%' ''')
            elif request.data['filter'][i]['filterOperator']=="equals":
                whereClause.append(''' ''' + request.data['filter'][i]['filterField'] + '''=''' + request.data['filter'][i]['filterValue'] + ''' ''')
            elif request.data['filter'][i]['filterOperator']=="range":
                rangeStr = request.data['filter'][i]['filterValue']
                rangeStrSplit = rangeStr.split(',')
                lowerBound = rangeStrSplit[0]
                upperBound = rangeStrSplit[1]
                whereClause.append(''' ''' + request.data['filter'][i]['filterField'] + ''' BETWEEN ''' + lowerBound + ''' AND ''' + upperBound)
        if i > 0:
            whereClauseStr = ''' AND '''.join(whereClause)
        else:
            whereClauseStr = ''' '''.join(whereClause)
        print(whereClauseStr)
        queryStr += whereClauseStr + ''' LIMIT ''' + str(request.data['pageSize']) + ''' '''  #+ ''',''' + request.data.pageoffset
        print(queryStr)
        cur.execute(queryStr)
    else:
        queryStr += ''' LIMIT ''' + pageSize + ''' '''
        cur.execute(queryStr)
    rv = cur.fetchall()
    for j in range(0, len(rv)):
       measurements.append(copy.deepcopy(dictionary))
       for k in range(0, len(rv[j])):
          measurements[j][keys[k][0]] = rv[j][k]

    annotations[0]['field'] = str(rv)

    res = jsonify({"dataMeasurements": measurements})
    res.headers['Access-Control-Allow-Origin'] = '*'
    res.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
    return res


if __name__ == '__main__':
    app.run(debug=True)

