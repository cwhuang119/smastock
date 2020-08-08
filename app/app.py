from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask import request,Response
import json
import datetime
app = Flask(__name__)
import os
db_path = os.path.abspath('../data/data.db')
db_connection_str = f'sqlite:///{db_path}'
print('db connection string',db_connection_str)
app.config['SQLALCHEMY_DATABASE_URI'] = db_connection_str
db = SQLAlchemy(app)





@app.route('/')
def index():
    sql_cmd=request.data.decode('utf-8')
    print(sql_cmd)
    query_data = db.engine.execute(sql_cmd).fetchall()
    r = json.dumps([dict(x) for x in query_data],indent=4)
    return Response(r,mimetype='application/json')
    
@app.route('/price',methods=['GET'])
def get_price():
    if 'stock_id' in request.args:
        stock_id = request.args['stock_id']
        sql_cmd = f'select * from price where stock_id="{stock_id}"'
        query_data = db.engine.execute(sql_cmd).fetchall()
        r = json.dumps([dict(x) for x in query_data],indent=4)
    return Response(r,mimetype='application/json')
@app.route('/price',methods=['POST'])
def insert_price():
    data = request.json
    insert_values = data['insert_values']
    sql_cmd =data['sql_cmd']
    for insert_value in insert_values:
        print(sql_cmd+insert_value)
        db.engine.execute(sql_cmd+insert_value)
    return Response({'status':'ok'},mimetype='application/json')




if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8181,debug=True)