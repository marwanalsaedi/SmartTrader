from crypto_class import Crypto,timestamp_to_datetime,timestamp_to_date,date_to_timestamp,datetime_to_timestamp
import os
from flask import Flask, request, json
from google.cloud import pubsub_v1
from typing import Dict
import pandas as pd
from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value

project_id = "########"
topic_id = "cryptos"
TOKEN='hunter2'

ml_project="########"
ml_endpoint_id="#########"

cr = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS','serviceacc.json')
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
coin=None

app = Flask(__name__)
@app.route("/", methods=['GET', 'POST'])
def p():
    
    if request.headers['TOKEN'] != TOKEN:
        return 'Invalid Token',403
    
    data = request.json
    

    symbol = data['symbol']
    date = data['start_date']
    if len(data) >= 3:
        sql_address=data['sql_address']
    else:
        sql_address="10.54.160.2"

    coin=Crypto(symbol,date_to_timestamp(date),sql_server_address=sql_address)
    coin.fetch()
    coin.update()
    last_update=coin.return_df().tail(1).astype(pd.StringDtype())
    last_update=last_update.to_dict('records')

    pred_input=list(last_update)
 
    pred=predict(ml_project,ml_endpoint_id,last_update)

    published_message =f'{{"symbol": "{symbol}","current_price": "{last_update[0]["close"]}","predictions": "{pred[0]["value"]}"}}'
    
    publish(published_message.encode('utf-8'))
    print(published_message)
    
    return 'OK',200

def publish(data):


    future = publisher.publish(topic_path, data)
    print(future.result())


def predict(
    project: str,
    endpoint_name: str,
    instances: list[Dict],
    location: str = "us-central1",
):
    aiplatform.init(project=project, location=location)

    endpoint = aiplatform.Endpoint(endpoint_name)

    response = endpoint.predict(instances=instances)

    return response.predictions



if __name__ == '__main__':

    server_port = os.environ.get('PORT', '5000')
    app.run(port=server_port, host='0.0.0.0')

   