# libraries 
# create virtual env using this libs

import sys
import json
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import datetime as dt

THE_BROKER = "HOST" # go to integration-MQTT-public address
THE_TOPIC = "#" # get all uplink payload
TTN_USERNAME = "APPID" # application name 
TTN_PASSWORD = "PASSWORD" # go to integration-MQTT-password generate new API Key and use it here

db_client = InfluxDBClient(host='HOST', port=8086) 
db_client.create_database('DBNAME')
db_client.switch_database('DBNAME')

# client-server exchange 

def on_connect(client, userdata, flags, rc):
    print("Connected to ", client._host, "port: ", client._port)
    print("Flags: ", flags, "return code: ", rc)
    client.subscribe(THE_TOPIC)

# The callback for when a PUBLISH message is received from the server.

def on_message(client, userdata, message):
    msg = json.loads(message.payload)
    print(msg)

    # The below influx fields are based on my type of 
    # Adjust to suit you

    influxdb_entry = {}
    influxdb_entry['time'] = msg["uplink_message"]["rx_metadata"][0]["time"]

    fields = {}
    fields['device_id'] = msg["end_device_ids"]["device_id"]
    fields['application_ids'] = msg["end_device_ids"]["application_ids"]["application_id"]
    fields['dev_eui'] = msg["end_device_ids"]["dev_eui"]
    fields['dev_addr'] = msg["end_device_ids"]["dev_addr"]
    
    fields['received_at'] = msg["received_at"]

    fields['frame_counter'] = int(msg["uplink_message"]["f_cnt"])
    fields['port'] = int(msg["uplink_message"]["f_port"])
    
    fields['frm_payload'] = msg["uplink_message"]["frm_payload"]

    fields['frequency'] = float(msg["uplink_message"]["settings"]["frequency"])
    fields['rssi'] = float(msg["uplink_message"]["rx_metadata"][0]["rssi"])
    fields['snr'] = float(msg["uplink_message"]["rx_metadata"][0]["snr"])

    fields['ts'] = float(msg["uplink_message"]["rx_metadata"][0]["timestamp"])
    
    fields['gweui'] = msg["uplink_message"]["rx_metadata"][0]["gateway_ids"]["eui"]
    fields['gateway_id'] = msg["uplink_message"]["rx_metadata"][0]["gateway_ids"]["gateway_id"]

    fields['channel_rssi'] = float(msg["uplink_message"]["rx_metadata"][0]["channel_rssi"])
    fields['uplink_token'] = msg["uplink_message"]["rx_metadata"][0]["uplink_token"]

    fields['lat'] = float(msg["uplink_message"]["locations"]["user"]["latitude"])
    fields['lon'] = float(msg["uplink_message"]["rx_metadata"][0]["location"]["longitude"])
    
    fields['bandwidth'] = float(msg["uplink_message"]["settings"]["data_rate"]["lora"]["bandwidth"])
    fields['spreading_factor'] = int(msg["uplink_message"]["settings"]["data_rate"]["lora"]["spreading_factor"])
    fields['coding_rate'] = msg["uplink_message"]["settings"]["coding_rate"]
    fields['consumed_airtime'] = msg["uplink_message"]["consumed_airtime"]
    
    fields['x'] = msg["uplink_message"]["decoded_payload"]["bytes"][0]
    fields['y'] = msg["uplink_message"]["decoded_payload"]["bytes"][1]
    fields['z'] = msg["uplink_message"]["decoded_payload"]["bytes"][2]

    influxdb_entry['fields'] = fields
    influxdb_entry['measurement'] = 'agri_data'
    influxdb_entry['tags'] = {'sensor': msg["end_device_ids"]["dev_eui"]}
    print(influxdb_entry)
    db_client.write_points([influxdb_entry])



client = mqtt.Client()

# check for username

if TTN_USERNAME == 'VOID':
    print("You must set the values of your app and device first!!")
    sys.exit()
client.username_pw_set(TTN_USERNAME, password=TTN_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.connect(THE_BROKER, 1883, 60)
client.loop_forever()
