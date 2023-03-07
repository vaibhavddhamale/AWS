"""
Designed and maintained by : <bhupendra/bhupendra.jmd@gmail.com>
Developed for : PGDESD and PGDIoT students of CDAC, ACTS, Pune
Batch Name : September 2022
pip install python-dotenv
https://www.eclipse.org/paho/index.php?page=clients/python/docs/index.php
https://docs.aws.amazon.com/iot/latest/developerguide/iot-ddb-rule.html
pip install paho-mqtt
"""
import time,os
import paho.mqtt.client as mqtt
import ssl
import json
import threading
import time
#Section for reading details from the .env file
from dotenv import load_dotenv
load_dotenv()

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

root_ca = os.getenv('ROOT_CA_FILE_PATH')
device_certificate=os.getenv('DEVICE_CERTIFICATE_FILE_PATH')
private_key=os.getenv('PRIVATE_KEY_FILE_PATH')
aws_iot_end_point = os.getenv('AWS_IOT_END_POINT')
keep_alive_interval = int(os.getenv('KEEP_ALIVE_INTERVAL'))
broker_secure_port_number = int(os.getenv("MQTT_SECURE_PORT_NUMBER"))
sensor_sample_interval = int(os.getenv("SENSOR_SAMPLE_INTERVAL"))
client = mqtt.Client()
client.on_connect = on_connect
client.tls_set(ca_certs=root_ca, certfile=device_certificate, keyfile=private_key, tls_version=ssl.PROTOCOL_SSLv23)
client.tls_insecure_set(True)
client.connect(aws_iot_end_point,broker_secure_port_number, keep_alive_interval) #Taken from REST API endpoint - Use your own. 


def send_data_to_aws_iot(sensor_data_telemetry):
    try:
        while (1):    
            print("Sending Data comming from ESP32") 
            print(sensor_data_telemetry)
            client.publish("d/desd_device_02/telemetry", payload=json.dumps(sensor_data_telemetry) , qos=0, retain=False)
            time.sleep(sensor_sample_interval)
    except Exception as e:
        print("Exception occured in sending data ",e)

#sample payload
sensor_data_telemetry = {
                "temperature" : 30,
                "humidity" : 87
            } 
device_1_telemetry = threading.Thread(target=send_data_to_aws_iot, args=(sensor_data_telemetry,))
device_1_telemetry.start()
device_1_telemetry.join()    

"""
{
 "device_id": "desd_device_02",
 "ts": "1678096042604",
 "payload": {
  "humidity": 87,
  "temperature": 30
 }
}
SELECT * FROM 'd/+/telemetry/data'
"""