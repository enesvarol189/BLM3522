import ssl
import time
import json
import random
from datetime import datetime
import paho.mqtt.client as mqtt

AWS_ENDPOINT = "avsc71rx6zfq1-ats.iot.us-east-1.amazonaws.com"
PORT = 8883
TOPIC = "smartcity/devices/data"

CA_PATH = ""
CERT_PATH = ""
KEY_PATH = ""

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

client = mqtt.Client(client_id="iot-simulator-01")
client.on_connect = on_connect

client.tls_set(
    ca_certs=CA_PATH,
    certfile=CERT_PATH,
    keyfile=KEY_PATH,
    tls_version=ssl.PROTOCOL_TLSv1_2,
)

client.connect(AWS_ENDPOINT, PORT, keepalive=60)
client.loop_start()

try:
    while True:
        timestamp = time.time()

        payload = {
            "data": {
                "co": round(random.uniform(0.0, 0.02), 8),
                "humidity": round(random.uniform(30.0, 70.0), 2),
                "light": random.choice([True, False]),
                "lpg": round(random.uniform(0.0, 0.02), 8),
                "motion": random.choice([True, False]),
                "smoke": round(random.uniform(0.0, 0.05), 8),
                "temp": round(random.uniform(60.0, 100.0), 2),
            },
            "device_id": random.choice([
                "00:0f:00:70:91:0a",
                "1c:bf:ce:15:ec:4d",
                "b8:27:eb:bf:9d:51"
            ]),
            "ts": timestamp
        }

        print("Publishing:", payload)
        client.publish(TOPIC, json.dumps(payload), qos=1)
        time.sleep(5)

except KeyboardInterrupt:
    print("Stopped by user.")
    client.loop_stop()
    client.disconnect()