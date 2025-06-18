import boto3
from boto3.dynamodb.conditions import Key
import matplotlib.pyplot as plt
from datetime import datetime

aws_access_key_id = ""
aws_secret_access_key = ""
aws_region = "us-east-1"

table_name = "SensorTelemetry"

dynamodb = boto3.resource(
    'dynamodb',
    region_name=aws_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

table = dynamodb.Table(table_name)

# device_id = "1c:bf:ce:15:ec:4d"

# response = table.query(
#     KeyConditionExpression=Key('device_id').eq(device_id)
# )

response = table.scan()

def convert_item(item):
    payload = item['payload']
    return {
        "lpg": float(payload["lpg"]),
        "temp": float(payload["temp"]),
        "device_id": payload["device_id"],
        "motion": payload["motion"],
        "light": payload["light"],
        "smoke": float(payload["smoke"]),
        "humidity": float(payload["humidity"]),
        "co": float(payload["co"]),
        "ts": float(payload["ts"])
    }

items = [convert_item(item) for item in response['Items']]
items.sort(key=lambda x: x['ts'])

timestamps = [datetime.utcfromtimestamp(float(item['ts'])) for item in items]
temperatures = [item['temp'] for item in items]
humidities = [item['humidity'] for item in items]

plt.figure(figsize=(12, 6))

plt.subplot(2, 1, 1)
plt.plot(timestamps, temperatures, label='Temperature (F)', color='tomato')
plt.xlabel('Time')
plt.ylabel('Temperature (°F)')
plt.title('Temperature Over Time')
plt.grid(True)

plt.subplot(2, 1, 2)
plt.plot(timestamps, humidities, label='Humidity (%)', color='skyblue')
plt.xlabel('Time')
plt.ylabel('Humidity (%)')
plt.title('Humidity Over Time')
plt.grid(True)

plt.tight_layout()
plt.show()
