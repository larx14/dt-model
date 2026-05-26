try:
    import paho.mqtt.client as mqtt
except ModuleNotFoundError:
    print("Error: The 'paho-mqtt' package is not installed. Please install it using 'pip install paho-mqtt' and try again.")
    exit(1)

import json

# MQTT broker details
BROKER = "mqtt.iot-lab.utwente.nl"  # Change this to your broker's address
PORT = 1883  # Change this if your broker uses a different port
TOPIC = "trackers/HTIT_51/#"  # Change this to your desired topic Change HTIT_4 to you own user
USERNAME = "HTIT_51"  # Change this to your username
PASSWORD = "cubG38j0Lraj"  # Change this to your password

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(TOPIC)
    else:
        print(f"Connection failed with code {rc}")

# Here you can also put code that reacts to messages
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        print(f"Received message on topic '{msg.topic}':\n{json.dumps(payload, indent=2)}")
    except json.JSONDecodeError:
        print(f"Received non-JSON message on topic '{msg.topic}':", msg.payload.decode("utf-8"))

# Create MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(BROKER, PORT, 60)

# Start the MQTT loop to process messages
client.loop_forever()
