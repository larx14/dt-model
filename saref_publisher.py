try:
    import paho.mqtt.client as mqtt
except ModuleNotFoundError:
    print("Error: The 'paho-mqtt' package is not installed. Please install it using 'pip install paho-mqtt' and try again.")
    exit(1)

try:
    from kafka import KafkaProducer
except ModuleNotFoundError:
    print("Error: The 'kafka-python' package is not installed. Please install it using 'pip install kafka-python' and try again.")
    exit(1)

import json
import time
import threading
import random
from datetime import datetime

# MQTT broker details
MQTT_BROKER = "mqtt.iot-lab.utwente.nl"
MQTT_PORT = 1883
MQTT_TOPIC = "trackers/HTIT_51/#"
MQTT_USERNAME = "HTIT_51"
MQTT_PASSWORD = "cubG38j0Lraj"

# Kafka broker details
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "trackers.HTIT_51.gps"

# Global variable to store latest GPS data
latest_gps_data = None
mqtt_data_lock = threading.Lock()

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def create_saref_message(gps_data, temperature):
    """
    Create a JSON-LD message following ETSI SAREF Ontology v4 for smart home proximity detection
    """
    timestamp = datetime.now().isoformat()

    car_lat = float(gps_data.get("latitude", 0))
    car_lon = float(gps_data.get("longitude", 0))

    saref_message = {
        "@context": {
            "saref": "https://saref.etsi.org/core/v4.1.1/",
            "saref-core": "https://saref.etsi.org/core/",
            "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "dcat": "http://www.w3.org/ns/dcat#",
            "dcterms": "http://purl.org/dc/terms/"
        },
        "@id": f"urn:message:location:car:{timestamp}",
        "@type": "saref:Message",
        "dcterms:issued": timestamp,
        "saref:hasMeasurement": [
            {
                "@id": f"urn:measurement:location:car:{timestamp}",
                "@type": "saref:Measurement",
                "saref:hasProperty": {
                    "@type": "saref:LocationProperty",
                    "geo:lat": {
                        "@type": "xsd:float",
                        "@value": car_lat
                    },
                    "geo:long": {
                        "@type": "xsd:float",
                        "@value": car_lon
                    }
                }
            },
            {
                "@id": f"urn:measurement:temperature:car:{timestamp}",
                "@type": "saref:Measurement",
                "saref:hasProperty": {
                    "@type": "saref:TemperatureProperty",
                    "saref:hasValue": {
                        "@type": "xsd:float",
                        "@value": temperature
                    },
                    "saref:hasUnit": "saref:Celsius"
                }
            }
        ],
        "saref:forDevice": {
            "@id": "urn:device:car:gps-tracker",
            "@type": "saref:Device",
            "rdfs:label": "Car GPS Tracker",
            "dcterms:identifier": "HTIT_51"
        },
        "saref:targetDevice": [
            {
                "@id": "urn:device:home:smart-tv",
                "@type": "saref:Device",
                "rdfs:label": "Smart TV"
            },
            {
                "@id": "urn:device:home:smart-hvac",
                "@type": "saref:Device",
                "rdfs:label": "Smart HVAC"
            },
            {
                "@id": "urn:device:home:smart-barbecue",
                "@type": "saref:Device",
                "rdfs:label": "Smart Barbecue"
            }
        ]
    }

    return saref_message

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    global latest_gps_data
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        with mqtt_data_lock:
            latest_gps_data = payload
        print(f"Received GPS data from MQTT: {payload.get('latitude')}, {payload.get('longitude')}")
    except json.JSONDecodeError:
        print(f"Received non-JSON message on topic '{msg.topic}'")

def publish_saref_messages():
    """
    Publish SAREF messages every 2 seconds with mocked temperature data
    """
    print("Starting SAREF publisher...")

    while True:
        with mqtt_data_lock:
            if latest_gps_data is not None:
                # Mock temperature data (random between 15 and 35 degrees Celsius)
                temperature = round(random.uniform(15, 35), 2)

                # Create SAREF formatted message
                saref_msg = create_saref_message(latest_gps_data, temperature)

                # Publish to Kafka
                kafka_producer.send(KAFKA_TOPIC, value=saref_msg)

                print(f"\nPublished SAREF message:")
                print(json.dumps(saref_msg, indent=2))
                print(f"Temperature: {temperature}°C")
                print("-" * 60)

        time.sleep(2)

# Create MQTT client
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT broker
print("Connecting to MQTT broker...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Start MQTT loop in a separate thread
mqtt_thread = threading.Thread(target=mqtt_client.loop_forever, daemon=True)
mqtt_thread.start()

# Give MQTT time to connect
time.sleep(2)

# Start publishing SAREF messages
try:
    publish_saref_messages()
except KeyboardInterrupt:
    print("\nShutting down...")
    mqtt_client.disconnect()
    kafka_producer.close()
