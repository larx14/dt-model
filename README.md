#  MQTT Pub/Sub Architecture

## Getting Started

### Prerequisites

- **Node.js** v14+
- **Python 3.x**
- **Kafka** running on `localhost:9092`

### Installation


1. Install Node.js dependencies
   ```bash
   npm install
   ```

2. Install Python dependencies
   ```bash
   pip install paho-mqtt kafka-python python-dotenv
   ```


### Running the System

**Terminal 1: Start Kafka** (if not already running)
```bash
# Ensure Kafka is running on localhost:9092
```

**Terminal 2: Start SAREF Publisher**
```bash
python saref_publisher.py
```

**Terminal 3: Start HVAC Subscriber**
```bash
node hvac-server.js
```

**Browser: Access Dashboard**
```
http://localhost:3000
```
