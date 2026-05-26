const express = require('express');
const { Kafka } = require('kafkajs');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: { origin: "*" }
});

// Configuration
const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'trackers.HTIT_51.gps';
const CONSUMER_GROUP = 'hvac-subscriber-group';

// House location (hard-coded) - Within 3km of GPS tracker
const HOUSE_LAT = 52.2180;
const HOUSE_LON = 6.8900;

// HVAC threshold
const PROXIMITY_THRESHOLD_KM = 3;

// HVAC State
let hvacState = {
  isPowerOn: false,
  mode: 'OFF', // OFF, HEATING, COOLING
  currentTemp: 0,
  carDistance: null,
  carLatitude: null,
  carLongitude: null,
  lastUpdate: null,
  messageCount: 0
};

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'hvac-subscriber',
  brokers: [KAFKA_BROKER]
});

const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

// Serve hvac.html at root
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'hvac.html'));
});

// Calculate distance between two GPS coordinates using Haversine formula
function calculateDistance(lat1, lon1, lat2, lon2) {
  const R = 6371; // Earth's radius in km
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// Extract temperature and location from SAREF message
function extractDataFromSarefMessage(sarefMsg) {
  let temperature = null;
  let latitude = null;
  let longitude = null;

  const measurements = sarefMsg['saref:hasMeasurement'] || [];

  for (const measurement of measurements) {
    const propType = measurement['saref:hasProperty']?.['@type'];

    if (propType === 'saref:TemperatureProperty') {
      temperature = measurement['saref:hasProperty']['saref:hasValue']['@value'];
    }

    if (propType === 'saref:LocationProperty') {
      latitude = measurement['saref:hasProperty']['geo:lat']?.['@value'];
      longitude = measurement['saref:hasProperty']['geo:long']?.['@value'];
    }
  }

  // Calculate distance from home
  let distance = null;
  if (latitude !== null && longitude !== null) {
    distance = calculateDistance(latitude, longitude, HOUSE_LAT, HOUSE_LON);
  }

  return { distance, temperature, latitude, longitude };
}

// HVAC Logic
function updateHvacState(distance, temperature, latitude, longitude) {
  const previousState = { ...hvacState };

  hvacState.carDistance = distance;
  hvacState.currentTemp = temperature;
  hvacState.carLatitude = latitude;
  hvacState.carLongitude = longitude;
  hvacState.lastUpdate = new Date().toISOString();
  hvacState.messageCount++;

  // If car is within threshold distance, turn on HVAC
  if (distance <= PROXIMITY_THRESHOLD_KM) {
    hvacState.isPowerOn = true;

    // Determine heating or cooling based on temperature
    if (temperature > 25) {
      hvacState.mode = 'COOLING';
    } else {
      hvacState.mode = 'HEATING';
    }
  } else {
    // Car is far away, turn off HVAC
    hvacState.isPowerOn = false;
    hvacState.mode = 'OFF';
  }

  return previousState !== hvacState;
}

// Kafka consumer setup
async function startHvacSubscriber() {
  await consumer.connect();
  console.log('Kafka consumer connected');
  io.emit('status', { kafka: 'connected' });

  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log(`Subscribed to Kafka topic: ${KAFKA_TOPIC}\n`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          const stateChanged = updateHvacState(distance, temperature, latitude, longitude);

          // Log the update
          console.log(`\n Received SAREF Message:`);
          console.log(`   Distance from home: ${distance.toFixed(2)} km`);
          console.log(`   Temperature: ${temperature}°C`);
          console.log(`   HVAC Power: ${hvacState.isPowerOn ? 'ON' : 'OFF'}`);
          console.log(`   Mode: ${hvacState.mode}`);
          console.log(`   Timestamp: ${hvacState.lastUpdate}`);

          // Emit state update to web clients
          io.emit('hvac-state-update', hvacState);

          if (stateChanged) {
            console.log(`   ⚡ State changed!\n`);
          }
        }
      } catch (err) {
        console.error('Error processing SAREF message:', err);
        io.emit('status', { kafka: 'error', message: err.message });
      }
    }
  });
}

// Socket.io events
io.on('connection', (socket) => {
  console.log(' Web client connected');

  // Send current state to new client
  socket.emit('hvac-state-update', hvacState);

  socket.on('disconnect', () => {
    console.log('Web client disconnected');
  });
});

// Start server
const PORT = 3000;
server.listen(PORT, () => {
  console.log(`\nSmart HVAC Subscriber running on http://localhost:${PORT}`);
  console.log(`House Location: ${HOUSE_LAT}, ${HOUSE_LON}`);
  console.log(`Proximity Threshold: ${PROXIMITY_THRESHOLD_KM} km\n`);

  // Start listening for messages
  startHvacSubscriber();
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n\n Shutting down HVAC subscriber...');
  consumer.disconnect();
  server.close();
  process.exit(0);
});
