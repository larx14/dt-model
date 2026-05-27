const express = require('express');
const { Kafka } = require('kafkajs');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');
const axios = require('axios');
const N3 = require('n3');
const { DataFactory, Writer } = N3;
const namedNode = DataFactory.namedNode;
const literal = DataFactory.literal;

const graphQueue = [];
let graphProcessing = false;

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: { origin: "*" }
});

// Configuration
const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'trackers.HTIT_51.gps';

// Consumer Groups for Each Device
const HVAC_CONSUMER_GROUP = 'hvac-subscriber-group';
const TV_CONSUMER_GROUP = 'tv-subscriber-group';
const BARBECUE_CONSUMER_GROUP = 'barbecue-subscriber-group';

// GraphDB 
const GRAPHDB_ENDPOINT =
  'http://localhost:7200/repositories/smart-home/statements';

const RDF_PREFIX = 'http://example.org/';
const SAREF = "https://saref.etsi.org/core/";
const GEO = "http://www.w3.org/2003/01/geo/wgs84_pos#";
const RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const DCT = "http://purl.org/dc/terms/";

const GRAPH_HVAC = "http://example.org/graph/hvac";
const GRAPH_TV = "http://example.org/graph/tv";
const GRAPH_BBQ = "http://example.org/graph/barbecue";

// House location
const HOUSE_LAT = 52.2180;
const HOUSE_LON = 6.8900;

// Device thresholds
const HVAC_THRESHOLD_KM = 3;
const TV_THRESHOLD_KM = 2;
const BARBECUE_THRESHOLD_KM = 2;

// GraphDB throttling 
const GRAPHDB_INTERVAL_MS = 2000; // 2 seconds per device

let lastGraphWrite = {
  hvac: 0,
  tv: 0,
  barbecue: 0
};

// Global Device State
let deviceState = {
  carDistance: null,
  carLatitude: null,
  carLongitude: null,
  currentTemp: 0,
  lastUpdate: null,
  messageCount: 0,
  devices: {
    hvac: {
      isPowerOn: false,
      mode: 'OFF' // OFF, HEATING, COOLING
    },
    tv: {
      isPowerOn: false
    },
    barbecue: {
      isPowerOn: true // Default to ON
    }
  }
};

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'smart-home-subscriber',
  brokers: [KAFKA_BROKER]
});

// Create separate consumers for each device
const hvacConsumer = kafka.consumer({ groupId: HVAC_CONSUMER_GROUP });
const tvConsumer = kafka.consumer({ groupId: TV_CONSUMER_GROUP });
const barbecueConsumer = kafka.consumer({ groupId: BARBECUE_CONSUMER_GROUP });

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'smartHome.html'));
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

// Update shared location data
function updateLocationData(distance, temperature, latitude, longitude) {
  deviceState.carDistance = distance;
  deviceState.currentTemp = temperature;
  deviceState.carLatitude = latitude;
  deviceState.carLongitude = longitude;
  deviceState.lastUpdate = new Date().toISOString();
  deviceState.messageCount++;
}


// making rdf triples out of message & uploading to GraphDB
async function storeInGraphDB(
  deviceName,
  deviceState,
  temperature,
  distance,
  graphIRI
) {

  const writer = new Writer({
    format: 'application/trig',
    prefixes: {
      ex: RDF_PREFIX,
      saref: SAREF,
      geo: GEO,
      rdf: RDF,
      dcterms: DCT
    }
  });

  // graph node
  const graphNode = namedNode(graphIRI);

  // unique IDs
  const uniqueId = `${Date.now()}_${Math.random().toString(36).substring(2,8)}`;

  const messageNode = namedNode(`${RDF_PREFIX}Message_${uniqueId}`);
  const measurementNode = namedNode(`${RDF_PREFIX}Measurement_${uniqueId}`);
  const deviceNode = namedNode(`${RDF_PREFIX}${deviceName}`);

  // determine device state
  let stateValue = "UNKNOWN";

  if (deviceState?.mode !== undefined) {
    stateValue = deviceState.mode; // HVAC
  } else if (deviceState?.isPowerOn !== undefined) {
    stateValue = deviceState.isPowerOn ? "ON" : "OFF"; // TV / BBQ
  }


  writer.addQuad(
    messageNode,
    namedNode(`${RDF}type`),
    namedNode(`${SAREF}Message`),
    graphNode
  );


  writer.addQuad(
    messageNode,
    namedNode(`${SAREF}relatesTo`),
    deviceNode,
    graphNode
  );

  

  writer.addQuad(
    messageNode,
    namedNode(`${SAREF}hasMeasurement`),
    measurementNode,
    graphNode
  );

  // measurement type
  writer.addQuad(
    measurementNode,
    namedNode(`${RDF}type`),
    namedNode(`${SAREF}Measurement`),
    graphNode
  );

  // temperature value
  writer.addQuad(
    measurementNode,
    namedNode(`${SAREF}hasValue`),
    literal(String(temperature)),
    graphNode
  );

  writer.addQuad(
    messageNode,
    namedNode(`${RDF_PREFIX}hasDistance`),
    literal(String(distance)),
    graphNode
  );

  writer.addQuad(
    messageNode,
    namedNode(`${RDF_PREFIX}hasState`),
    literal(stateValue),
    graphNode
  );

  writer.addQuad(
    messageNode,
    namedNode(`${DCT}issued`),
    literal(new Date().toISOString()),
    graphNode
  );

  
  writer.end(async (error, result) => {

    if (error) {
      console.error("RDF generation error:", error);
      return;
    }

    try {

      await axios.post(
        GRAPHDB_ENDPOINT,
        result,
        {
          headers: {
            "Content-Type": "application/trig"
          }
        }
      );

      console.log(`Stored RDF in GraphDB (${graphIRI})`);

    } catch (err) {

      console.error("GraphDB ERROR FULL DEBUG:");
      console.error("message:", err.message);
      console.error("code:", err.code);
      console.error("status:", err.response?.status);
      console.error("data:", err.response?.data);
    }
  });
}

   

// HVAC Subscriber Logic
async function startHvacSubscriber() {
  await hvacConsumer.connect();
  console.log('[HVAC] Kafka consumer connected');

  await hvacConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[HVAC] Subscribed to Kafka topic');

  await hvacConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.hvac.isPowerOn;

          if (distance <= HVAC_THRESHOLD_KM) {
            deviceState.devices.hvac.isPowerOn = true;
            if (temperature > 25) {
              deviceState.devices.hvac.mode = 'COOLING';
            } else {
              deviceState.devices.hvac.mode = 'HEATING';
            }
          } else {
            deviceState.devices.hvac.isPowerOn = false;
            deviceState.devices.hvac.mode = 'OFF';
          }
          const now = Date.now();
            graphQueue.push({
              deviceName: "hvac",
              state: deviceState.devices.hvac,
              temperature,
              distance,
              graphIRI: GRAPH_HVAC
            });
            processGraphQueue();

          console.log(`[HVAC] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.hvac.isPowerOn ? 'ON' : 'OFF'} (${deviceState.devices.hvac.mode})`);

          if (previousState !== deviceState.devices.hvac.isPowerOn) {
            console.log(`[HVAC] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[HVAC] Error processing message:', err.message);
      }
    }
  });
}

// Smart TV Subscriber Logic
async function startTvSubscriber() {
  await tvConsumer.connect();
  console.log('[TV] Kafka consumer connected');

  await tvConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[TV] Subscribed to Kafka topic');

  await tvConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.tv.isPowerOn;

          if (distance <= TV_THRESHOLD_KM) {
            deviceState.devices.tv.isPowerOn = true;
          } else {
            deviceState.devices.tv.isPowerOn = false;
          }
         

                graphQueue.push({
        deviceName: "tv",
        state: deviceState.devices.tv,
        temperature,
        distance,
        graphIRI: GRAPH_TV
      });
      processGraphQueue();

          console.log(`[TV] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF'}`);

          if (previousState !== deviceState.devices.tv.isPowerOn) {
            console.log(`[TV] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[TV] Error processing message:', err.message);
      }
    }
  });
}

// Barbecue Subscriber Logic
async function startBarbecueSubscriber() {
  await barbecueConsumer.connect();
  console.log('[BARBECUE] Kafka consumer connected');

  await barbecueConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[BARBECUE] Subscribed to Kafka topic');

  await barbecueConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.barbecue.isPowerOn;

          if (distance > BARBECUE_THRESHOLD_KM) {
            deviceState.devices.barbecue.isPowerOn = false;
          } else {
            deviceState.devices.barbecue.isPowerOn = true;
          }
    
                  graphQueue.push({
          deviceName: "barbecue",
          state: deviceState.devices.barbecue,
          temperature,
          distance,
          graphIRI: GRAPH_BBQ
        });
        processGraphQueue();

          console.log(`[BARBECUE] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF'}`);

          if (previousState !== deviceState.devices.barbecue.isPowerOn) {
            console.log(`[BARBECUE] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[BARBECUE] Error processing message:', err.message);
      }
    }
  });
}

// throttling of rest calls to graphDB
async function processGraphQueue() {
  if (graphProcessing) return;

  graphProcessing = true;

  while (graphQueue.length > 0) {
    const job = graphQueue.shift();

    try {
      await storeInGraphDB(
        job.deviceName,
        job.state,
        job.temperature,
        job.distance,
        job.graphIRI
      );

      // throttle between writes
      await new Promise(r => setTimeout(r, 1000));

    } catch (err) {
      console.error("GraphDB queue error:", err.message);
    }
  }

  graphProcessing = false;
}

// Socket.io events
io.on('connection', (socket) => {
  console.log('[Socket.io] Web client connected');

  // Send current global device state to new client
  socket.emit('device-state-update', deviceState);

  socket.on('disconnect', () => {
    console.log('[Socket.io] Web client disconnected');
  });
});

// Start server
const PORT = 3000;
server.listen(PORT, () => {
  console.log(`\n=== Smart Home Multi-Device Subscriber ===`);
  console.log(`Running on http://localhost:${PORT}`);
  console.log(`House Location: ${HOUSE_LAT}, ${HOUSE_LON}\n`);

  console.log(`Device Thresholds:`);
  console.log(`  HVAC: ${HVAC_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Smart TV: ${TV_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Barbecue: ${BARBECUE_THRESHOLD_KM} km (separate consumer group)\n`);

  console.log(`Starting subscribers...\n`);

  startHvacSubscriber().catch(err => console.error('HVAC subscriber error:', err));
  startTvSubscriber().catch(err => console.error('TV subscriber error:', err));
  startBarbecueSubscriber().catch(err => console.error('Barbecue subscriber error:', err));
});

process.on('SIGINT', () => {
  console.log('\n\nShutting down all subscribers...');
  Promise.all([
    hvacConsumer.disconnect(),
    tvConsumer.disconnect(),
    barbecueConsumer.disconnect()
  ]).then(() => {
    server.close();
    process.exit(0);
  });
});
