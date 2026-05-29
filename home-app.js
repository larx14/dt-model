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

/* 
   CONFIGURATION
 */

const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'trackers.HTIT_51.gps'; // GPS topic from previous assignment

const HVAC_CONSUMER_GROUP = 'hvac-subscriber-group';
const TV_CONSUMER_GROUP = 'tv-subscriber-group';
const BARBECUE_CONSUMER_GROUP = 'barbecue-subscriber-group';

const GRAPHDB_ENDPOINT = 'http://localhost:7200/repositories/smart-home/statements';

const RDF_PREFIX = 'http://example.org/';
const SAREF = "https://saref.etsi.org/core/";
const GEO = "http://www.w3.org/2003/01/geo/wgs84_pos#";
const RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const DCT = "http://purl.org/dc/terms/";

const GRAPH_HVAC = "http://example.org/graph/hvac";
const GRAPH_TV = "http://example.org/graph/tv";
const GRAPH_BBQ = "http://example.org/graph/barbecue";

const HOUSE_LAT = 52.2180;
const HOUSE_LON = 6.8900;

const HVAC_THRESHOLD_KM = 3;
const TV_THRESHOLD_KM = 2;
const BARBECUE_THRESHOLD_KM = 2;

/* 
   GLOBAL STATE
 */

let deviceState = {
  carDistance: null,
  carLatitude: null,
  carLongitude: null,
  currentTemp: 0,
  lastUpdate: null,
  messageCount: 0,
  devices: {
    hvac: { isPowerOn: false, mode: 'OFF' },
    tv: { isPowerOn: false },
    barbecue: { isPowerOn: true }
  }
};

/* 
   KAFKA
 */

const kafka = new Kafka({
  clientId: 'smart-home-subscriber',
  brokers: [KAFKA_BROKER]
});

const hvacConsumer = kafka.consumer({ groupId: HVAC_CONSUMER_GROUP });
const tvConsumer = kafka.consumer({ groupId: TV_CONSUMER_GROUP });
const barbecueConsumer = kafka.consumer({ groupId: BARBECUE_CONSUMER_GROUP });

const { Partitioners } = require('kafkajs');
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

/* 
   EXPRESS
 */

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'smartHome.html'));
});

/* 
   HELPERS
 */

function calculateDistance(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function updateLocationData(distance, temperature, latitude, longitude) {
  deviceState.carDistance = distance;
  deviceState.currentTemp = temperature;
  deviceState.carLatitude = latitude;
  deviceState.carLongitude = longitude;
  deviceState.lastUpdate = new Date().toISOString();
  deviceState.messageCount++;
}

function extractDataFromSarefMessage(sarefMsg) {
  let temperature = null;
  let latitude = null;
  let longitude = null;

  const measurements = sarefMsg?.["saref:hasMeasurement"] || [];

  for (const m of measurements) {
    const prop = m?.["saref:hasProperty"];
    const type = prop?.["@type"];

    if (type === "saref:TemperatureProperty") {
      temperature = prop?.["saref:hasValue"]?.["@value"];
    }

    if (type === "saref:LocationProperty") {
      latitude = prop?.["geo:lat"]?.["@value"];
      longitude = prop?.["geo:long"]?.["@value"];
    }
  }

  let distance = null;
  if (latitude != null && longitude != null) {
    distance = calculateDistance(Number(latitude), Number(longitude), HOUSE_LAT, HOUSE_LON);
  }

  return { distance: distance ?? null, temperature: temperature ?? null, latitude, longitude };
}

/* 
   GRAPHDB
 */

async function storeInGraphDB(deviceName, deviceState, temperature, distance, graphIRI) {
  const writer = new Writer({
    format: 'application/trig',
    prefixes: { ex: RDF_PREFIX, saref: SAREF, geo: GEO, rdf: RDF, dcterms: DCT }
  });

  const graphNode = namedNode(graphIRI);
  const uniqueId = `${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
  const messageNode = namedNode(`${RDF_PREFIX}Message_${uniqueId}`);
  const measurementNode = namedNode(`${RDF_PREFIX}Measurement_${uniqueId}`);
  const deviceNode = namedNode(`${RDF_PREFIX}${deviceName}`);

  let stateValue = "UNKNOWN";
  if (deviceState?.mode !== undefined) stateValue = deviceState.mode;
  else if (deviceState?.isPowerOn !== undefined) stateValue = deviceState.isPowerOn ? "ON" : "OFF";

  writer.addQuad(messageNode, namedNode(`${RDF}type`), namedNode(`${SAREF}Message`), graphNode);
  writer.addQuad(messageNode, namedNode(`${SAREF}relatesTo`), deviceNode, graphNode);
  writer.addQuad(messageNode, namedNode(`${SAREF}hasMeasurement`), measurementNode, graphNode);
  writer.addQuad(measurementNode, namedNode(`${RDF}type`), namedNode(`${SAREF}Measurement`), graphNode);
  writer.addQuad(measurementNode, namedNode(`${SAREF}hasValue`), literal(String(temperature)), graphNode);
  writer.addQuad(messageNode, namedNode(`${RDF_PREFIX}hasDistance`), literal(String(distance)), graphNode);
  writer.addQuad(messageNode, namedNode(`${RDF_PREFIX}hasState`), literal(stateValue), graphNode);
  writer.addQuad(messageNode, namedNode(`${DCT}issued`), literal(new Date().toISOString()), graphNode);

  writer.end(async (error, result) => {
    if (error) { console.error("RDF generation error:", error); return; }
    try {
      await axios.post(GRAPHDB_ENDPOINT, result, { headers: { "Content-Type": "application/trig" } });
    } catch (err) {
      console.error(`[GraphDB] Write failed:`, err.message);
    }
  });
}

async function processGraphQueue() {
  if (graphProcessing) return;
  graphProcessing = true;
  while (graphQueue.length > 0) {
    const job = graphQueue.shift();
    try {
      await storeInGraphDB(job.deviceName, job.state, job.temperature, job.distance, job.graphIRI);
      await new Promise(r => setTimeout(r, 1000));
    } catch (err) {
      console.error("GraphDB queue error:", err.message);
    }
  }
  graphProcessing = false;
}

/* 
   PUBLISH STATE -> reused by subscribers 
 */

async function publishDeviceState(deviceName, state) {
  const stateMessage = {
    "@context": { "saref": "https://saref.etsi.org/core/" },
    "@type": "saref:State",
    "saref:hasState": { "@value": state }
  };

  await producer.send({
    topic: `home.${deviceName}.state`,
    messages: [{ value: JSON.stringify(stateMessage) }]
  });

  console.log(`[${deviceName.toUpperCase()}] State published -> home.${deviceName}.state: ${state}`);
}

/* 
   HVAC SUBSCRIBER

   */

async function startHvacSubscriber() {
  await hvacConsumer.connect();
  await hvacConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  await hvacConsumer.subscribe({ topic: 'home.hvac.command', fromBeginning: false });

  await hvacConsumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const parsed = JSON.parse(message.value.toString());

        // Command (DT -> PT) // command handling
        if (topic === 'home.hvac.command') {
          const cmd = parsed?.["saref:hasCommandKind"];
          console.log(`[HVAC] Command received: ${cmd}`);

          if (cmd === "TURN_ON" || cmd === "TurnOn") {
            deviceState.devices.hvac.isPowerOn = true;
            deviceState.devices.hvac.mode = "HEATING";
          }
          if (cmd === "TURN_OFF" || cmd === "TurnOff") {
            deviceState.devices.hvac.isPowerOn = false;
            deviceState.devices.hvac.mode = "OFF";
          }

          await publishDeviceState("hvac", deviceState.devices.hvac.mode);
          io.emit('device-state-update', deviceState);
          return;
        }

        // GPS data (PT -> DT)
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(parsed);
        if (distance === null || temperature === null) return;

        updateLocationData(distance, temperature, latitude, longitude);
        const previousState = deviceState.devices.hvac.isPowerOn;

        if (distance <= HVAC_THRESHOLD_KM) {
          deviceState.devices.hvac.isPowerOn = true;
          deviceState.devices.hvac.mode = temperature > 25 ? 'COOLING' : 'HEATING';
        } else {
          deviceState.devices.hvac.isPowerOn = false;
          deviceState.devices.hvac.mode = 'OFF';
        }

        graphQueue.push({ deviceName: "hvac", state: deviceState.devices.hvac, temperature, distance, graphIRI: GRAPH_HVAC });
        processGraphQueue();

        if (previousState !== deviceState.devices.hvac.isPowerOn) {
          // Publish state changes
          await publishDeviceState("hvac", deviceState.devices.hvac.mode);
        }

        io.emit('device-state-update', deviceState);

      } catch (err) {
        console.error('[HVAC] Error:', err.message);
      }
    }
  });
}

/*
   TV SUBSCRIBER
 */

async function startTvSubscriber() {
  await tvConsumer.connect();
  await tvConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  await tvConsumer.subscribe({ topic: 'home.tv.command', fromBeginning: false });

  await tvConsumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const payload = JSON.parse(message.value.toString());

        // Command (DT -> PT)
        if (topic === 'home.tv.command') {
          const command = payload?.["saref:hasCommandKind"];
          console.log(`[TV] Command received: ${command}`);

          if (!command) return;
          if (command === "TurnOn") deviceState.devices.tv.isPowerOn = true;
          if (command === "TurnOff") deviceState.devices.tv.isPowerOn = false;

          await publishDeviceState("tv", deviceState.devices.tv.isPowerOn ? "ON" : "OFF");
          io.emit('device-state-update', deviceState);
          return;
        }

        // GPS data (PT -> DT)
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(payload);
        if (distance === null || temperature === null) return;

        updateLocationData(distance, temperature, latitude, longitude);
        const previousState = deviceState.devices.tv.isPowerOn;

        deviceState.devices.tv.isPowerOn = distance <= TV_THRESHOLD_KM;

        graphQueue.push({ deviceName: "tv", state: deviceState.devices.tv, temperature, distance, graphIRI: GRAPH_TV });
        processGraphQueue();

        // publish state changes upon receiving them 
        if (previousState !== deviceState.devices.tv.isPowerOn) {
          await publishDeviceState("tv", deviceState.devices.tv.isPowerOn ? "ON" : "OFF");
        }

        io.emit('device-state-update', deviceState);

      } catch (err) {
        console.error('[TV] Error:', err.message);
      }
    }
  });
}

/* 
   BARBECUE SUBSCRIBER
*/

async function startBarbecueSubscriber() {
  await barbecueConsumer.connect();
  await barbecueConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  await barbecueConsumer.subscribe({ topic: 'home.bbq.command', fromBeginning: false });

  await barbecueConsumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const payload = JSON.parse(message.value.toString());

        // Command (DT -> PT) Handle commands
        if (topic === 'home.bbq.command') {
          const command = payload?.["saref:hasCommandKind"];
          console.log(`[BBQ] Command received: ${command}`);

          if (!command) return;
          if (command === "TurnOn") deviceState.devices.barbecue.isPowerOn = true;
          if (command === "TurnOff") deviceState.devices.barbecue.isPowerOn = false;

          // publish states 
          await publishDeviceState("bbq", deviceState.devices.barbecue.isPowerOn ? "ON" : "OFF");
          io.emit('device-state-update', deviceState);
          return;
        }

        // GPS data (PT -> DT)
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(payload);
        if (distance === null || temperature === null) return;

        updateLocationData(distance, temperature, latitude, longitude);
        const previousState = deviceState.devices.barbecue.isPowerOn;

        deviceState.devices.barbecue.isPowerOn = distance <= BARBECUE_THRESHOLD_KM;

        graphQueue.push({ deviceName: "barbecue", state: deviceState.devices.barbecue, temperature, distance, graphIRI: GRAPH_BBQ });
        processGraphQueue();

        if (previousState !== deviceState.devices.barbecue.isPowerOn) {
          await publishDeviceState("bbq", deviceState.devices.barbecue.isPowerOn ? "ON" : "OFF");
        }

        io.emit('device-state-update', deviceState);

      } catch (err) {
        console.error('[BARBECUE] Error:', err.message);
      }
    }
  });
}

/* 
   SOCKET.IO
 */

io.on('connection', (socket) => {
  socket.emit('device-state-update', deviceState);
});

/* 
   START

   */

const PORT = 3000;

async function start() {
  await producer.connect();

  server.listen(PORT, () => {
    console.log(`\n=== Smart Home PT ===`);
    console.log(`Running on http://localhost:${PORT}`);
    console.log(`GPS topic: ${KAFKA_TOPIC}`);
    console.log(`Thresholds: HVAC ${HVAC_THRESHOLD_KM}km | TV ${TV_THRESHOLD_KM}km | BBQ ${BARBECUE_THRESHOLD_KM}km\n`);

    startHvacSubscriber().catch(err => console.error('HVAC error:', err));
    startTvSubscriber().catch(err => console.error('TV error:', err));
    startBarbecueSubscriber().catch(err => console.error('BBQ error:', err));
  });
}

start().catch(console.error);