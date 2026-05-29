const express = require('express');
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');
const { Kafka } = require('kafkajs');
const axios = require('axios');

/* 
   EXPRESS + SOCKET SETUP
 */

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = 4000;
app.use(express.json());

// UI
app.use('/ui', express.static(path.join(__dirname, 'dt-ui')));
app.get('/ui', (req, res) => {
  res.sendFile(path.join(__dirname, 'dt-ui', 'index.html'));
});

// Command endpoint — called by the UI buttons
app.post('/api/command', async (req, res) => {
  const { device, command } = req.body;

  try {
    await sendCommand(device, command);
    res.json({ success: true });
  } catch (err) {
    console.error('[API] Command failed:', err.message);
    res.status(500).json({ success: false });
  }
});

/* 
   KAFKA SETUP

   */

const kafka = new Kafka({
  clientId: 'digital-twin',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({
  groupId: 'digital-twin-group-' + Date.now()
});

const producer = kafka.producer();

const STATE_TOPICS = [
  'home.tv.state',
  'home.hvac.state',
  'home.bbq.state'
];

/* 
   DIGITAL TWIN STATE, DT needs to know and display the states at all times
*/

const latestState = {
  tv: null,
  hvac: null,
  bbq: null
};

const history = {
  tv: [],
  hvac: [],
  bbq: []
};

/* 
   GRAPHDB STORAGE
    */

async function storeStateTriple(device, state, timestamp) {
  const rdf = `
INSERT DATA {
  GRAPH <http://example.org/dt> {
    <http://example.org/${device}>
      <http://example.org/hasState> "${state}" ;
      <http://example.org/hasTimestamp> "${timestamp}" .
  }
}`;

  await axios.post(
    'http://localhost:7200/repositories/smart-home/statements',
    rdf,
    { headers: { 'Content-Type': 'application/sparql-update' } }
  );
}

async function storeCommandTriple(device, command, timestamp) {
  const rdf = `
INSERT DATA {
  GRAPH <http://example.org/dt> {
    <http://example.org/${device}/command/${timestamp}>
      <http://example.org/hasCommand> "${command}" ;
      <http://example.org/hasTimestamp> "${timestamp}" .
  }
}`;

  await axios.post(
    'http://localhost:7200/repositories/smart-home/statements',
    rdf,
    { headers: { 'Content-Type': 'application/sparql-update' } }
  );
}

/* 
   SEND COMMAND (DT → PT)
   Uses saref:hasCommandKind 
*/

async function sendCommand(device, command) {
  const topic = `home.${device}.command`;

  const message = {
    "@context": {
      "saref": "https://saref.etsi.org/core/"
    },
    "@type": "saref:Command",
    "saref:actsUpon": device,
    "saref:hasCommandKind": command  //  home-app reads this field
  };

  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(message) }]
  });

  const timestamp = new Date().toISOString();
  console.log(`[DT COMMAND] ${device.toUpperCase()} <- ${command} @ ${timestamp}`);

  try {
    await storeCommandTriple(device, command, timestamp);
  } catch (err) {
    console.error('[GraphDB] Command store failed:', err.message);
  }
}

/*
   STATE EXTRACTION

   */

function extractState(payload) {
  // State messages from home-app look like:
  // { "@type": "saref:State", "saref:hasState": { "@value": "ON" } }
  return payload?.["saref:hasState"]?.["@value"] || null;
}

/* 
   KAFKA CONSUMER
 */

async function start() {
  await producer.connect();
  await consumer.connect();

  for (const topic of STATE_TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  console.log('[DT] Subscribed to state topics');

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const raw = message.value.toString();
      const payload = JSON.parse(raw);
      const timestamp = new Date().toISOString();

      let device = null;
      if (topic.includes('tv'))   device = 'tv';
      if (topic.includes('hvac')) device = 'hvac';
      if (topic.includes('bbq'))  device = 'bbq';

      if (!device) return;

      const state = extractState(payload);

      if (!state) {
        console.warn(`[DT] No state found in ${topic}:`, payload);
        return;
      }

      // Update twin state
      latestState[device] = { state, timestamp };
      history[device].push({ state, timestamp });

      console.log(`[DT] ${device.toUpperCase()} state: ${state}`);

      // Store in GraphDB
      try {
        await storeStateTriple(device, state, timestamp);
      } catch (err) {
        console.error('[GraphDB] State store failed:', err.message);
      }

      // Push to UI
      io.emit('device-state-update', { latestState, history });
    }
  });
}

/* 
   START SERVER
 */

io.on('connection', (socket) => {
  // Send current state immediately to the newly connected client
  socket.emit('device-state-update', { latestState, history });
});

server.listen(PORT, () => {
  console.log(`[DT] UI running on http://localhost:${PORT}/ui`);
  start().catch(console.error);
});