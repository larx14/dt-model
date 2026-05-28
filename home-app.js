const express = require('express');
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');
const { Kafka } = require('kafkajs');

/* =========================
   EXPRESS + SOCKET SETUP
========================= */

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = 3000;

// UI
app.use('/ui', express.static(path.join(__dirname, 'dt-ui')));
app.get('/ui', (req, res) => {
  res.sendFile(path.join(__dirname, 'dt-ui', 'index.html'));
});

/* =========================
   KAFKA SETUP
========================= */

const kafka = new Kafka({
  clientId: 'digital-twin',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'digital-twin-group' });
const producer = kafka.producer();

const TOPICS = [
  'home.tv.state',
  'home.hvac.state',
  'home.bbq.state'
];

/* =========================
   DIGITAL TWIN STATE
========================= */

const state = {
  tv: { isPowerOn: false },
  hvac: { mode: 'OFF' },
  bbq: { isPowerOn: false }
};

/* =========================
   STATE EXTRACTION
========================= */

function extractState(payload) {
  const measurements = payload?.["saref:hasMeasurement"] || [];

  for (const m of measurements) {
    const props = m?.["saref:hasProperty"];

    if (props?.["saref:hasValue"]) {
      return props["saref:hasValue"]["@value"];
    }
  }

  return payload?.["saref:hasState"]?.["@value"] || null;
}

/* =========================
   STATE PUBLISHER
========================= */

async function publishDeviceState(device, value) {
  const message = {
    "@context": { saref: "https://saref.etsi.org/core/" },
    "@type": "saref:State",
    "saref:hasState": { "@value": value }
  };

  await producer.send({
    topic: `home.${device}.state`,
    messages: [{ value: JSON.stringify(message) }]
  });

  console.log(`📤 STATE PUBLISHED: ${device} -> ${value}`);
}

/* =========================
   KAFKA LOOP
========================= */

async function start() {
  await consumer.connect();
  await producer.connect();

  for (const topic of TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  // ALSO subscribe to commands
  await consumer.subscribe({ topic: 'home.tv.command', fromBeginning: false });
  await consumer.subscribe({ topic: 'home.hvac.command', fromBeginning: false });
  await consumer.subscribe({ topic: 'home.bbq.command', fromBeginning: false });

  console.log('DT running...');

  await consumer.run({
    eachMessage: async ({ topic, message }) => {

      const payload = JSON.parse(message.value.toString());

      /* =========================
         COMMANDS
      ========================= */

      if (topic.includes('command')) {
        const cmd = payload?.["saref:hasCommandKind"];

        console.log(`📩 COMMAND RECEIVED: ${topic} -> ${cmd}`);

        if (topic === 'home.tv.command') {
          if (cmd?.includes('TurnOn')) state.tv.isPowerOn = true;
          if (cmd?.includes('TurnOff')) state.tv.isPowerOn = false;

          await publishDeviceState('tv', state.tv.isPowerOn ? 'ON' : 'OFF');
        }

        if (topic === 'home.hvac.command') {
          if (cmd?.includes('TurnOn')) state.hvac.mode = 'HEATING';
          if (cmd?.includes('TurnOff')) state.hvac.mode = 'OFF';

          await publishDeviceState('hvac', state.hvac.mode);
        }

        if (topic === 'home.bbq.command') {
          if (cmd?.includes('TurnOn')) state.bbq.isPowerOn = true;
          if (cmd?.includes('TurnOff')) state.bbq.isPowerOn = false;

          await publishDeviceState('bbq', state.bbq.isPowerOn ? 'ON' : 'OFF');
        }

        io.emit('state', state);
        return;
      }

      /* =========================
         STATE UPDATES
      ========================= */

     // console.log(`📥 STATE RECEIVED: ${topic}`);

      const value = extractState(payload);
      if (!value) return;

      if (topic === 'home.tv.state') {
        state.tv.isPowerOn = value === 'ON';
      }

      if (topic === 'home.hvac.state') {
        state.hvac.mode = value;
      }

      if (topic === 'home.bbq.state') {
        state.bbq.isPowerOn = value === 'ON';
      }

      io.emit('state', state);
    }
  });
}

/* =========================
   START SERVER
========================= */

server.listen(PORT, async () => {
  console.log(`DT UI running on http://localhost:${PORT}/ui`);
  await start();
});