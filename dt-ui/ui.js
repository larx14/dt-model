const socket = io('http://localhost:4000');

socket.on('device-state-update', (data) => {
  update(data);
});

function update(data) {
  const { latestState } = data;
  if (!latestState) return;

  if (latestState.tv) {
    document.getElementById("tv-status").innerText = latestState.tv.state;
  }

  if (latestState.hvac) {
    document.getElementById("hvac-status").innerText = latestState.hvac.state;
  }

  if (latestState.bbq) {
    document.getElementById("bbq-status").innerText = latestState.bbq.state;
  }
}

// Exposed globally so HTML onclick buttons can call it
function sendCommand(device, command) {
  fetch('/api/command', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ device, command })
  })
  .then(res => res.json())
  .then(data => {
    if (!data.success) console.error('Command failed');
  })
  .catch(err => console.error('Command error:', err));
}