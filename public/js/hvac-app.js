const socket = io();

// Update HVAC state from server
socket.on('hvac-state-update', (hvacState) => {
  updateDisplay(hvacState);
});

function updateDisplay(hvacState) {
  // Update GPS data
  document.getElementById('latitude').textContent = hvacState.carLatitude?.toFixed(4) || '--';
  document.getElementById('longitude').textContent = hvacState.carLongitude?.toFixed(4) || '--';

  if (hvacState.carDistance !== null) {
    document.getElementById('distance').textContent = hvacState.carDistance.toFixed(2);
  }

  document.getElementById('temperature').textContent = hvacState.currentTemp.toFixed(1);

  // Update HVAC Power Status (ON if distance <= 3km, OFF otherwise)
  const hvacPower = document.getElementById('hvac-power');
  if (hvacState.isPowerOn) {
    hvacPower.classList.add('on');
    hvacPower.classList.remove('off');
    hvacPower.querySelector('.status-icon').textContent = '🟢';
    hvacPower.querySelector('.status-text').textContent = 'ON';
  } else {
    hvacPower.classList.remove('on');
    hvacPower.classList.add('off');
    hvacPower.querySelector('.status-icon').textContent = '⚪';
    hvacPower.querySelector('.status-text').textContent = 'OFF';
  }

  // Update Cooling Status (ON if mode is COOLING)
  const coolingStatus = document.getElementById('cooling-status');
  if (hvacState.mode === 'COOLING') {
    coolingStatus.classList.add('on');
    coolingStatus.classList.remove('off');
    coolingStatus.querySelector('.status-icon').textContent = '🟦';
    coolingStatus.querySelector('.status-text').textContent = 'ON';
  } else {
    coolingStatus.classList.remove('on');
    coolingStatus.classList.add('off');
    coolingStatus.querySelector('.status-icon').textContent = '⚪';
    coolingStatus.querySelector('.status-text').textContent = 'OFF';
  }

  // Update Heating Status (ON if mode is HEATING)
  const heatingStatus = document.getElementById('heating-status');
  if (hvacState.mode === 'HEATING') {
    heatingStatus.classList.add('on');
    heatingStatus.classList.remove('off');
    heatingStatus.querySelector('.status-icon').textContent = '🟥';
    heatingStatus.querySelector('.status-text').textContent = 'ON';
  } else {
    heatingStatus.classList.remove('on');
    heatingStatus.classList.add('off');
    heatingStatus.querySelector('.status-icon').textContent = '⚪';
    heatingStatus.querySelector('.status-text').textContent = 'OFF';
  }
}

console.log('🚀 Smart HVAC Subscriber loaded');
