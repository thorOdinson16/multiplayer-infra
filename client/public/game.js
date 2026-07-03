// Arena Game Client
const canvas = document.getElementById('gameCanvas');
const ctx = canvas.getContext('2d');

const WS_URL = 'ws://localhost:8080/ws';
const NOTIFY_WS_URL = 'ws://localhost:8080/ws/notifications';
const HTTP_URL = 'http://localhost:8080';

let token = null;
let playerId = null;
let ws = null;
let notifyWs = null;
let gameState = null;
let matchId = null;
let isSpectator = false;
let connected = false;
let keys = { w: false, a: false, s: false, d: false };
let inputInterval = null;

// DOM elements
const loginPanel = document.getElementById('login-panel');
const statusText = document.getElementById('status-text');
const btnLogin = document.getElementById('btn-login');
const btnRegister = document.getElementById('btn-register');
const btnJoinMatchmaking = document.getElementById('btn-join-matchmaking');
const btnSpectate = document.getElementById('btn-spectate');
const btnDisconnect = document.getElementById('btn-disconnect');
const usernameInput = document.getElementById('username');
const passwordInput = document.getElementById('password');
const playerList = document.getElementById('player-list');
const leaderboardContent = document.getElementById('leaderboard-content');

// Listen for keyboard input
document.addEventListener('keydown', (e) => { keys[e.key.toLowerCase()] = true; });
document.addEventListener('keyup', (e) => { keys[e.key.toLowerCase()] = false; });

// Canvas click to shoot
canvas.addEventListener('click', () => {
  if (ws && ws.readyState === WebSocket.OPEN && !isSpectator) {
    ws.send(JSON.stringify({ shoot: true }));
  }
});

async function apiCall(endpoint, method = 'GET', body = null, auth = false) {
  const headers = { 'Content-Type': 'application/json' };
  if (auth && token) headers['Authorization'] = `Bearer ${token}`;
  const options = { method, headers };
  if (body) options.body = JSON.stringify(body);
  try {
    const resp = await fetch(`${HTTP_URL}${endpoint}`, options);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json();
  } catch (err) {
    throw err;
  }
}

async function register() {
  const username = usernameInput.value.trim();
  const password = passwordInput.value.trim();
  if (!username || !password) return alert('Enter username and password');
  try {
    const data = await apiCall('/auth/register', 'POST', { username, password });
    token = data.access_token;
    const validate = await apiCall('/auth/validate', 'GET', null, true);
    playerId = validate.player_id;
    onLogin();
  } catch (err) {
    alert('Registration failed: ' + err.message);
  }
}

async function login() {
  const username = usernameInput.value.trim();
  const password = passwordInput.value.trim();
  if (!username || !password) return alert('Enter username and password');
  try {
    const data = await apiCall('/auth/login', 'POST', { username, password });
    token = data.access_token;
    const validate = await apiCall('/auth/validate', 'GET', null, true);
    playerId = validate.player_id;
    onLogin();
  } catch (err) {
    alert('Login failed: ' + err.message);
  }
}

function onLogin() {
  loginPanel.style.display = 'none';
  statusText.textContent = `Connected as: ${usernameInput.value} (${playerId.slice(0, 8)}...)`;
  btnJoinMatchmaking.disabled = false;
  btnSpectate.disabled = false;
  btnDisconnect.disabled = false;
  connectNotificationWS();
  loadLeaderboard();
}

function connectNotificationWS() {
  if (notifyWs) notifyWs.close();
  notifyWs = new WebSocket(NOTIFY_WS_URL);
  notifyWs.onopen = () => {
    notifyWs.send(JSON.stringify({ token }));
  };
  notifyWs.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'notification' && msg.event === 'match.found') {
        statusText.textContent = `Match found! Joining room ${msg.payload.room_id}...`;
        matchId = msg.payload.room_id;
        connectGameWS();
      }
    } catch (e) {}
  };
}

function connectGameWS() {
  if (ws) ws.close();
  ws = new WebSocket(`${WS_URL}?match=${matchId}`);
  ws.onopen = () => {
    ws.send(JSON.stringify({ token, mode: isSpectator ? 'spectator' : 'player' }));
    connected = true;
    statusText.textContent = `In game: ${matchId}`;
    startInputLoop();
  };
  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (data.type === 'state' || data.players) {
        gameState = data;
      } else if (data.type === 'match_end') {
        statusText.textContent = 'Match ended!';
        connected = false;
        stopInputLoop();
        ws.close();
        showMatchEnd(data.scores);
      }
    } catch (e) {}
  };
  ws.onclose = () => {
    if (connected) {
      connected = false;
      stopInputLoop();
      statusText.textContent = 'Disconnected. Attempting reconnect...';
      setTimeout(connectGameWS, 2000);
    }
  };
}

function startInputLoop() {
  if (inputInterval) clearInterval(inputInterval);
  inputInterval = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN || isSpectator) return;
    const dx = (keys.d ? 1 : 0) - (keys.a ? 1 : 0);
    const dy = (keys.s ? 1 : 0) - (keys.w ? 1 : 0);
    if (dx !== 0 || dy !== 0) {
      ws.send(JSON.stringify({ dx, dy, speed: 5 }));
    }
  }, 50);
}

function stopInputLoop() {
  if (inputInterval) clearInterval(inputInterval);
  inputInterval = null;
}

async function joinMatchmaking() {
  statusText.textContent = 'Joining matchmaking queue...';
  isSpectator = false;
  try {
    await apiCall('/matchmaking/queue', 'POST', { token });
  } catch (err) {
    statusText.textContent = 'Matchmaking failed: ' + err.message;
  }
}

async function spectateMatch() {
  const matchIdInput = prompt('Enter match ID to spectate:');
  if (!matchIdInput) return;
  matchId = matchIdInput;
  isSpectator = true;
  connectGameWS();
}

function disconnect() {
  if (ws) ws.close();
  if (notifyWs) notifyWs.close();
  connected = false;
  stopInputLoop();
  statusText.textContent = 'Disconnected';
  btnJoinMatchmaking.disabled = true;
  btnSpectate.disabled = true;
  btnDisconnect.disabled = true;
  loginPanel.style.display = 'block';
}

function showMatchEnd(scores) {
  let msg = 'Match Over!\nScores:\n';
  for (const [pid, score] of Object.entries(scores || {})) {
    msg += `${pid.slice(0, 8)}: ${score}\n`;
  }
  alert(msg);
}

async function loadLeaderboard() {
  try {
    const data = await apiCall('/leaderboard?limit=10');
    let html = '<ol>';
    for (const p of (data.rankings || [])) {
      html += `<li>${p.username || p.playerId?.slice(0, 8)} - ${p.wins}W/${p.losses}L (Elo: ${p.eloRating})</li>`;
    }
    html += '</ol>';
    leaderboardContent.innerHTML = html;
  } catch (e) {
    leaderboardContent.textContent = 'Failed to load leaderboard';
  }
  setTimeout(loadLeaderboard, 30000);
}

// Rendering
function render() {
  ctx.clearRect(0, 0, 800, 600);
  if (!gameState) {
    ctx.fillStyle = '#4ecca3';
    ctx.font = '24px monospace';
    ctx.textAlign = 'center';
    ctx.fillText('Waiting for game state...', 400, 300);
    requestAnimationFrame(render);
    return;
  }
  const players = gameState.players || {};
  // Draw grid
  ctx.strokeStyle = '#1a1a3e';
  ctx.lineWidth = 1;
  for (let x = 0; x < 800; x += 50) {
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, 600); ctx.stroke();
  }
  for (let y = 0; y < 600; y += 50) {
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(800, y); ctx.stroke();
  }
  // Draw players
  for (const [pid, p] of Object.entries(players)) {
    const cx = 400 + p.x;
    const cy = 300 + p.y;
    const isMe = pid === playerId;
    ctx.beginPath();
    ctx.arc(cx, cy, 15, 0, Math.PI * 2);
    ctx.fillStyle = isMe ? '#4ecca3' : (p.connected ? '#e94560' : '#666');
    ctx.fill();
    ctx.strokeStyle = '#fff';
    ctx.lineWidth = 2;
    ctx.stroke();
    ctx.fillStyle = '#fff';
    ctx.font = '10px monospace';
    ctx.textAlign = 'center';
    ctx.fillText(pid.slice(0, 6), cx, cy - 20);
    // Health bar
    const healthPct = (p.health || 100) / 100;
    ctx.fillStyle = '#333';
    ctx.fillRect(cx - 20, cy - 25, 40, 4);
    ctx.fillStyle = healthPct > 0.5 ? '#4ecca3' : (healthPct > 0.25 ? '#ffd700' : '#e94560');
    ctx.fillRect(cx - 20, cy - 25, 40 * healthPct, 4);
    // Score
    ctx.fillStyle = '#ffd700';
    ctx.font = '9px monospace';
    ctx.fillText(`Score: ${p.score || 0}`, cx, cy + 28);
  }
  // HUD
  ctx.fillStyle = 'rgba(0,0,0,0.5)';
  ctx.fillRect(0, 0, 200, 60);
  ctx.fillStyle = '#fff';
  ctx.font = '12px monospace';
  ctx.textAlign = 'left';
  ctx.fillText(`Tick: ${gameState.tick || 0}`, 10, 20);
  ctx.fillText(`Players: ${Object.keys(players).length}`, 10, 40);
  ctx.fillText(`Spectator: ${isSpectator ? 'YES' : 'NO'}`, 10, 55);
  if (isSpectator) {
    ctx.fillStyle = '#ffd700';
    ctx.font = '14px monospace';
    ctx.textAlign = 'center';
    ctx.fillText('SPECTATOR MODE (10s delay)', 400, 30);
  }
  requestAnimationFrame(render);
}

// Event handlers
btnRegister.addEventListener('click', register);
btnLogin.addEventListener('click', login);
btnJoinMatchmaking.addEventListener('click', joinMatchmaking);
btnSpectate.addEventListener('click', spectateMatch);
btnDisconnect.addEventListener('click', disconnect);

// Start render loop
render();

// Load leaderboard on startup
loadLeaderboard();
