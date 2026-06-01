// App State Machine: login → queue → playing
const state = {
  screen: 'login',
  token: null,
  playerId: null,
  username: null,
  matchId: null,
  ws: null,
  queueTimer: null,
  queueSeconds: 0,
};

// Screen switching
function showScreen(name) {
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  document.getElementById(`screen-${name}`).classList.add('active');
  state.screen = name;
}

// ========== LOGIN ==========
document.getElementById('btn-login').addEventListener('click', async () => {
  const username = document.getElementById('username').value.trim();
  const password = document.getElementById('password').value.trim();
  const statusEl = document.getElementById('login-status');

  if (!username || !password) {
    statusEl.textContent = 'Username and password required';
    statusEl.className = 'status error';
    return;
  }

  statusEl.textContent = 'Connecting...';
  statusEl.className = 'status';

  try {
    const res = await fetch('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });

    const data = await res.json();

    if (!res.ok) {
      statusEl.textContent = data.message || 'Login failed';
      statusEl.className = 'status error';
      return;
    }

    state.token = data.token;
    state.playerId = data.playerId;
    state.username = data.username;
    showScreen('queue');
    startQueue();
  } catch (err) {
    statusEl.textContent = 'Server unreachable';
    statusEl.className = 'status error';
  }
});

// ========== QUEUE ==========
function startQueue() {
  state.queueSeconds = 0;
  document.getElementById('queue-status').textContent = 'Searching for players...';
  updateQueueTimer();

  state.queueTimer = setInterval(() => {
    state.queueSeconds++;
    updateQueueTimer();
  }, 1000);

  // Poll for match (simplified — in production, WebSocket notification)
  pollForMatch();
}

function updateQueueTimer() {
  const m = Math.floor(state.queueSeconds / 60).toString().padStart(2, '0');
  const s = (state.queueSeconds % 60).toString().padStart(2, '0');
  document.getElementById('queue-timer').textContent = `${m}:${s}`;
}

async function pollForMatch() {
  // In production, the Notification Service pushes match-found events
  // For now, we'll simulate by connecting directly after a delay
  // The real flow: matchmaking service → notification → client gets matchId
  setTimeout(() => {
    if (state.screen === 'queue') {
      // Simulate match found
      state.matchId = 'demo-match-' + Date.now();
      clearInterval(state.queueTimer);
      joinGame();
    }
  }, 3000);
}

document.getElementById('btn-cancel-queue').addEventListener('click', () => {
  clearInterval(state.queueTimer);
  showScreen('login');
});

// ========== GAME ==========
function joinGame() {
  showScreen('game');
  
  // Focus the canvas for keyboard events
  const canvas = document.getElementById('game-canvas');
  canvas.focus();
  
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/game/?playerId=${state.playerId}&username=${state.username}`;

  state.ws = createGameConnection(wsUrl, {
    onMessage: (data) => {
      console.log('📨 Game state received');
      const gameState = JSON.parse(data);
      updateHUD(gameState);
      renderGame(gameState, state.playerId);
    },
    onClose: () => {
      showScreen('login');
      document.getElementById('login-status').textContent = 'Disconnected from game';
      document.getElementById('login-status').className = 'status error';
    },
    onError: () => {
      showScreen('login');
      document.getElementById('login-status').textContent = 'Connection error';
      document.getElementById('login-status').className = 'status error';
    },
  });

  setupInput(state.ws, state.playerId);
  console.log('Input handler setup complete, waiting for key presses');
}

function updateHUD(gameState) {
  const playerCount = Object.keys(gameState.players || {}).length;
  document.getElementById('hud-players').textContent = `Players: ${playerCount}/8`;
  document.getElementById('hud-tick').textContent = `Tick: ${gameState.tick || 0}`;

  const me = gameState.players?.[state.playerId];
  if (me) {
    document.getElementById('hud-score').textContent = `Score: ${me.score || 0}`;
  }
}

document.getElementById('btn-leave').addEventListener('click', () => {
  if (state.ws) state.ws.close();
  showScreen('login');
});