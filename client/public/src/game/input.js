const keys = {};
let lastMoveTime = 0;
const MOVE_DELAY = 50; // ms between moves

function setupInput(ws, playerId) {
  console.log('🎮 Setting up input handler for player:', playerId);
  
  // Use window instead of document to capture all keyboard events
  window.addEventListener('keydown', (e) => {
    // Don't capture if typing in an input field
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    
    console.log('🔑 Key down:', e.key);
    e.preventDefault(); // Prevent page scrolling with arrow keys
    
    const now = Date.now();
    if (now - lastMoveTime < MOVE_DELAY) return;
    lastMoveTime = now;
    
    keys[e.key] = true;
    sendMove(ws, playerId);
  });

  window.addEventListener('keyup', (e) => {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    console.log('🔑 Key up:', e.key);
    keys[e.key] = false;
  });
}

function sendMove(ws, playerId) {
  let dx = 0, dy = 0;
  if (keys['w'] || keys['ArrowUp']) dy -= 10;
  if (keys['s'] || keys['ArrowDown']) dy += 10;
  if (keys['a'] || keys['ArrowLeft']) dx -= 10;
  if (keys['d'] || keys['ArrowRight']) dx += 10;

  console.log('🎮 Calculating move:', { dx, dy });

  if (dx === 0 && dy === 0) return;

  const message = {
    type: 'move',
    playerId: playerId,
    deltaX: dx,
    deltaY: dy,
  };
  
  console.log('📤 Sending move:', message);
  ws.send(JSON.stringify(message));
}