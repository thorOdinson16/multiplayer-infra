const canvas = document.getElementById('game-canvas');
const ctx = canvas.getContext('2d');

const WORLD_W = 1000;
const WORLD_H = 1000;
const COLORS = ['#e94560', '#0f3460', '#16c79a', '#f5a623', '#7b2ff7', '#00b4d8', '#ff6b6b', '#48dbfb'];

function renderGame(gameState, myPlayerId) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  // Draw grid
  ctx.strokeStyle = '#1a3a1a';
  ctx.lineWidth = 0.5;
  for (let x = 0; x < canvas.width; x += 40) {
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, canvas.height);
    ctx.stroke();
  }
  for (let y = 0; y < canvas.height; y += 40) {
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(canvas.width, y);
    ctx.stroke();
  }

  // Draw players
  const players = gameState.players || {};
  const scaleX = canvas.width / WORLD_W;
  const scaleY = canvas.height / WORLD_H;
  let i = 0;

  for (const [id, p] of Object.entries(players)) {
    if (!p.connected && id !== myPlayerId) continue;

    const x = p.x * scaleX;
    const y = p.y * scaleY;
    const color = COLORS[i % COLORS.length];
    const isMe = id === myPlayerId;

    // Player circle
    ctx.beginPath();
    ctx.arc(x, y, isMe ? 16 : 12, 0, Math.PI * 2);
    ctx.fillStyle = isMe ? '#fff' : color;
    ctx.fill();
    ctx.strokeStyle = isMe ? color : '#fff';
    ctx.lineWidth = 2;
    ctx.stroke();

    // Username label
    ctx.fillStyle = '#fff';
    ctx.font = '10px Courier New';
    ctx.textAlign = 'center';
    ctx.fillText(p.username || id.substring(0, 6), x, y - 20);

    // Health bar
    if (p.health !== undefined) {
      const barW = 30;
      const barH = 4;
      const hpPercent = p.health / 100;
      ctx.fillStyle = '#333';
      ctx.fillRect(x - barW / 2, y - 24, barW, barH);
      ctx.fillStyle = hpPercent > 0.5 ? '#16c79a' : hpPercent > 0.25 ? '#f5a623' : '#e94560';
      ctx.fillRect(x - barW / 2, y - 24, barW * hpPercent, barH);
    }

    i++;
  }
}