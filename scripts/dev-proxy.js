const http = require('http');
const httpProxy = require('http-proxy');
const fs = require('fs');
const path = require('path');

const NGINX_URL = process.env.NGINX_URL || 'http://localhost:8080';
const proxy = httpProxy.createProxyServer({
  ws: true  // Enable WebSocket proxying
});
const PROXY_PATHS = ['/auth', '/game', '/leaderboard', '/replay', '/reconnect', '/spectate'];
const CLIENT_DIR = path.join(__dirname, '../client/public');

const MIME = {
  '.html': 'text/html',
  '.js': 'application/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
};

const server = http.createServer((req, res) => {
  if (PROXY_PATHS.some(p => req.url.startsWith(p))) {
    proxy.web(req, res, { target: NGINX_URL }, (err) => {
      console.error('Proxy error:', err);
      res.writeHead(502);
      res.end('Proxy error: ' + err.message);
    });
    return;
  }

  const filePath = path.join(CLIENT_DIR, req.url === '/' ? 'index.html' : req.url);
  const ext = path.extname(filePath);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'text/plain' });
    res.end(data);
  });
});

// Handle WebSocket upgrade requests
server.on('upgrade', function (req, socket, head) {
  console.log('WebSocket upgrade request:', req.url);
  if (PROXY_PATHS.some(p => req.url.startsWith(p))) {
    proxy.ws(req, socket, head, { target: NGINX_URL });
  } else {
    socket.destroy();
  }
});

proxy.on('error', function(err, req, res) {
  console.error('Proxy error:', err);
  if (res && !res.headersSent) {
    res.writeHead(502, { 'Content-Type': 'text/plain' });
    res.end('Proxy error: ' + err.message);
  }
});

server.listen(3001, () => {
  console.log('Dev proxy on http://localhost:3001');
  console.log('Proxying API to ' + NGINX_URL);
  console.log('WebSocket proxying enabled');
});