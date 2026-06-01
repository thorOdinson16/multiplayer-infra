function createGameConnection(url, handlers) {
  const ws = new WebSocket(url);
  const { onMessage, onClose, onError } = handlers;

  ws.onopen = () => {
    console.log('WebSocket connected:', url);
  };

  ws.onmessage = (event) => {
    console.log('Received message:', event.data);
    if (onMessage) onMessage(event.data);
  };

  ws.onclose = (event) => {
    console.log('WebSocket closed:', event.code, event.reason);
    if (onClose) onClose();
  };

  ws.onerror = (err) => {
    console.error('WebSocket error:', err);
    if (onError) onError();
  };

  return ws;
}