'use strict';
const messages = document.getElementById('messages');
const status = document.getElementById('status');
const count = document.getElementById('count');
const toggle = document.getElementById('toggle');
const latest = document.getElementById('latest');
const rows = [];
let source;
let cursor;
let received = 0;
let paused = false;
let retry;

function stop(text) {
  clearTimeout(retry);
  source.close();
  paused = true;
  toggle.textContent = 'Resume';
  status.textContent = text;
}

function start() {
  clearTimeout(retry);
  const url = new URL('/v1/blockhashes', location.origin);
  if (cursor) url.searchParams.set('after', cursor);
  source = new EventSource(url);
  paused = false;
  toggle.disabled = false;
  toggle.textContent = 'Pause';
  latest.hidden = true;
  status.textContent = 'Connecting…';
  source.onopen = () => { status.textContent = 'Connected'; };
  source.onerror = () => {
    if (source.readyState === EventSource.CLOSED) {
      stop('Connection closed. You can start from latest.');
      latest.hidden = false;
    } else {
      // Reconnect with the last cursor in a new URL. Native EventSource would
      // otherwise keep an old ?after while sending a newer Last-Event-ID.
      source.close();
      status.textContent = 'Reconnecting…';
      retry = setTimeout(() => { if (!paused) start(); }, 1500);
    }
  };
  for (const type of ['block', 'root', 'invalidate']) {
    source.addEventListener(type, event => {
      cursor = event.lastEventId;
      rows.unshift(`id: ${event.lastEventId}\nevent: ${type}\ndata: ${event.data}`);
      if (rows.length > 100) rows.pop();
      messages.textContent = rows.join('\n\n');
      received++;
      count.textContent = `${received} messages`;
      status.textContent = 'Connected';
    });
  }
  source.addEventListener('status', event => {
    const data = JSON.parse(event.data);
    status.textContent = data.stale ? 'Waiting for source' : 'Connected';
  });
  source.addEventListener('gap', event => {
    const data = JSON.parse(event.data);
    stop(`Stream stopped: ${data.error}. You can start from latest.`);
    toggle.disabled = true;
    latest.hidden = false;
  });
}
toggle.addEventListener('click', () => {
  if (paused) start();
  else stop('Paused');
});
latest.addEventListener('click', () => {
  source.close();
  cursor = undefined;
  start();
});
window.addEventListener('pagehide', () => stop('Paused'));
start();
