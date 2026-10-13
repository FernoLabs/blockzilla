(() => {
  const countdown = document.getElementById('epoch-countdown');
  const startTime = document.getElementById('epoch-start-time');
  if (!countdown?.dataset.startUnix) return;
  const startMs = Number(countdown.dataset.startUnix) * 1000;
  if (!Number.isFinite(startMs)) return;
  if (startTime) {
    startTime.textContent = new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Europe/Paris', weekday: 'short', day: 'numeric', month: 'short',
      hour: '2-digit', minute: '2-digit', timeZoneName: 'short', hourCycle: 'h23',
    }).format(new Date(startMs));
  }
  const tick = () => {
    const seconds = Math.max(0, Math.ceil((startMs - Date.now()) / 1000));
    if (!seconds) {
      countdown.textContent = 'Waiting for the next epoch';
      return;
    }
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    countdown.textContent = `${hours}h ${minutes}m ${seconds % 60}s`;
  };
  tick();
  setInterval(tick, 1000);
})();
