async function refresh() {
  const status = await chrome.runtime.sendMessage({type: 'status'});
  document.querySelector('#status').textContent = status.connected ? 'Подключено к локальному боту' : status.enabled ? 'Жду запуска локального бота' : 'Подключение выключено';
}
document.querySelector('#connect').onclick = async () => { await chrome.runtime.sendMessage({type:'connect'}); await refresh(); };
document.querySelector('#disconnect').onclick = async () => { await chrome.runtime.sendMessage({type:'disconnect'}); await refresh(); };
refresh();
