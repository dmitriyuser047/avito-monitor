import {ENDPOINT, TOKEN} from './settings.js';
import {inspectDocument, fetchDocument} from './inspector.js';
let socket = null;
let connecting = false;
let navigation = null;
let lastResponse = null;
const pageUrl = url => {
  try { const u = new URL(url); return url === 'about:blank' || (u.protocol === 'https:' && ['avito.ru','www.avito.ru'].includes(u.hostname) && !u.username && !u.password && (!u.port || u.port === '443')); }
  catch { return false; }
};
const catalogUrl = url => {
  if (!pageUrl(url) || url === 'about:blank') return false;
  return new URL(url).pathname.split('/').filter(Boolean).length === 2;
};
function send(data) { if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(data)); }
async function ownedTab() {
  const {tabId} = await chrome.storage.session.get('tabId');
  if (Number.isInteger(tabId)) {
    try {
      const tab = await chrome.tabs.get(tabId);
      const frame = await chrome.webNavigation.getFrame({tabId,frameId:0});
      if (frame && pageUrl(frame.url)) return {...tab,url:frame.url};
    } catch {}
  }
  const tab = await chrome.tabs.create({url:'about:blank', active:false});
  await chrome.storage.session.set({tabId:tab.id});
  return tab;
}
async function connect() {
  if (connecting || socket && socket.readyState <= WebSocket.OPEN) return;
  const {enabled} = await chrome.storage.local.get('enabled');
  if (!enabled) return;
  connecting = true;
  try {
    const tab = await ownedTab();
    const ws = new WebSocket(ENDPOINT);
    socket = ws;
    ws.onopen = () => { send({type:'hello',token:TOKEN,url:tab.url || 'about:blank'}); };
    ws.onmessage = event => { let command; try {command = JSON.parse(event.data);} catch {return;} execute(command).catch(() => {}); };
    ws.onclose = () => { if (socket === ws) socket = null; };
    ws.onerror = () => { ws.close(); };
  } finally { connecting = false; }
}
async function execute(command) {
  try {
    const tab = await ownedTab();
    let result;
    if (command.operation === 'navigate') {
      if (!catalogUrl(command.url) || navigation) throw Error('Unsupported navigation');
      lastResponse = null;
      result = await new Promise(async (resolve, reject) => {
        const timer = setTimeout(() => { navigation = null; reject(Error('Navigation timeout')); }, 30000);
        navigation = {tabId:tab.id, resolve: value => {clearTimeout(timer); navigation=null; resolve(value);}, reject: error => {clearTimeout(timer);navigation=null;reject(error);}};
        try { await chrome.tabs.update(tab.id,{url:command.url}); } catch(error) {navigation?.reject(error);}
      });
    } else if (command.operation === 'inspect' || command.operation === 'fetch') {
      if (!pageUrl(tab.url) || tab.url === 'about:blank') throw Error('Open the Avito catalog first');
      if (command.operation === 'fetch' && !catalogUrl(command.url)) throw Error('Unsupported fetch');
      const results = await chrome.scripting.executeScript({target:{tabId:tab.id},world:'MAIN',
        func:command.operation === 'fetch' ? fetchDocument : inspectDocument,
        args:command.operation === 'fetch' ? [[command.url, false]] : []});
      result = results[0].result;
    } else throw Error('Unsupported operation');
    send({type:'result',id:command.id,result});
  } catch {send({type:'result',id:command.id,error:'operation_failed'});}
}
chrome.webRequest.onHeadersReceived.addListener(details => {
  if (navigation && details.tabId === navigation.tabId && details.type === 'main_frame') {
    const retry = details.responseHeaders?.find(h => h.name.toLowerCase() === 'retry-after')?.value;
    lastResponse = {status:details.statusCode,retry_after:retry ?? null,url:details.url};
  }
},{urls:['https://www.avito.ru/*','https://avito.ru/*'],types:['main_frame']},['responseHeaders']);
chrome.webNavigation.onCommitted.addListener(async details => {
  const {tabId} = await chrome.storage.session.get('tabId');
  if (details.tabId === tabId && details.frameId === 0) send({type:'navigation',url:details.url});
});
chrome.webNavigation.onDOMContentLoaded.addListener(details => {
  if (navigation && details.tabId === navigation.tabId && details.frameId === 0) {
    navigation.resolve({...lastResponse,url:details.url});
  }
});
chrome.webNavigation.onErrorOccurred.addListener(details => {
  if (navigation && details.tabId === navigation.tabId && details.frameId === 0) navigation.reject(Error('Navigation failed'));
});
chrome.tabs.onRemoved.addListener(async tabId => {
  const saved = await chrome.storage.session.get('tabId');
  if (saved.tabId === tabId) {await chrome.storage.session.remove('tabId');send({type:'tab_closed'});socket?.close();}
});
chrome.runtime.onMessage.addListener((message, sender, respond) => {
  if (sender.id !== chrome.runtime.id) return false;
  (async () => {
    if (message.type === 'connect') {await chrome.storage.local.set({enabled:true});await connect();}
    else if (message.type === 'disconnect') {await chrome.storage.local.set({enabled:false});socket?.close();}
    const {enabled} = await chrome.storage.local.get('enabled');
    respond({enabled:!!enabled,connected:socket?.readyState === WebSocket.OPEN});
  })().catch(() => respond({enabled:false,connected:false}));
  return true;
});
chrome.alarms.create('bridge-reconnect',{periodInMinutes:.5});
chrome.alarms.onAlarm.addListener(alarm => {if(alarm.name === 'bridge-reconnect')connect().catch(() => {});});
setInterval(() => {send({type:'heartbeat'});connect().catch(() => {});},10000);
chrome.runtime.onStartup.addListener(() => {connect().catch(() => {});});
connect().catch(() => {});
