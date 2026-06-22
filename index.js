const WebSocket = require('ws');
const http = require('http');

const TARGET_COINS = new Set([
    "BTCUSDT", "ETHUSDT", "DOTUSDT", "HBARUSDT", "XRPUSDT", "LINKUSDT", "ARBUSDT",
    "BNBUSDT", "SOLUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT", "AVAXUSDT", "MATICUSDT",
    "SHIBUSDT", "LTCUSDT", "UNIUSDT", "BCHUSDT", "ICPUSDT", "ETCUSDT", "NEARUSDT",
    "ATOMUSDT", "OPUSDT", "XLMUSDT", "FILUSDT", "INJUSDT", "IMXUSDT", "APTUSDT",
    "CROUSDT", "LDOUSDT", "VETUSDT", "MKRUSDT", "GRTUSDT", "RNDRUSDT", "SUIUSDT",
    "AAVEUSDT", "ALGOUSDT", "EGLDUSDT", "AXSUSDT", "SANDUSDT", "MANAUSDT", "FTMUSDT",
    "THETAUSDT", "XTZUSDT", "SNXUSDT", "NEOUSDT", "FLOWUSDT", "KAVAUSDT", "MINAUSDT",
    "GALAUSDT", "APEUSDT", "DYDXUSDT", "LUNA2USDT", "EOSUSDT", "TWTUSDT", "ZILUSDT",
    "CRVUSDT", "GMTUSDT", "1INCHUSDT", "COMPUSDT", "STXUSDT", "XMRUSDT", "RUNEUSDT",
    "KLAYUSDT", "ARUSDT", "FETUSDT", "PAXGUSDT", "WLDUSDT", "WAVESUSDT", "ZECUSDT",
    "CAKEUSDT", "SEIUSDT", "GMXUSDT", "FXSUSDT", "DASHUSDT", "ENSUSDT", "PEPEUSDT",
    "CFXUSDT", "MASKUSDT", "ROSEUSDT", "LRCUSDT", "CVXUSDT", "WOOUSDT", "CELOUSDT",
    "IOTXUSDT", "FLOKIUSDT", "AGIXUSDT", "KSMUSDT", "CHZUSDT", "OCEANUSDT", "SUSHIUSDT",
    "BATUSDT", "BANDUSDT", "QTUMUSDT", "ANKRUSDT", "IOTAUSDT", "ENJUSDT", "YFIUSDT",
    "ONEUSDT", "STORJUSDT"
]);

const normalize = (s) => s.replace(/[-_]/g, '').replace('SWAP', '').replace('XBT', 'BTC').toUpperCase();
const port = process.env.PORT || 10000;
const server = http.createServer((req, res) => { res.writeHead(200); res.end('PALACE v7 LIVE'); });
const wss = new WebSocket.Server({ server });

let liquidationHistory = [];
let tickerMap = {}; 
let stats = { Binance: 'OFF', Bybit: 'OFF', OKX: 'OFF', total: 0 };
let clients = new Set();
let engineActive = false;
let remoteSockets = new Map();
let sleepTimer = null;

const broadcast = (p) => { const d = JSON.stringify(p); clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(d); }); };

const trackAndBroadcast = (p) => {
    stats.total++;
    liquidationHistory.push(p);
    if (liquidationHistory.length > 1000) liquidationHistory.shift();
    broadcast(p);
};

const connectExch = (name, url, onOpen, onMsg) => {
    const ws = new WebSocket(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    ws.on('open', () => { stats[name] = 'LIVE'; console.log(`>>> [${name}] CONNECTED`); onOpen(ws); });
    ws.on('message', (m) => { const txt = m.toString(); if (txt === "pong" || txt === "") return; try { onMsg(ws, JSON.parse(txt)); } catch(e){} });
    ws.on('error', (e) => { stats[name] = 'ERROR'; console.log(`!!! [${name}] ERR:`, e.message); });
    ws.on('close', () => { stats[name] = 'OFF'; if (engineActive) setTimeout(() => { if (engineActive) connectExch(name, url, onOpen, onMsg); }, 5000); });
    remoteSockets.set(name, ws);
};

const startEngines = () => {
    if (engineActive) return;
    engineActive = true;
    console.log('>>> IGNITION SEQUENCE');
    connectExch('BinanceLiq', 'wss://fstream.binance.com/market/ws/!forceOrder@arr', () => {}, (ws, d) => {
        const proc = (i) => { if(i.e === "forceOrder") { const sym = normalize(i.o.s); if (TARGET_COINS.has(sym)) trackAndBroadcast({ exch: 'Binance', symbol: sym, side: i.o.S.toLowerCase(), value: parseFloat(i.o.ap || i.o.p) * parseFloat(i.o.q), price: parseFloat(i.o.ap || i.o.p), quantity: parseFloat(i.o.q) }); }};
        if(Array.isArray(d)) d.forEach(proc); else proc(d);
    });
    connectExch('BinanceTickers', 'wss://fstream.binance.com/ws/!ticker@arr', () => {}, (ws, d) => { if (Array.isArray(d)) d.forEach(t => { const sym = normalize(t.s); if (TARGET_COINS.has(sym)) tickerMap[sym] = { p: parseFloat(t.c), v: parseFloat(t.q), c: parseFloat(t.P) }; }); });
    connectExch('Bybit', 'wss://stream.bybit.com/v5/public/linear', (ws) => { const c = Array.from(TARGET_COINS); for (let i = 0; i < c.length; i += 10) ws.send(JSON.stringify({"op": "subscribe", "args": c.slice(i, i + 10).map(x => `liquidation.${x}`)})); ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000); }, (ws, d) => { if (d.topic && d.topic.startsWith("liquidation") && d.data) { const items = Array.isArray(d.data) ? d.data : [d.data]; items.forEach(item => { const sym = normalize(item.symbol || item.s); if (TARGET_COINS.has(sym)) trackAndBroadcast({ exch: 'Bybit', symbol: sym, side: item.side.toLowerCase(), value: parseFloat(item.price || item.p) * parseFloat(item.size || item.v), price: parseFloat(item.price || item.p), quantity: parseFloat(item.size || item.v) }); }); } });
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public', (ws) => { ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]})); ws.pingTimer = setInterval(() => ws.send("ping"), 20000); }, (ws, d) => { if (d.data) d.data.forEach(i => { const sym = normalize(i.instId); if (TARGET_COINS.has(sym)) trackAndBroadcast({ exch: 'OKX', symbol: sym, side: i.side.toLowerCase(), value: parseFloat(i.bkPx) * parseFloat(i.sz), price: parseFloat(i.bkPx), quantity: parseFloat(i.sz) }); }); });
};

const stopEngines = () => { console.log('>>> AUTO-SLEEP ACTIVATED'); engineActive = false; remoteSockets.forEach(ws => { if (ws.pingTimer) clearInterval(ws.pingTimer); ws.terminate(); }); remoteSockets.clear(); };

setInterval(() => {
    clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.subscribedCoins && client.subscribedCoins.size > 0) {
            let up = {}; client.subscribedCoins.forEach(s => { if (tickerMap[s]) up[s] = tickerMap[s]; });
            if (Object.keys(up).length > 0) client.send(JSON.stringify({ type: 'ticker_batch', data: up }));
        }
    });
}, 3000);

wss.on('connection', (ws) => {
    console.log('>>> Client Connected');
    if (sleepTimer) { clearTimeout(sleepTimer); sleepTimer = null; }
    clients.add(ws);
    ws.subscribedCoins = new Set();
    startEngines();
    liquidationHistory.slice(-50).forEach(l => ws.send(JSON.stringify(l)));
    ws.on('message', (m) => { try { const c = JSON.parse(m.toString()); if (c.op === 'subscribe_tickers') ws.subscribedCoins = new Set(c.args.map(s => s.toUpperCase())); } catch(e){} });
    ws.on('close', () => { 
        clients.delete(ws); 
        console.log('>>> Client Left');
        if (clients.size === 0) sleepTimer = setTimeout(() => { if (clients.size === 0) stopEngines(); }, 300000); 
    });
});

setInterval(() => { console.log(`[HEARTBEAT] Apps: ${clients.size} | Engine: ${engineActive ? 'ON' : 'SLEEP'} | Liq: ${stats.total}`); }, 30000);
server.listen(port, () => console.log(`PALACE v7 ON ${port}`));
