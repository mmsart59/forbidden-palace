const WebSocket = require('ws');
const http = require('http');

// --- 1. THE 100 SACRED COINS ---
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
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('2026 PALACE ENGINE IS LIVE');
});
const wss = new WebSocket.Server({ server });

// --- MEMORY MANAGEMENT ---
const MAX_HISTORY = 1000; // Limit to 1000 items to prevent "collapsing"
let liquidationHistory = [];
let stats = { Binance: 'OFF', Bybit: 'OFF', OKX: 'OFF', total: 0 };
let clients = new Set();
let engineActive = false;
let remoteSockets = new Map();

const broadcast = (payload) => {
    const data = JSON.stringify(payload);
    clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
};

// New function to track data and manage memory
const trackAndBroadcast = (payload) => {
    stats.total++;
    // Add to history
    liquidationHistory.push(payload);
    // Cleanup old data
    if (liquidationHistory.length > MAX_HISTORY) {
        liquidationHistory.shift();
    }
    broadcast(payload);
};

const connectExch = (name, url, onOpen, onMsg) => {
    const ws = new WebSocket(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    ws.on('open', () => {
        stats[name] = 'LIVE';
        console.log(`>>> [${name}] CONNECTED`);
        onOpen(ws);
    });
    ws.on('message', (m) => {
        const txt = m.toString();
        if (txt === "pong" || txt === "") return;
        try {
            const d = JSON.parse(txt);
            if (d.e === "serverShutdown") { console.log(`!!! [${name}] SHUTDOWN WARNING`); return; }
            onMsg(ws, d);
        } catch(e){}
    });
    ws.on('error', (e) => {
        stats[name] = 'ERROR';
        console.log(`!!! [${name}] ERR:`, e.message);
    });
    ws.on('close', () => {
        stats[name] = 'OFF';
        if (engineActive) setTimeout(() => connectExch(name, url, onOpen, onMsg), 5000);
    });
    remoteSockets.set(name, ws);
};

const startEngines = () => {
    if (engineActive) return;
    engineActive = true;
    console.log('>>> 2026 IGNITION SEQUENCE');

    // 1. BINANCE (Aggregated Firehose)
    connectExch('Binance', 'wss://fstream.binance.com/market/ws/!forceOrder@arr', () => {}, (ws, d) => {
        const process = (i) => { if(i.e === "forceOrder") {
            const sym = normalize(i.o.s);
            if (TARGET_COINS.has(sym)) {
                const p = parseFloat(i.o.ap || i.o.p);
                const q = parseFloat(i.o.q);
                trackAndBroadcast({ exch: 'Binance', symbol: sym, side: i.o.S.toLowerCase(), value: p * q, price: p, quantity: q });
            }
        }};
        if(Array.isArray(d)) d.forEach(process); else process(d);
    });

    // 2. BYBIT (Fixed: Subscribing to specific coins)
    connectExch('Bybit', 'wss://stream.bybit.com/v5/public/linear',
        (ws) => {
            const coins = Array.from(TARGET_COINS);
            for (let i = 0; i < coins.length; i += 10) {
                const chunk = coins.slice(i, i + 10).map(c => `liquidation.${c}`);
                ws.send(JSON.stringify({"op": "subscribe", "args": chunk}));
            }
            ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000);
        },
        (ws, d) => {
            if (d.topic && d.topic.startsWith("liquidation") && d.data) {
                // Bybit V5 liquidation data can be an object or a single-item array
                const items = Array.isArray(d.data) ? d.data : [d.data];
                items.forEach(item => {
                    const sym = normalize(item.symbol || item.s);
                    if (TARGET_COINS.has(sym)) {
                        const p = parseFloat(item.price || item.p);
                        const q = parseFloat(item.size || item.v);
                        if (!isNaN(p) && !isNaN(q)) {
                            trackAndBroadcast({ exch: 'Bybit', symbol: sym, side: item.side.toLowerCase(), value: p * q, price: p, quantity: q });
                        }
                    }
                });
            }
        }
    );

    // 3. OKX (Public Swap Channel)
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public',
        (ws) => {
            ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]}));
            ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
        },
        (ws, d) => {
            if (d.data && d.data[0]) {
                d.data.forEach(i => {
                    const sym = normalize(i.instId);
                    if (TARGET_COINS.has(sym)) {
                        const p = parseFloat(i.bkPx);
                        const q = parseFloat(i.sz);
                        trackAndBroadcast({ exch: 'OKX', symbol: sym, side: i.side.toLowerCase(), value: p * q, price: p, quantity: q });
                    }
                });
            }
        }
    );
};

const stopEngines = () => {
    engineActive = false;
    remoteSockets.forEach(ws => { if(ws.pingTimer) clearInterval(ws.pingTimer); ws.close(); });
    remoteSockets.clear();
};

wss.on('connection', (ws) => {
    clients.add(ws);

    // Catch up: Send the last 50 liquidations to new client so they aren't empty
    liquidationHistory.slice(-50).forEach(item => ws.send(JSON.stringify(item)));

    startEngines();
    ws.on('close', () => {
        clients.delete(ws);
        if (clients.size === 0) stopEngines();
    });
});

setInterval(() => {
    if (clients.size > 0) {
        broadcast({ type: 'ping' });
        console.log(`[2026 HEARTBEAT] Clients: ${clients.size} | History: ${liquidationHistory.length} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX}`);
    }
}, 30000);

server.listen(port, () => console.log(`2026 ENGINE LIVE ON ${port}`));
