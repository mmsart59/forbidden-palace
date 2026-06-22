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
    res.end('PALACE v8 LIVE');
});

const wss = new WebSocket.Server({ server: server });

let stats = { Binance: 'OFF', Bybit: 'OFF', OKX: 'OFF', total: 0 };
let clients = new Set();
let engineActive = false;
let remoteSockets = new Map();
let liquidationHistory = [];
let tickerMap = {};
let sleepTimer = null;

const broadcast = (payload) => {
    const data = JSON.stringify(payload);
    clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(data);
        }
    });
};

const trackAndBroadcast = (payload) => {
    stats.total++;
    liquidationHistory.push(payload);
    if (liquidationHistory.length > 1000) liquidationHistory.shift();
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
            onMsg(ws, d);
        } catch (e) {}
    });

    ws.on('error', (e) => {
        stats[name] = 'ERROR';
        console.log(`!!! [${name}] ERR:`, e.message);
    });

    ws.on('close', () => {
        stats[name] = 'OFF';
        if (engineActive) {
            setTimeout(() => {
                if (engineActive) connectExch(name, url, onOpen, onMsg);
            }, 5000);
        }
    });

    remoteSockets.set(name, ws);
};

const startEngines = () => {
    if (engineActive) return;
    engineActive = true;
    console.log('>>> IGNITION SEQUENCE STARTED');

    // 1. BINANCE LIQUIDATIONS
    connectExch('BinanceLiq', 'wss://fstream.binance.com/market/ws/!forceOrder@arr', () => {}, (ws, d) => {
        const process = (i) => {
            if (i.e === "forceOrder") {
                const sym = normalize(i.o.s);
                if (TARGET_COINS.has(sym)) {
                    const p = parseFloat(i.o.ap || i.o.p);
                    const q = parseFloat(i.o.q);
                    trackAndBroadcast({ exch: 'Binance', symbol: sym, side: i.o.S.toLowerCase(), value: p * q, price: p, quantity: q });
                }
            }
        };
        if (Array.isArray(d)) d.forEach(process); else process(d);
    });

    // 2. BINANCE TICKERS
    connectExch('BinanceTickers', 'wss://fstream.binance.com/ws/!ticker@arr', () => {}, (ws, d) => {
        if (!Array.isArray(d)) return;
        d.forEach(t => {
            const sym = normalize(t.s);
            if (TARGET_COINS.has(sym)) {
                tickerMap[sym] = { p: parseFloat(t.c), v: parseFloat(t.q), c: parseFloat(t.P) };
            }
        });
    });

    // 3. BYBIT
    connectExch('Bybit', 'wss://stream.bybit.com/v5/public/linear',
        (ws) => {
            const coins = Array.from(TARGET_COINS);
            for (let i = 0; i < coins.length; i += 10) {
                const chunk = coins.slice(i, i + 10).map(c => `liquidation.${c}`);
                ws.send(JSON.stringify({ "op": "subscribe", "args": chunk }));
            }
            ws.pingTimer = setInterval(() => ws.send(JSON.stringify({ "op": "ping" })), 20000);
        },
        (ws, d) => {
            if (d.topic && d.topic.startsWith("liquidation") && d.data) {
                const items = Array.isArray(d.data) ? d.data : [d.data];
                items.forEach(item => {
                    const sym = normalize(item.symbol || item.s);
                    if (TARGET_COINS.has(sym)) {
                        const p = parseFloat(item.price || item.p);
                        const q = parseFloat(item.size || item.v);
                        trackAndBroadcast({ exch: 'Bybit', symbol: sym, side: item.side.toLowerCase(), value: p * q, price: p, quantity: q });
                    }
                });
            }
        }
    );

    // 4. OKX
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public',
        (ws) => {
            ws.send(JSON.stringify({ "op": "subscribe", "args": [{ "channel": "liquidation-orders", "instType": "SWAP" }] }));
            ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
        },
        (ws, d) => {
            if (d.data) {
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
    console.log('>>> AUTO-SLEEP ACTIVATED');
    engineActive = false;
    remoteSockets.forEach((ws) => {
        if (ws.pingTimer) clearInterval(ws.pingTimer);
        ws.terminate();
    });
    remoteSockets.clear();
};

// Selective Broadcast
setInterval(() => {
    clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN && client.subscribedCoins && client.subscribedCoins.size > 0) {
            let updates = {};
            client.subscribedCoins.forEach(sym => {
                if (tickerMap[sym]) updates[sym] = tickerMap[sym];
            });
            if (Object.keys(updates).length > 0) {
                client.send(JSON.stringify({ type: 'ticker_batch', data: updates }));
            }
        }
    });
}, 3000);

wss.on('connection', function connection(ws) {
    console.log('>>> NEW APP CLIENT CONNECTED');
    
    if (sleepTimer) {
        clearTimeout(sleepTimer);
        sleepTimer = null;
    }

    clients.add(ws);
    ws.subscribedCoins = new Set();
    
    startEngines();

    // Send history
    liquidationHistory.slice(-50).forEach(l => ws.send(JSON.stringify(l)));

    ws.on('message', function incoming(message) {
        try {
            const cmd = JSON.parse(message.toString());
            if (cmd.op === 'subscribe_tickers') {
                ws.subscribedCoins = new Set(cmd.args.map(s => s.toUpperCase()));
                console.log(`>>> Client subscribed to: ${Array.from(ws.subscribedCoins)}`);
            }
        } catch (e) {}
    });

    ws.on('close', function close() {
        console.log('>>> APP CLIENT DISCONNECTED');
        clients.delete(ws);
        if (clients.size === 0) {
            console.log('>>> Last client left. 5m countdown to sleep started...');
            sleepTimer = setTimeout(() => {
                if (clients.size === 0) stopEngines();
            }, 300000);
        }
    });
});

setInterval(() => {
    console.log(`[HEARTBEAT] Active Apps: ${clients.size} | Engine: ${engineActive ? 'ON' : 'SLEEPING'} | Total Liq: ${stats.total}`);
}, 30000);

server.listen(port, () => console.log(`PALACE v8 LIVE ON ${port}`));
