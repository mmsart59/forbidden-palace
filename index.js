const WebSocket = require('ws');
const http = require('http');

// --- 1. THE SACRED COINS ---
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
    res.end('PALACE DATA ENGINE IS LIVE');
});
const wss = new WebSocket.Server({ server });

let stats = { Binance: 'OFF', Bybit: 'OFF', OKX: 'OFF', total: 0 };
let clients = new Set();
let engineActive = false;
let remoteSockets = new Map();

const broadcast = (payload) => {
    const data = JSON.stringify(payload);
    clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
};

const connectExch = (name, url, onOpen, onMsg) => {
    const ws = new WebSocket(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    ws.on('open', () => { stats[name] = 'LIVE'; onOpen(ws); });
    ws.on('message', (m) => {
        const txt = m.toString();
        if (txt === "pong") return;
        try { onMsg(ws, JSON.parse(txt)); } catch(e){}
    });
    ws.on('error', () => { stats[name] = 'ERROR'; });
    ws.on('close', () => {
        stats[name] = 'OFF';
        if (engineActive) setTimeout(() => connectExch(name, url, onOpen, onMsg), 5000);
    });
    remoteSockets.set(name, ws);
};

const startEngines = () => {
    if (engineActive) return;
    engineActive = true;
    console.log('>>> IGNITION: CONNECTING TO GLOBAL EXCHANGES...');

    // 1. BINANCE
    connectExch('Binance', 'wss://fstream.binance.com/ws/!forceOrder@arr', () => {}, (ws, d) => {
        const proc = (i) => { if(i.e === "forceOrder") {
            const sym = normalize(i.o.s);
            if (TARGET_COINS.has(sym)) {
                stats.total++;
                broadcast({ exch: 'Binance', symbol: sym, side: i.o.S === 'BUY' ? 'short' : 'long', value: Math.round(i.o.q * i.o.p) });
                console.log(`[CAPTURE] Binance: ${sym} $${Math.round(i.o.q * i.o.p)}`);
            }
        }};
        if(Array.isArray(d)) d.forEach(proc); else proc(d);
    });

    // 2. BYBIT
    connectExch('Bybit', 'wss://stream.bytick.com/v5/public/linear',
        (ws) => {
            ws.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.linear"]}));
            ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000);
        },
        (ws, d) => {
            if (d.data) {
                const item = d.data;
                const sym = normalize(item.symbol);
                if (TARGET_COINS.has(sym)) {
                    stats.total++;
                    const val = Math.round(item.size * item.price);
                    broadcast({ exch: 'Bybit', symbol: sym, side: item.side === 'Buy' ? 'short' : 'long', value: val });
                    console.log(`[CAPTURE] Bybit: ${sym} $${val}`);
                }
            }
        }
    );

    // 3. OKX
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public',
        (ws) => {
            ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]}));
            ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
        },
        (ws, d) => {
            if (d.data && d.data[0]) {
                const i = d.data[0];
                const sym = normalize(i.instId);
                if (TARGET_COINS.has(sym)) {
                    stats.total++;
                    const val = Math.round(i.sz * i.bkPx);
                    broadcast({ exch: 'OKX', symbol: sym, side: i.side === 'buy' ? 'short' : 'long', value: val });
                    console.log(`[CAPTURE] OKX: ${sym} $${val}`);
                }
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
    startEngines();
    console.log(`>>> APP CONNECTED. Active Listeners: ${clients.size}`);
    ws.on('close', () => {
        clients.delete(ws);
        console.log(`>>> APP DISCONNECTED. Active Listeners: ${clients.size}`);
        if (clients.size === 0) stopEngines();
    });
});

setInterval(() => {
    if (clients.size > 0) {
        broadcast({ type: 'ping' });
        console.log(`[STATUS] Captures: ${stats.total} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX}`);
    }
}, 30000);

server.listen(port, () => console.log(`DATA ENGINE READY ON PORT ${port}`));
