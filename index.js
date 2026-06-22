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
    res.end('PALACE SMART ENGINE v3 ACTIVE');
});
const wss = new WebSocket.Server({ server });

// --- MEMORY & STATE ---
const MAX_HISTORY = 1000;
let liquidationHistory = [];
let tickerMap = {}; // symbol -> { p, v, c }
let stats = { Binance: 'OFF', Bybit: 'OFF', OKX: 'OFF', total: 0 };
let engineActive = false;
let remoteSockets = new Map();

const broadcast = (payload) => {
    const data = JSON.stringify(payload);
    wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
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

    // 1. BINANCE LIQUIDATIONS
    connectExch('BinanceLiq', 'wss://fstream.binance.com/market/ws/!forceOrder@arr', () => {}, (ws, d) => {
        const process = (i) => { if(i.e === "forceOrder") {
            const sym = normalize(i.o.s);
            if (TARGET_COINS.has(sym)) {
                const p = parseFloat(i.o.ap || i.o.p);
                const q = parseFloat(i.o.q);
                const liq = { exch: 'Binance', symbol: sym, side: i.o.S.toLowerCase(), value: p * q, price: p, quantity: q };
                liquidationHistory.push(liq);
                if (liquidationHistory.length > MAX_HISTORY) liquidationHistory.shift();
                broadcast(liq);
                stats.total++;
            }
        }};
        if(Array.isArray(d)) d.forEach(process); else process(d);
    });

    // 2. BINANCE TICKERS (Proxy Engine)
    connectExch('BinanceTickers', 'wss://fstream.binance.com/ws/!ticker@arr', () => {}, (ws, d) => {
        if (!Array.isArray(d)) return;
        d.forEach(t => {
            const sym = normalize(t.s);
            if (TARGET_COINS.has(sym)) {
                tickerMap[sym] = { p: parseFloat(t.c), v: parseFloat(t.q), c: parseFloat(t.P) };
            }
        });
    });

    // 3. BYBIT LIQUIDATIONS
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
                const item = d.data;
                const sym = normalize(item.symbol);
                if (TARGET_COINS.has(sym)) {
                    const p = parseFloat(item.price);
                    const q = parseFloat(item.size);
                    const liq = { exch: 'Bybit', symbol: sym, side: item.side.toLowerCase(), value: p * q, price: p, quantity: q };
                    liquidationHistory.push(liq);
                    if (liquidationHistory.length > MAX_HISTORY) liquidationHistory.shift();
                    broadcast(liq);
                    stats.total++;
                }
            }
        }
    );

    // 4. OKX LIQUIDATIONS
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public',
        (ws) => {
            ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]}));
            ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
        },
        (ws, d) => {
            if (d.data) {
                d.data.forEach(i => {
                    const sym = normalize(i.instId);
                    if (TARGET_COINS.has(sym)) {
                        const p = parseFloat(i.bkPx);
                        const q = parseFloat(i.sz);
                        const liq = { exch: 'OKX', symbol: sym, side: i.side.toLowerCase(), value: p * q, price: p, quantity: q };
                        liquidationHistory.push(liq);
                        if (liquidationHistory.length > MAX_HISTORY) liquidationHistory.shift();
                        broadcast(liq);
                        stats.total++;
                    }
                });
            }
        }
    );
};

// --- SELECTIVE BROADCAST (Ticker Engine) ---
setInterval(() => {
    wss.clients.forEach(client => {
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

wss.on('connection', (ws) => {
    ws.subscribedCoins = new Set();
    startEngines();

    // Send history snippet
    liquidationHistory.slice(-50).forEach(l => ws.send(JSON.stringify(l)));

    ws.on('message', (msg) => {
        try {
            const cmd = JSON.parse(msg.toString());
            if (cmd.op === 'subscribe_tickers') {
                ws.subscribedCoins = new Set(cmd.args.map(s => s.toUpperCase()));
            }
        } catch(e){}
    });

    ws.on('close', () => {
        if (wss.clients.size === 0) {
            engineActive = false;
            remoteSockets.forEach(s => s.close());
            remoteSockets.clear();
        }
    });
});

server.listen(port, () => console.log(`SMART ENGINE ON ${port}`));
