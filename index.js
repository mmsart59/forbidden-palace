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
const server = http.createServer((req, res) => {
    // RENDER WAKE-UP ENDPOINT
    if (req.url === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
        res.end('PONG');
        return;
    }

    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(getHTML());
});
const wss = new WebSocket.Server({
    server,
    clientTracking: true,
    maxPayload: 1024 * 1024 // 1MB Limit
});

// GLOBAL ERROR HANDLING
process.on('uncaughtException', (err) => console.error('FATAL EXCEPTION:', err));
process.on('unhandledRejection', (reason) => console.error('UNHANDLED REJECTION:', reason));

let stats = { Binance: 'OFF', Bybit: 'OFF', OKX: 'OFF', total: 0 };
let clients = new Set();
let engineActive = false;
let remoteSockets = new Map();

// --- PROXY DATA STORE ---
let tickerCache = {}; // Stores { symbol: { p, v, c } }
let subscribedTickers = new Set(); // Symbols currently needed by mobile clients
let tickerEngineActive = false;

const broadcast = (payload) => {
    try {
        const data = JSON.stringify(payload);
        clients.forEach(c => {
            if (c.readyState === WebSocket.OPEN) {
                try {
                    c.send(data);
                } catch (e) {
                    console.error('[SERVER] Broadcast send error:', e.message);
                    clients.delete(c);
                }
            }
        });
    } catch (e) {
        console.error('[SERVER] Broadcast stringify error:', e.message);
    }
};

const connectExch = (name, url, onOpen, onMsg) => {
    const ws = new WebSocket(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    ws.on('open', () => { stats[name] = 'LIVE'; onOpen(ws); });
    ws.on('message', (m) => { try { onMsg(ws, JSON.parse(m)); } catch(e){} });
    ws.on('error', () => { stats[name] = 'ERROR'; });
    ws.on('close', () => { 
        stats[name] = 'OFF'; 
        if (engineActive) setTimeout(() => connectExch(name, url, onOpen, onMsg), 5000); 
    });
    remoteSockets.set(name, ws);
};

const startEngines = () => {
    if (engineActive) {
        console.log('>>> [ENGINES] Already running, skipping init.');
        return;
    }
    engineActive = true;
    console.log('>>> [ENGINES] Starting Engines...');

    // 1. BINANCE (Correct Topic)
    connectExch('Binance', 'wss://fstream.binance.com/ws/!forceOrder@arr', 
        () => { console.log('>>> [BINANCE] GATE OPEN'); }, 
        (ws, d) => {
            const proc = (i) => { if(i.e === "forceOrder") { stats.total++; broadcast({ exch: 'Binance', symbol: normalize(i.o.s), side: i.o.S === 'BUY' ? 'short' : 'long', price: i.o.p, quantity: i.o.q, value: Math.round(i.o.q * i.o.p) }); } };
            if(Array.isArray(d)) d.forEach(proc); else proc(d);
        }
    );

    // 2. BYBIT (Corrected V5 Topic)
    connectExch('Bybit', 'wss://stream.bytick.com/v5/public/linear', 
        (ws) => {
            console.log('>>> [BYBIT] GATE OPEN');
            ws.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.linear"]})); // Global Linear Topic
            ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000);
        }, 
        (ws, d) => {
            if (d.data) {
                const item = d.data;
                stats.total++;
                broadcast({ exch: 'Bybit', symbol: normalize(item.symbol), side: item.side === 'Buy' ? 'short' : 'long', price: item.price, quantity: item.size, value: Math.round(item.size * item.price) });
            }
        }
    );

    // 3. OKX (Corrected instType)
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public', 
        (ws) => {
            console.log('>>> [OKX] GATE OPEN');
            ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]}));
            ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
        }, 
        (ws, d) => {
            if (d.data && d.data[0]) {
                const item = d.data[0];
                stats.total++;
                broadcast({ exch: 'OKX', symbol: normalize(item.instId), side: item.side === 'buy' ? 'short' : 'long', price: item.bkPx, quantity: item.sz, value: Math.round(item.sz * item.bkPx) });
            }
        }
    );
};

const stopEngines = () => {
    engineActive = false;
    remoteSockets.forEach(ws => { clearInterval(ws.pingTimer); ws.close(); });
    remoteSockets.clear();
};

const startTickerEngine = () => {
    if (tickerEngineActive) return;
    tickerEngineActive = true;
    console.log('>>> [PROXY] Starting Binance Ticker Engine...');

    const ws = new WebSocket('wss://fstream.binance.com/ws');

    ws.on('open', () => {
        console.log('>>> [PROXY] Binance Ticker Socket Open');
        updateTickerSubscriptions(ws);
    });

    ws.on('message', (data) => {
        const d = JSON.parse(data);
        if (d.e === "24hrTicker") {
            tickerCache[d.s] = { p: d.c, v: d.q, c: d.P };
        }
    });

    ws.on('close', () => {
        tickerEngineActive = false;
        setTimeout(startTickerEngine, 5000);
    });

    remoteSockets.set('BinanceTickers', ws);
};

const updateTickerSubscriptions = (ws) => {
    if (ws.readyState !== WebSocket.OPEN) return;
    const params = Array.from(subscribedTickers).map(s => `${s.toLowerCase()}@ticker`);
    if (params.length === 0) return;

    ws.send(JSON.stringify({
        method: "SUBSCRIBE",
        params: params,
        id: Date.now()
    }));
};

wss.on('connection', (ws) => {
    clients.add(ws);
    console.log(`[SERVER] Client connected. Total clients: ${clients.size}`);
    startEngines();
    startTickerEngine();

    ws.on('message', (msg) => {
        try {
            const j = JSON.parse(msg);
            if (j.op === 'subscribe_tickers') {
                j.args.forEach(s => subscribedTickers.add(s.toUpperCase()));
                const tickerWs = remoteSockets.get('BinanceTickers');
                if (tickerWs) updateTickerSubscriptions(tickerWs);
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        clients.delete(ws);
        console.log(`[SERVER] Client disconnected. Total clients: ${clients.size}`);
        // Removed stopEngines() to keep Palace alive even if no one is watching
    });
});

setInterval(() => {
    if (clients.size > 0) {
        broadcast({ type: 'ping' });

        // Broadcast Ticker Batch
        if (Object.keys(tickerCache).length > 0) {
            broadcast({ type: 'ticker_batch', data: tickerCache });
            tickerCache = {}; // Clear after send
        }

        console.log(`--- [HEARTBEAT] Clients:${clients.size} | Total Captures:${stats.total} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX} ---`);
    }
}, 10000); // 10s Batch Interval

server.listen(port, () => console.log(`Palace LIVE on ${port}`));

function getHTML() {
    return `<!DOCTYPE html><html><head><title>PALACE</title><style>body{background:#000;color:#fff;font-family:monospace;padding:20px;text-transform:uppercase;overflow:hidden;} .row{display:grid;grid-template-columns:100px 120px 120px 80px 1fr;background:#080808;padding:12px;border-left:2px solid #333;font-size:13px;margin-bottom:4px;} .short{border-left-color:#f44;color:#f44;} .long{border-left-color:#0f8;color:#0f8;} .dot{height:8px;width:8px;background:#444;border-radius:50%;display:inline-block;margin-right:10px;}</style></head><body><div style="display:flex;justify-content:space-between"><div><span class="dot" id="d"></span>🏰 FORBIDDEN PALACE</div><div style="color:#f44;font-size:10px">YES IT'S LIVE BUT FORBIDDEN</div></div><div id="f" style="margin-top:20px;height:80vh"></div><script>const ws=new WebSocket(location.origin.replace('http','ws')),f=document.getElementById('f'),d=document.getElementById('d');ws.onmessage=(e)=>{const j=JSON.parse(e.data);if(j.type==='ping'){d.style.background='#0f8';setTimeout(()=>{d.style.background='#444'},500);return;}const r=document.createElement('div');r.className='row '+j.side;r.innerHTML='<span>'+new Date().toLocaleTimeString([],{hour12:false})+'</span><span style="color:#666">['+j.exch.toUpperCase()+']</span><span>'+j.symbol+'</span><span>'+j.side+'</span><span style="text-align:right;font-weight:bold">$'+j.value.toLocaleString()+'</span>';f.insertBefore(r,f.firstChild);if(f.children.length>40)f.removeChild(f.lastChild);};ws.onclose=()=>{d.style.background='red'};</script></body></html>`;
}
