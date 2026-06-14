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
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(getHTML());
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
        if (txt === "pong") return; // CRITICAL: Stop the SyntaxError crash
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

    // 1. BINANCE
    connectExch('Binance', 'wss://fstream.binance.com/ws/!forceOrder@arr', 
        () => { console.log('>>> [BINANCE] GATE OPEN'); }, 
        (ws, d) => {
            const proc = (i) => { if(i.e === "forceOrder") { 
                const sym = normalize(i.o.s);
                if(TARGET_COINS.has(sym)) {
                    stats.total++; 
                    broadcast({ exch: 'Binance', symbol: sym, side: i.o.S === 'BUY' ? 'short' : 'long', value: Math.round(i.o.q * i.o.p) }); 
                }
            }};
            if(Array.isArray(d)) d.forEach(proc); else proc(d);
        }
    );

    // 2. BYBIT
    connectExch('Bybit', 'wss://stream.bytick.com/v5/public/linear', 
        (ws) => {
            console.log('>>> [BYBIT] GATE OPEN');
            ws.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.linear"]}));
            ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000);
        }, 
        (ws, d) => {
            if (d.data) {
                const sym = normalize(d.data.symbol);
                if(TARGET_COINS.has(sym)) {
                    stats.total++;
                    broadcast({ exch: 'Bybit', symbol: sym, side: d.data.side === 'Buy' ? 'short' : 'long', value: Math.round(d.data.size * d.data.price) });
                }
            }
        }
    );

    // 3. OKX
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public', 
        (ws) => {
            console.log('>>> [OKX] GATE OPEN');
            ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]}));
            ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
        }, 
        (ws, d) => {
            if (d.data && d.data[0]) {
                const i = d.data[0];
                const sym = normalize(i.instId);
                if(TARGET_COINS.has(sym)) {
                    stats.total++;
                    broadcast({ exch: 'OKX', symbol: sym, side: i.side === 'buy' ? 'short' : 'long', value: Math.round(i.sz * i.bkPx) });
                }
            }
        }
    );
};

const stopEngines = () => {
    engineActive = false;
    remoteSockets.forEach(ws => { clearInterval(ws.pingTimer); ws.close(); });
    remoteSockets.clear();
};

wss.on('connection', (ws) => {
    clients.add(ws);
    startEngines();
    ws.on('close', () => { clients.delete(ws); if (clients.size === 0) stopEngines(); });
});

setInterval(() => {
    if (clients.size > 0) {
        broadcast({ type: 'ping' });
        console.log(`--- [HEARTBEAT] Clients:${clients.size} | Total Captures:${stats.total} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX} ---`);
    }
}, 30000);

server.listen(port, () => console.log(`Palace LIVE on ${port}`));

function getHTML() {
    return `<!DOCTYPE html><html><head><title>FORBIDDEN PALACE</title><style>
    :root { --red: #ff3e3e; --green: #00ff9d; --bg: #030303; }
    body { background: var(--bg); color: #fff; font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-transform: uppercase; overflow: hidden; }
    .header { display: flex; justify-content: space-between; border-bottom: 1px solid #222; padding-bottom: 10px; margin-bottom: 20px; }
    #f { height: 80vh; overflow: hidden; display: flex; flex-direction: column; gap: 5px; }
    .row { display: grid; grid-template-columns: 100px 140px 120px 80px 1fr; background: #080808; padding: 12px; border-left: 2px solid #333; font-size: 13px; margin-bottom: 4px; }
    .short { border-left-color: var(--green); color: var(--green); }
    .long { border-left-color: var(--red); color: var(--red); }
    .source { color: #666; font-weight: bold; }
    </style></head><body>
    <div class="header">
        <div style="color: gold; font-weight: bold;">🏰 FORBIDDEN PALACE</div>
        <div id="status" style="color: #444; font-size: 10px;">CONNECTING...</div>
    </div>
    <div id="f"></div>
    <script>
        const wsUrl = (window.location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host;
        const ws = new WebSocket(wsUrl);
        const f = document.getElementById('f');
        const s = document.getElementById('status');
        
        ws.onopen = () => { s.innerText = "YES IT'S LIVE BUT FORBIDDEN"; s.style.color = "red"; };
        
        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            if (d.type === 'ping') return;
            
            const r = document.createElement('div');
            r.className = 'row ' + d.side;
            const time = new Date().toLocaleTimeString([], {hour12:false});
            r.innerHTML = "<span>"+time+"</span><span class='source'>["+d.exch.toUpperCase()+"]</span><span>"+d.symbol+"</span><span>"+d.side.toUpperCase()+"</span><span style='text-align:right;font-weight:bold'>$"+d.value.toLocaleString()+"</span>";
            
            f.insertBefore(r, f.firstChild);
            if (f.children.length > 40) f.removeChild(f.lastChild);
        };
        
        ws.onclose = () => { s.innerText = "CONNECTION CLOSED"; s.style.color = "white"; };
    </script></body></html>`;
}
