const WebSocket = require('ws');
const http = require('http');

// --- THE SACRED COINS ---
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
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
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
    console.log('>>> [PALACE] IGNITION');

    // 1. BINANCE
    connectExch('Binance', 'wss://fstream.binance.com/ws/!forceOrder@arr', () => {}, (ws, d) => {
        const proc = (i) => { if(i.e === "forceOrder") { 
            const sym = normalize(i.o.s);
            const val = Math.round(parseFloat(i.o.q) * parseFloat(i.o.p));
            if (TARGET_COINS.has(sym)) {
                stats.total++; 
                broadcast({ exch: 'Binance', symbol: sym, side: i.o.S.toLowerCase(), value: val }); 
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
                const sym = normalize(d.data.symbol);
                const val = Math.round(parseFloat(d.data.size) * parseFloat(d.data.price));
                if (TARGET_COINS.has(sym)) {
                    stats.total++;
                    broadcast({ exch: 'Bybit', symbol: sym, side: d.data.side.toLowerCase(), value: val });
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
                const sym = normalize(d.data[0].instId);
                const val = Math.round(parseFloat(d.data[0].sz) * parseFloat(d.data[0].bkPx));
                if (TARGET_COINS.has(sym)) {
                    stats.total++;
                    broadcast({ exch: 'OKX', symbol: sym, side: d.data[0].side.toLowerCase(), value: val });
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
        broadcast({ type: 'ping', status: stats });
        console.log(`--- [LOG] Total Capture:${stats.total} | Clients:${clients.size} | Status: B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX} ---`);
    }
}, 30000);

server.listen(port, () => console.log(`Palace LIVE on ${port}`));

function getHTML() {
    return `
<!DOCTYPE html><html><head>
<meta charset="UTF-8">
<title>PALACE</title>
<style>
    :root { --red: #ff3e3e; --green: #00ff9d; --bg: #030303; }
    body { background: var(--bg); color: #fff; font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-transform: uppercase; overflow: hidden; }
    .header { display: flex; justify-content: space-between; border-bottom: 1px solid #222; padding-bottom: 10px; margin-bottom: 20px; }
    #mon { font-size: 10px; color: #666; margin-bottom: 10px; }
    #f { height: 80vh; overflow-y: hidden; display: flex; flex-direction: column; gap: 5px; }
    .row { display: grid; grid-template-columns: 100px 120px 120px 80px 1fr; background: #080808; padding: 12px; border-left: 2px solid #333; font-size: 13px; }
    .buy, .short { border-left-color: var(--green); color: var(--green); }
    .sell, .long { border-left-color: var(--red); color: var(--red); }
</style></head>
<body>
    <div class="header">
        <div style="color: gold; font-weight: bold;">🏰 FORBIDDEN PALACE</div>
        <div id="status" style="color: var(--red); font-size: 10px;">YES IT'S LIVE BUT FORBIDDEN</div>
    </div>
    <div id="mon">WAITING FOR DATA...</div>
    <div id="f"></div>
    <script>
        const wsUrl = window.location.origin.replace(/^http/, 'ws');
        const ws = new WebSocket(wsUrl);
        const f = document.getElementById('f'), m = document.getElementById('mon');
        
        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            if (d.type === 'ping') {
                m.innerText = "CAPTURES: " + d.status.total + " | " + JSON.stringify(d.status);
                return;
            }
            const r = document.createElement('div');
            r.className = 'row ' + d.side;
            const time = new Date().toLocaleTimeString([], {hour12:false});
            r.innerHTML = "<span>"+time+"</span><span style='color:#888'>["+d.exch.toUpperCase()+"]</span><span>"+d.symbol+"</span><span>"+d.side.toUpperCase()+"</span><span style='text-align:right;font-weight:bold'>$"+d.value.toLocaleString()+"</span>";
            f.insertBefore(r, f.firstChild);
            if (f.children.length > 40) f.removeChild(f.lastChild);
        };
        ws.onerror = (err) => { console.error("Palace WS Error:", err); };
    </script>
</body></html>`;
}
