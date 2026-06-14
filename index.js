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

    // 1. BINANCE
    connectExch('Binance', 'wss://fstream.binance.com/ws/!forceOrder@arr', () => {}, (ws, d) => {
        const proc = (i) => { if(i.e === "forceOrder") { 
            stats.total++; 
            broadcast({ exch: 'Binance', symbol: i.o.s, side: i.o.S, value: Math.round(i.o.q * i.o.p) }); 
        }};
        if(Array.isArray(d)) d.forEach(proc); else proc(d);
    });

    // 2. BYBIT
    connectExch('Bybit', 'wss://stream.bytick.com/v5/public/linear', (ws) => {
        ws.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.linear"]}));
        ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000);
    }, (ws, d) => {
        if (d.data) {
            stats.total++;
            broadcast({ exch: 'Bybit', symbol: d.data.symbol, side: d.data.side, value: Math.round(d.data.size * d.data.price) });
        }
    });

    // 3. OKX
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public', (ws) => {
        ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]}));
        ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
    }, (ws, d) => {
        if (d.data && d.data[0]) {
            stats.total++;
            broadcast({ exch: 'OKX', symbol: d.data[0].instId, side: d.data[0].side, value: Math.round(d.data[0].sz * d.data[0].bkPx) });
        }
    });
};

wss.on('connection', (ws) => {
    clients.add(ws);
    startEngines();
    ws.on('close', () => { clients.delete(ws); if (clients.size === 0) { engineActive=false; remoteSockets.forEach(s=>s.close()); remoteSockets.clear(); } });
});

setInterval(() => {
    if (clients.size > 0) {
        broadcast({ type: 'ping', status: stats });
        console.log(`--- [LOG] Captures:${stats.total} | Clients:${clients.size} ---`);
    }
}, 20000);

server.listen(port, () => console.log(`Palace LIVE on ${port}`));

function getHTML() {
    return `
<!DOCTYPE html><html><head><title>FORBIDDEN</title>
<style>
    body { background: #000; color: #fff; font-family: monospace; padding: 20px; text-transform: uppercase; overflow: hidden; }
    .header { display: flex; justify-content: space-between; border-bottom: 2px solid #222; padding-bottom: 10px; }
    #mon { font-size: 10px; color: #555; margin: 10px 0; }
    #f { height: 80vh; overflow-y: auto; display: flex; flex-direction: column; gap: 4px; border: 1px solid #111; padding: 10px; }
    .row { display: grid; grid-template-columns: 80px 100px 120px 80px 1fr; background: #080808; padding: 8px; border-left: 2px solid #444; }
    .BUY, .Buy, .buy { border-left-color: #0f8; color: #0f8; }
    .SELL, .Sell, .sell { border-left-color: #f44; color: #f44; }
</style></head>
<body>
    <div class="header"><div>🏰 PALACE</div><div id="status" style="color:red">BOOTING...</div></div>
    <div id="mon">WAITING FOR HEARTBEAT...</div>
    <div id="f"></div>
    <script>
        const ws = new WebSocket(location.origin.replace('http','ws')), f = document.getElementById('f'), m = document.getElementById('mon'), s = document.getElementById('status');
        ws.onopen = () => { s.innerText = "YES IT'S LIVE BUT FORBIDDEN"; };
        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            if (d.type === 'ping') {
                m.innerText = JSON.stringify(d.status);
                return;
            }
            const r = document.createElement('div');
            r.className = 'row ' + d.side;
            r.innerHTML = "<span>"+new Date().toLocaleTimeString()+"</span><span>["+d.exch+"]</span><span>"+d.symbol+"</span><span>"+d.side+"</span><span style='text-align:right'>$"+d.value.toLocaleString()+"</span>";
            f.insertBefore(r, f.firstChild);
            if (f.children.length > 50) f.removeChild(f.lastChild);
        };
    </script>
</body></html>`;
}
