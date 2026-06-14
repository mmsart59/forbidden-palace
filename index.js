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
    ws.on('message', (m) => { try { onMsg(ws, JSON.parse(m)); } catch(e){} });
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
                const val = Math.round(parseFloat(i.o.q) * parseFloat(i.o.p));
                if (val > 100) { stats.total++; broadcast({ exch: 'Binance', symbol: normalize(i.o.s), side: i.o.S.toLowerCase(), value: val }); }
            }};
            if(Array.isArray(d)) d.forEach(proc); else proc(d);
        }
    );

    // 2. BYBIT
    connectExch('Bybit', 'wss://stream.bytick.com/v5/public/linear', 
        (ws) => {
            ws.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.linear"]}));
            ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000);
        }, 
        (ws, d) => {
            if (d.data) {
                const val = Math.round(parseFloat(d.data.size) * parseFloat(d.data.price));
                if (val > 100) { stats.total++; broadcast({ exch: 'Bybit', symbol: normalize(d.data.symbol), side: d.data.side.toLowerCase(), value: val }); }
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
                const val = Math.round(parseFloat(i.sz) * parseFloat(i.bkPx));
                if (val > 100) { stats.total++; broadcast({ exch: 'OKX', symbol: normalize(i.instId), side: i.side.toLowerCase(), value: val }); }
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
        console.log(`--- [HEARTBEAT] Captures:${stats.total} ---`);
    }
}, 20000);

server.listen(port, () => console.log(`Palace LIVE on ${port}`));

function getHTML() {
    return `
<!DOCTYPE html><html><head><title>FORBIDDEN PALACE</title>
<style>
    :root { --red: #ff3e3e; --green: #00ff9d; --bg: #030303; }
    body { background: var(--bg); color: #fff; font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-transform: uppercase; overflow: hidden; }
    .header { display: flex; justify-content: space-between; border-bottom: 1px solid #222; padding-bottom: 10px; }
    #mon { display: flex; gap: 15px; margin: 10px 0; font-size: 11px; color: #666; }
    .state-LIVE { color: var(--green); } .state-OFF { color: #444; }
    #f { height: 80vh; overflow-y: hidden; display: flex; flex-direction: column; gap: 4px; margin-top: 10px; }
    .row { display: grid; grid-template-columns: 80px 140px 120px 80px 1fr; background: #080808; padding: 10px; border-left: 2px solid #333; font-size: 12px; }
    .buy, .short { border-left-color: var(--green); color: var(--green); }
    .sell, .long { border-left-color: var(--red); color: var(--red); }
    .source { color: #888; font-weight: bold; }
</style></head>
<body>
    <div class="header">
        <div style="color: gold">🏰 FORBIDDEN PALACE</div>
        <div id="pulse" style="color: #444">● CONNECTION IDLE</div>
    </div>
    <div id="mon"></div>
    <div id="f"></div>
    <script>
        const ws = new WebSocket(location.origin.replace('http','ws')), f = document.getElementById('f'), m = document.getElementById('mon'), p = document.getElementById('pulse');
        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            // Visual Pulse
            p.style.color = '#00ff9d'; p.innerText = '● RECEIVING DATA';
            setTimeout(() => { p.style.color = '#444'; }, 500);

            if (d.type === 'ping') {
                m.innerHTML = Object.entries(d.status).map(([n,s]) => \`<span class="state-\${s}">\${n}:\${s}</span>\`).join('');
                return;
            }

            const r = document.createElement('div');
            r.className = 'row ' + d.side;
            const time = new Date().toLocaleTimeString([], {hour12:false});
            r.innerHTML = \`<span>\${time}</span><span class="source">[\${d.exch.toUpperCase()}]</span><span>\${d.symbol}</span><span>\${d.side.toUpperCase()}</span><span style="text-align:right;font-weight:bold">$\${d.value.toLocaleString()}</span>\`;
            f.insertBefore(r, f.firstChild);
            if (f.children.length > 40) f.removeChild(f.lastChild);
        };
        ws.onclose = () => { p.style.color = 'red'; p.innerText = '● CONNECTION LOST'; };
    </script>
</body></html>`;
}
