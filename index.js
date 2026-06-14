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

// Normalize symbols like "BTC-USDT-SWAP" -> "BTCUSDT"
const normalize = (s) => s.replace(/[-_]/g, '').replace('SWAP', '').replace('XBT', 'BTC').toUpperCase();

const port = process.env.PORT || 10000;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(getHTML());
});
const wss = new WebSocket.Server({ server });

// Global Palace State
let stats = { Binance: 'OFF', Bybit: 'OFF', OKX: 'OFF', total: 0 };
let clients = new Set();
let remoteSockets = [];

const broadcast = (exch, symbol, side, value) => {
    const cleanSymbol = normalize(symbol);
    const roundedValue = Math.round(value);
    
    // Captured in logs for proof of life
    if (roundedValue > 500) {
        console.log(`[${exch}] Captured: ${cleanSymbol} $${roundedValue}`);
    }

    if (TARGET_COINS.has(cleanSymbol) && roundedValue > 100) {
        stats.total++;
        const payload = JSON.stringify({ exch, symbol: cleanSymbol, side, value: roundedValue });
        clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(payload); });
    }
};

// --- MULTI-EXCHANGE ENGINE ---

const startEngines = () => {
    if (remoteSockets.length > 0) return;
    console.log('>>> [PALACE] INFILTRATING EXCHANGES...');

    // 1. BINANCE PRODUCTION
    const bn = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr', { headers: {'User-Agent': 'Mozilla/5.0'} });
    bn.on('open', () => { stats.Binance = 'LIVE'; console.log('>>> BINANCE: GATE OPEN'); });
    bn.on('message', (m) => {
        const d = JSON.parse(m);
        if (d.e === "forceOrder") broadcast('Binance', d.o.s, d.o.S === 'BUY' ? 'short' : 'long', parseFloat(d.o.q) * parseFloat(d.o.p));
    });
    bn.on('close', () => stats.Binance = 'OFF');
    remoteSockets.push(bn);

    // 2. BYBIT V5 PRODUCTION
    const bb = new WebSocket('wss://stream.bybit.com/v5/public/linear');
    bb.on('open', () => { 
        stats.Bybit = 'LIVE'; console.log('>>> BYBIT: GATE OPEN');
        // Subscribe to top pairs (Bybit requires manual sub)
        const topPairs = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "PEPEUSDT", "XRPUSDT"];
        bb.send(JSON.stringify({"op": "subscribe", "args": topPairs.map(p => `liquidation.${p}`)}));
    });
    bb.on('message', (m) => {
        const d = JSON.parse(m).data;
        if (d) broadcast('Bybit', d.symbol, d.side === 'Buy' ? 'short' : 'long', parseFloat(d.size) * parseFloat(d.price));
    });
    bb.on('close', () => stats.Bybit = 'OFF');
    remoteSockets.push(bb);

    // 3. OKX V5 PRODUCTION (GLOBAL STREAM)
    const ok = new WebSocket('wss://ws.okx.com:8443/ws/v5/public');
    ok.on('open', () => { 
        stats.OKX = 'LIVE'; console.log('>>> OKX: GATE OPEN');
        ok.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "ANY"}]}));
    });
    ok.on('message', (m) => {
        const d = JSON.parse(m).data;
        if (d && d[0]) broadcast('OKX', d[0].instId, d[0].side === 'buy' ? 'short' : 'long', parseFloat(d[0].sz) * parseFloat(d[0].bkPx));
    });
    ok.on('close', () => stats.OKX = 'OFF');
    remoteSockets.push(ok);
};

const stopEngines = () => {
    console.log('>>> [PALACE] EVACUATING (NO CLIENTS)');
    remoteSockets.forEach(s => { try { s.close(); } catch(e){} });
    remoteSockets = [];
    stats.Binance = stats.Bybit = stats.OKX = 'OFF';
};

// Palace Connection Manager
wss.on('connection', (ws) => {
    clients.add(ws);
    startEngines(); // Eco-mode: Wake up when you arrive
    console.log(`>>> Palace Infiltrated (Total: ${clients.size})`);
    
    ws.on('close', () => {
        clients.delete(ws);
        console.log(`>>> Client Evacuated (Left: ${clients.size})`);
        if (clients.size === 0) stopEngines(); // Eco-mode: Sleep when you leave
    });
});

// Keeper of Life
setInterval(() => {
    if (clients.size > 0) {
        clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(JSON.stringify({ type: 'ping' })); });
        console.log(`--- [HEARTBEAT] Clients:${clients.size} | Total Capture:${stats.total} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX} ---`);
    }
}, 30000);

server.listen(port, () => console.log(`FORBIDDEN PALACE LIVE ON ${port}`));

function getHTML() {
    return `
<!DOCTYPE html><html><head><title>FORBIDDEN PALACE</title>
<style>
    :root { --red: #ff3e3e; --green: #00ff9d; --bg: #030303; }
    body { background: var(--bg); color: #fff; font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-transform: uppercase; overflow: hidden; }
    .header { display: flex; justify-content: space-between; border-bottom: 1px solid #222; padding-bottom: 10px; }
    .dot { height: 8px; width: 8px; background-color: var(--green); border-radius: 50%; display: inline-block; margin-right: 10px; animation: blink 1s infinite; }
    @keyframes blink { 0% { opacity: 1; } 50% { opacity: 0.2; } 100% { opacity: 1; } }
    #feed { height: 80vh; overflow: hidden; display: flex; flex-direction: column; gap: 5px; margin-top: 20px; }
    .row { display: grid; grid-template-columns: 100px 120px 120px 80px 1fr; background: #080808; padding: 12px; border-left: 2px solid #333; font-size: 13px; }
    .short { border-left-color: var(--red); color: var(--red); }
    .long { border-left-color: var(--green); color: var(--green); }
    .source { color: #666; font-weight: bold; }
    .val { text-align: right; color: #fff; font-weight: bold; }
</style></head>
<body>
    <div class="header">
        <div><span class="dot" id="ping-dot"></span>🏰 FORBIDDEN PALACE</div>
        <div style="color: var(--red); font-size: 10px;">YES IT'S LIVE BUT FORBIDDEN</div>
    </div>
    <div id="feed"></div>
    <script>
        const ws = new WebSocket(location.origin.replace('http','ws'));
        const f = document.getElementById('feed');
        const dot = document.getElementById('ping-dot');
        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            if (d.type === 'ping') {
                dot.style.backgroundColor = '#00ff9d'; // Green for pulse
                setTimeout(() => { dot.style.backgroundColor = '#444'; }, 500);
                return;
            }
            const r = document.createElement('div');
            r.className = 'row ' + d.side;
            r.innerHTML = "<span>" + new Date().toLocaleTimeString([], {hour12:false}) + "</span><span class='source'>[" + d.exch + "]</span><span>" + d.symbol + "</span><span>" + d.side + "</span><span class='val'>$" + d.value.toLocaleString() + "</span>";
            f.insertBefore(r, f.firstChild);
            if (f.children.length > 40) f.removeChild(f.lastChild);
        };
        ws.onclose = () => { dot.style.backgroundColor = 'red'; };
    </script>
</body></html>`;
}
