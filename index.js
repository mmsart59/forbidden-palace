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

const broadcast = (payload) => {
    const data = JSON.stringify(payload);
    clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
};

// 1. BINANCE
const startBinance = () => {
    const ws = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr', { headers: {'User-Agent': 'Mozilla/5.0'} });
    ws.on('open', () => { stats.Binance = 'LIVE'; console.log('>>> [BINANCE] GATE OPEN'); });
    ws.on('message', (msg) => {
        try {
            const dataStr = msg.toString();
            if (dataStr === "" || dataStr === "pong") return;
            const d = JSON.parse(dataStr);
            const processItem = (i) => {
                if (i.e === "forceOrder") {
                    const val = Math.round(parseFloat(i.o.q) * parseFloat(i.o.p));
                    const sym = normalize(i.o.s);
                    if (TARGET_COINS.has(sym) && val > 100) {
                        stats.total++;
                        broadcast({ exch: 'Binance', symbol: sym, side: i.o.S.toLowerCase(), value: val });
                    }
                }
            };
            if (Array.isArray(d)) d.forEach(processItem); else processItem(d);
        } catch (e) { }
    });
    ws.on('close', () => { stats.Binance = 'OFF'; setTimeout(startBinance, 5000); });
};

// 2. BYBIT
const startBybit = () => {
    const ws = new WebSocket('wss://stream.bytick.com/v5/public/linear');
    ws.on('open', () => { 
        stats.Bybit = 'LIVE'; console.log('>>> [BYBIT] GATE OPEN');
        ws.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.linear"]}));
        ws.pingInterval = setInterval(() => ws.send(JSON.stringify({"op":"ping"})), 20000);
    });
    ws.on('message', (msg) => {
        try {
            const dataStr = msg.toString();
            const d = JSON.parse(dataStr).data;
            if (d) {
                const val = Math.round(parseFloat(d.size) * parseFloat(d.price));
                const sym = normalize(d.symbol);
                if (TARGET_COINS.has(sym) && val > 100) {
                    stats.total++;
                    broadcast({ exch: 'Bybit', symbol: sym, side: d.side.toLowerCase(), value: val });
                }
            }
        } catch (e) { }
    });
    ws.on('close', () => { clearInterval(ws.pingInterval); stats.Bybit = 'OFF'; setTimeout(startBybit, 5000); });
};

// 3. OKX
const startOKX = () => {
    const ws = new WebSocket('wss://ws.okx.com:8443/ws/v5/public');
    ws.on('open', () => { 
        stats.OKX = 'LIVE'; console.log('>>> [OKX] GATE OPEN');
        ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]}));
        ws.pingInterval = setInterval(() => ws.send("ping"), 20000);
    });
    ws.on('message', (msg) => {
        const text = msg.toString();
        if (text === "pong") return; // CRASH FIX
        try {
            const d = JSON.parse(text).data;
            if (d && d[0]) {
                const val = Math.round(parseFloat(d[0].sz) * parseFloat(d[0].bkPx));
                const sym = normalize(d[0].instId);
                if (TARGET_COINS.has(sym) && val > 100) {
                    stats.total++;
                    broadcast({ exch: 'OKX', symbol: sym, side: d[0].side.toLowerCase(), value: val });
                }
            }
        } catch (e) { }
    });
    ws.on('close', () => { clearInterval(ws.pingInterval); stats.OKX = 'OFF'; setTimeout(startOKX, 5000); });
};

startBinance();
startBybit();
startOKX();

wss.on('connection', (ws) => {
    clients.add(ws);
    ws.on('close', () => clients.delete(ws));
});

setInterval(() => {
    if (clients.size > 0) broadcast({ type: 'ping' });
    console.log(`--- [HEARTBEAT] Captures:${stats.total} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX} ---`);
}, 30000);

server.listen(port, () => console.log(`Palace LIVE on ${port}`));

function getHTML() {
    return `<!DOCTYPE html><html><head><title>FORBIDDEN PALACE</title><style>
    :root { --red: #ff3e3e; --green: #00ff9d; --bg: #030303; }
    body { background: var(--bg); color: #fff; font-family: monospace; margin: 0; padding: 20px; text-transform: uppercase; overflow: hidden; }
    .header { display: flex; justify-content: space-between; border-bottom: 1px solid #222; padding-bottom: 10px; margin-bottom: 20px; }
    #f { height: 80vh; overflow-y: hidden; display: flex; flex-direction: column; gap: 4px; }
    .row { display: grid; grid-template-columns: 80px 140px 120px 80px 1fr; background: #080808; padding: 12px; border-left: 2px solid #333; font-size: 13px; margin-bottom: 4px; }
    .buy, .short { border-left-color: var(--green); color: var(--green); }
    .sell, .long { border-left-color: var(--red); color: var(--red); }
    </style></head><body>
    <div class="header"><div style="color: gold">🏰 FORBIDDEN PALACE</div><div style="color: var(--red); font-size: 10px;">YES IT'S LIVE BUT FORBIDDEN</div></div>
    <div id="f"></div>
    <script>
        const ws=new WebSocket(location.origin.replace('http','ws')), f=document.getElementById('f');
        ws.onmessage=(e)=>{
            const d=JSON.parse(e.data); if(d.type==='ping') return;
            const r=document.createElement('div'); r.className='row '+d.side;
            r.innerHTML='<span>'+new Date().toLocaleTimeString([],{hour12:false})+'</span><span style="color:#888">['+d.exch.toUpperCase()+']</span><span>'+d.symbol+'</span><span>'+d.side.toUpperCase()+'</span><span style="text-align:right;font-weight:bold">$'+d.value.toLocaleString()+'</span>';
            f.insertBefore(r,f.firstChild); if(f.children.length>40) f.removeChild(f.lastChild);
        };
    </script></body></html>`;
}
