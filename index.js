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

const broadcast = (exch, symbol, side, value) => {
    const cleanSymbol = normalize(symbol);
    if (TARGET_COINS.has(cleanSymbol) && value > 100) {
        stats.total++;
        const payload = JSON.stringify({ exch, symbol: cleanSymbol, side, value: Math.round(value) });
        clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(payload); });
    }
};

const connectExch = (name, url, onOpen, onMsg) => {
    const ws = new WebSocket(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    ws.debugCount = 0;
    ws.on('open', () => { stats[name] = 'LIVE'; console.log(`>>> [${name}] CONNECTED`); onOpen(ws); });
    ws.on('message', (m) => { 
        try { 
            const d = JSON.parse(m);
            if (ws.debugCount < 3) { console.log(`[${name}] RAW SAMPLE:`, JSON.stringify(d).slice(0, 100)); ws.debugCount++; }
            onMsg(ws, d); 
        } catch(e){} 
    });
    ws.on('error', (e) => console.log(`!!! [${name}] Error:`, e.message));
    ws.on('close', () => { 
        stats[name] = 'OFF'; 
        if (engineActive) setTimeout(() => connectExch(name, url, onOpen, onMsg), 5000); 
    });
    remoteSockets.set(name, ws);
};

const startEngines = () => {
    if (engineActive) return;
    engineActive = true;
    console.log('>>> [PALACE] STARTING ALL SYSTEMS');

    // 1. BINANCE (Handles Array or Single Object)
    connectExch('Binance', 'wss://fstream.binance.com/ws/!forceOrder@arr', () => {}, (ws, d) => {
        const process = (item) => { if (item.e === "forceOrder") broadcast('Binance', item.o.s, item.o.S === 'BUY' ? 'short' : 'long', item.o.q * item.o.p); };
        if (Array.isArray(d)) d.forEach(process); else process(d);
    });

    // 2. BYBIT
    connectExch('Bybit', 'wss://stream.bytick.com/v5/public/linear', 
        (ws) => {
            const top = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "PEPEUSDT", "XRPUSDT"];
            ws.send(JSON.stringify({"op": "subscribe", "args": top.map(p => `liquidation.${p}`)}));
            ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000);
        }, 
        (ws, d) => { if (d.data) broadcast('Bybit', d.data.symbol, d.data.side === 'Buy' ? 'short' : 'long', d.data.size * d.data.price); }
    );

    // 3. OKX
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public', 
        (ws) => {
            ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "ANY"}]}));
            ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
        }, 
        (ws, d) => { if (d.data && d.data[0]) broadcast('OKX', d.data[0].instId, d.data[0].side === 'buy' ? 'short' : 'long', d.data[0].sz * d.data[0].bkPx); }
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
        clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(JSON.stringify({ type: 'ping' })); });
        console.log(`--- [HEARTBEAT] Clients:${clients.size} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX} | Captured:${stats.total} ---`);
    }
}, 30000);

server.listen(port, () => console.log(`Palace LIVE on ${port}`));

function getHTML() {
    return `<!DOCTYPE html><html><head><title>FORBIDDEN</title><style>body{background:#000;color:#fff;font-family:monospace;padding:20px;text-transform:uppercase;overflow:hidden;} .row{display:grid;grid-template-columns:100px 120px 120px 80px 1fr;background:#080808;padding:12px;border-left:2px solid #333;font-size:13px;margin-bottom:4px;} .short{border-left-color:#f44;color:#f44;} .long{border-left-color:#0f8;color:#0f8;} .dot{height:8px;width:8px;background:#444;border-radius:50%;display:inline-block;margin-right:10px;}</style></head><body><div style="display:flex;justify-content:space-between"><div><span class="dot" id="d"></span>🏰 FORBIDDEN PALACE</div><div style="color:#f44;font-size:10px">YES IT'S LIVE BUT FORBIDDEN</div></div><div id="f" style="margin-top:20px;height:80vh"></div><script>const ws=new WebSocket(location.origin.replace('http','ws')),f=document.getElementById('f'),d=document.getElementById('d');ws.onmessage=(e)=>{const j=JSON.parse(e.data);if(j.type==='ping'){d.style.background='#0f8';setTimeout(()=>{d.style.background='#444'},500);return;}const r=document.createElement('div');r.className='row '+j.side;r.innerHTML='<span>'+new Date().toLocaleTimeString([],{hour12:false})+'</span><span style="color:#666">['+j.exch+']</span><span>'+j.symbol+'</span><span>'+j.side+'</span><span style="text-align:right;font-weight:bold">$'+j.value.toLocaleString()+'</span>';f.insertBefore(r,f.firstChild);if(f.children.length>40)f.removeChild(f.lastChild);};ws.onclose=()=>{d.style.background='red'};</script></body></html>`;
}
