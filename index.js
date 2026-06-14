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
let remoteSockets = [];

const broadcast = (exch, symbol, side, value) => {
    try {
        const cleanSymbol = normalize(symbol);
        const roundedValue = Math.round(value);
        if (TARGET_COINS.has(cleanSymbol) && roundedValue > 100) {
            stats.total++;
            const payload = JSON.stringify({ exch, symbol: cleanSymbol, side, value: roundedValue });
            clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(payload); });
        }
    } catch (e) {}
};

const startEngines = () => {
    if (remoteSockets.length > 0) return;
    console.log('>>> [PALACE] Engines Warming Up...');

    const headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' };

    // 1. BINANCE
    const bn = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr', { headers });
    bn.on('open', () => { stats.Binance = 'LIVE'; console.log('>>> [BINANCE] Infiltrated'); });
    bn.on('message', (m) => {
        try {
            const d = JSON.parse(m);
            if (d.e === "forceOrder") broadcast('Binance', d.o.s, d.o.S === 'BUY' ? 'short' : 'long', d.o.q * d.o.p);
        } catch(e){}
    });
    bn.on('error', (e) => console.log('Binance Error:', e.message));
    bn.on('close', () => { stats.Binance = 'OFF'; });
    remoteSockets.push(bn);

    // 2. BYBIT (Using .bytick mirror for clouds)
    const bb = new WebSocket('wss://stream.bytick.com/v5/public/linear', { headers });
    bb.on('open', () => { 
        stats.Bybit = 'LIVE'; console.log('>>> [BYBIT] Infiltrated');
        const top = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "PEPEUSDT", "XRPUSDT", "WIFUSDT", "NEARUSDT", "LINKUSDT", "AVAXUSDT"];
        bb.send(JSON.stringify({"op": "subscribe", "args": top.map(p => `liquidation.${p}`)}));
    });
    bb.on('message', (m) => {
        try {
            const d = JSON.parse(m).data;
            if (d) broadcast('Bybit', d.symbol, d.side === 'Buy' ? 'short' : 'long', d.size * d.price);
        } catch(e){}
    });
    bb.on('error', (e) => console.log('Bybit Error:', e.message));
    bb.on('close', () => { stats.Bybit = 'OFF'; });
    remoteSockets.push(bb);

    // 3. OKX (Using standard port)
    const ok = new WebSocket('wss://ws.okx.com:8443/ws/v5/public', { headers });
    ok.on('open', () => { 
        stats.OKX = 'LIVE'; console.log('>>> [OKX] Infiltrated');
        ok.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "ANY"}]}));
    });
    ok.on('message', (m) => {
        try {
            const d = JSON.parse(m);
            if (d.data && d.data[0]) {
                const item = d.data[0];
                broadcast('OKX', item.instId, item.side === 'buy' ? 'short' : 'long', item.sz * item.bkPx);
            }
        } catch(e){}
    });
    ok.on('error', (e) => console.log('OKX Error:', e.message));
    ok.on('close', () => { stats.OKX = 'OFF'; });
    remoteSockets.push(ok);
};

const stopEngines = () => {
    console.log('>>> [PALACE] Engines Cooling Down...');
    remoteSockets.forEach(s => { try { s.close(); } catch(e){} });
    remoteSockets = [];
    stats.Binance = stats.Bybit = stats.OKX = 'OFF';
};

wss.on('connection', (ws) => {
    clients.add(ws);
    startEngines();
    ws.on('close', () => {
        clients.delete(ws);
        if (clients.size === 0) stopEngines();
    });
});

setInterval(() => {
    if (clients.size > 0) {
        clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(JSON.stringify({ type: 'ping' })); });
        console.log(`--- [PALACE] Active Clients:${clients.size} | Capture Total:${stats.total} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX} ---`);
    }
}, 30000);

server.listen(port, () => console.log(`Palace Server LIVE on ${port}`));

function getHTML() {
    return `<!DOCTYPE html><html><head><title>PALACE</title><style>body{background:#000;color:#fff;font-family:monospace;padding:20px;text-transform:uppercase;overflow:hidden;} .row{display:grid;grid-template-columns:100px 120px 120px 80px 1fr;background:#080808;padding:12px;border-left:2px solid #333;font-size:13px;margin-bottom:4px;} .short{border-left-color:#f44;color:#f44;} .long{border-left-color:#0f8;color:#0f8;} .dot{height:8px;width:8px;background:#444;border-radius:50%;display:inline-block;margin-right:10px;}</style></head><body><div style="display:flex;justify-content:space-between"><div><span class="dot" id="d"></span>🏰 FORBIDDEN PALACE</div><div style="color:#f44;font-size:10px">YES IT'S LIVE BUT FORBIDDEN</div></div><div id="f" style="margin-top:20px;height:80vh"></div><script>const ws=new WebSocket(location.origin.replace('http','ws')),f=document.getElementById('f'),d=document.getElementById('d');ws.onmessage=(e)=>{const j=JSON.parse(e.data);if(j.type==='ping'){d.style.background='#0f8';setTimeout(()=>{d.style.background='#444'},500);return;}const r=document.createElement('div');r.className='row '+j.side;r.innerHTML='<span>'+new Date().toLocaleTimeString([],{hour12:false})+'</span><span style="color:#666">['+j.exch+']</span><span>'+j.symbol+'</span><span>'+j.side+'</span><span style="text-align:right;font-weight:bold">$'+j.value.toLocaleString()+'</span>';f.insertBefore(r,f.firstChild);if(f.children.length>40)f.removeChild(f.lastChild);};ws.onclose=()=>{d.style.background='red'};</script></body></html>`;
}
