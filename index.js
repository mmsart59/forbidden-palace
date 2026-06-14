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
    const cleanSymbol = normalize(symbol);
    if (TARGET_COINS.has(cleanSymbol) && value > 100) {
        stats.total++;
        const payload = JSON.stringify({ exch, symbol: cleanSymbol, side, value: Math.round(value) });
        clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) client.send(payload);
        });
    }
};

// --- ON-DEMAND ENGINE LOGIC ---

const startEngines = () => {
    if (remoteSockets.length > 0) return; // Already running
    console.log('>>> [PALACE] Activating engines (Client connected)');
    
    // Binance
    const bn = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr', { headers: {'User-Agent': 'Mozilla/5.0'} });
    bn.on('open', () => stats.Binance = 'LIVE');
    bn.on('message', (msg) => {
        const d = JSON.parse(msg);
        if (d.e === "forceOrder") broadcast('Binance', d.o.s, d.o.S === 'BUY' ? 'short' : 'long', d.o.q * d.o.p);
    });
    bn.on('close', () => stats.Binance = 'OFF');
    remoteSockets.push(bn);

    // Bybit
    const bb = new WebSocket('wss://stream.bybit.com/v5/public/linear');
    bb.on('open', () => { 
        stats.Bybit = 'LIVE';
        bb.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.BTCUSDT", "liquidation.ETHUSDT", "liquidation.SOLUSDT"]}));
    });
    bb.on('message', (msg) => {
        const d = JSON.parse(msg).data;
        if (d) broadcast('Bybit', d.symbol, d.side === 'Buy' ? 'short' : 'long', d.size * d.price);
    });
    bb.on('close', () => stats.Bybit = 'OFF');
    remoteSockets.push(bb);
};

const stopEngines = () => {
    console.log('>>> [PALACE] Deactivating engines (No clients left)');
    remoteSockets.forEach(s => s.close());
    remoteSockets = [];
    stats.Binance = stats.Bybit = stats.OKX = 'OFF';
};

wss.on('connection', (ws) => {
    clients.add(ws);
    startEngines(); // Wake up Binance/Bybit when app connects
    
    ws.on('close', () => {
        clients.delete(ws);
        if (clients.size === 0) stopEngines(); // Go to sleep when app closes
    });
});

setInterval(() => {
    if (clients.size > 0) {
        console.log(`--- [ACTIVE] Clients: ${clients.size} | Capture Total: ${stats.total} ---`);
    }
}, 60000);

server.listen(port, () => console.log(`Palace Server LIVE on ${port}`));

function getHTML() {
    return `<!DOCTYPE html><html><head><title>FORBIDDEN</title><style>body{background:#000;color:#fff;font-family:monospace;padding:20px;text-transform:uppercase;} .row{display:grid;grid-template-columns:100px 100px 100px 80px 1fr;background:#080808;padding:10px;border-bottom:1px solid #222;} .short{color:#f44;} .long{color:#0f8;}</style></head><body><h2 style="color:gold">🏰 PALACE</h2><div id="f"></div><script>const ws=new WebSocket(location.origin.replace('http','ws')),f=document.getElementById('f');ws.onmessage=(e)=>{const d=JSON.parse(e.data);const r=document.createElement('div');r.className='row '+d.side;r.innerHTML='<span>'+new Date().toLocaleTimeString()+'</span><span>['+d.exch+']</span><span>'+d.symbol+'</span><span>'+d.side+'</span><span style="text-align:right">$'+d.value.toLocaleString()+'</span>';f.insertBefore(r,f.firstChild);if(f.children.length>40)f.removeChild(f.lastChild);};</script></body></html>`;
}
