const WebSocket = require('ws');
const http = require('http');

// --- 1. THE SACRED COINS ---
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

// Shared state for logs
let stats = { Binance: 'WAIT', Bybit: 'WAIT', OKX: 'WAIT', total: 0 };

wss.on('connection', (palaceClient) => {
    console.log('>>> [PALACE] New client connected');
    const remoteSockets = [];

    const broadcast = (exch, symbol, side, value) => {
        const cleanSymbol = normalize(symbol);
        if (TARGET_COINS.has(cleanSymbol) && value > 10) { // Lowered to $10 for testing
            stats.total++;
            const payload = { exch, symbol: cleanSymbol, side, value: Math.round(value) };
            console.log(`[${exch}] LIQ: ${cleanSymbol} | $${payload.value}`);
            if (palaceClient.readyState === WebSocket.OPEN) palaceClient.send(JSON.stringify(payload));
        }
    };

    const startBinance = () => {
        const ws = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr', { headers: {'User-Agent': 'Mozilla/5.0'} });
        ws.on('open', () => { console.log('>>> [BINANCE] Connected'); stats.Binance = 'LIVE'; });
        ws.on('message', (msg) => {
            const data = JSON.parse(msg);
            if (data.e === "forceOrder") broadcast('Binance', data.o.s, data.o.S === 'BUY' ? 'short' : 'long', data.o.q * data.o.p);
        });
        ws.on('error', (e) => { console.log('!!! [BINANCE] Error:', e.message); stats.Binance = 'ERROR'; });
        ws.on('close', () => { stats.Binance = 'CLOSED'; setTimeout(startBinance, 5000); });
        remoteSockets.push(ws);
    };

    const startBybit = () => {
        const ws = new WebSocket('wss://stream.bybit.com/v5/public/linear');
        ws.on('open', () => { 
            console.log('>>> [BYBIT] Connected'); stats.Bybit = 'LIVE';
            ws.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.BTCUSDT", "liquidation.ETHUSDT", "liquidation.SOLUSDT"]}));
        });
        ws.on('message', (msg) => {
            const d = JSON.parse(msg).data;
            if (d) broadcast('Bybit', d.symbol, d.side === 'Buy' ? 'short' : 'long', d.size * d.price);
        });
        ws.on('error', (e) => { console.log('!!! [BYBIT] Error:', e.message); stats.Bybit = 'ERROR'; });
        ws.on('close', () => { stats.Bybit = 'CLOSED'; setTimeout(startBybit, 5000); });
        remoteSockets.push(ws);
    };

    const startOKX = () => {
        const ws = new WebSocket('wss://wspap.okx.com:8443/ws/v5/public');
        ws.on('open', () => { 
            console.log('>>> [OKX] Connected'); stats.OKX = 'LIVE';
            ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "ANY"}]}));
        });
        ws.on('message', (msg) => {
            const d = JSON.parse(msg).data;
            if (d) broadcast('OKX', d[0].instId, d[0].side === 'buy' ? 'short' : 'long', d[0].sz * d[0].bkPx);
        });
        ws.on('error', (e) => { console.log('!!! [OKX] Error:', e.message); stats.OKX = 'ERROR'; });
        ws.on('close', () => { stats.OKX = 'CLOSED'; setTimeout(startOKX, 5000); });
        remoteSockets.push(ws);
    };

    startBinance();
    startBybit();
    startOKX();

    palaceClient.on('close', () => remoteSockets.forEach(s => s.close()));
});

// HEARTBEAT LOGS (Every 30 seconds in Render Console)
setInterval(() => {
    console.log(`--- [HEARTBEAT] Status: B:${stats.Binance} | BB:${stats.Bybit} | OKX:${stats.OKX} | Total Liquidations: ${stats.total} ---`);
}, 30000);

server.listen(port, () => console.log(`Palace Server LIVE on ${port}`));

function getHTML() {
    return `<!DOCTYPE html><html><head><title>FORBIDDEN</title><style>body{background:#000;color:#fff;font-family:monospace;padding:20px;text-transform:uppercase;} .row{display:grid;grid-template-columns:100px 100px 100px 80px 1fr;background:#080808;padding:10px;border-bottom:1px solid #222;} .short{color:#f44;} .long{color:#0f8;}</style></head><body><h2 style="color:gold">🏰 PALACE</h2><div id="f"></div><script>const ws=new WebSocket(location.origin.replace('http','ws')),f=document.getElementById('f');ws.onmessage=(e)=>{const d=JSON.parse(e.data);const r=document.createElement('div');r.className='row '+d.side;r.innerHTML='<span>'+new Date().toLocaleTimeString()+'</span><span>['+d.exch+']</span><span>'+d.symbol+'</span><span>'+d.side+'</span><span style="text-align:right">$'+d.value.toLocaleString()+'</span>';f.insertBefore(r,f.firstChild);if(f.children.length>40)f.removeChild(f.lastChild);};</script></body></html>`;
}
