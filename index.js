const WebSocket = require('ws');
const http = require('http');

// --- 1. THE SACRED COINS (Your 100 USDT Pairs) ---
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

// --- 2. THE SYMBOL NORMALIZER ---
// Converts "BTC-USDT-SWAP" or "XBTUSDT" to "BTCUSDT" for filtering
const normalize = (symbol) => {
    return symbol.replace(/[-_]/g, '')
                 .replace('SWAP', '')
                 .replace('XBT', 'BTC')
                 .toUpperCase();
};

// --- 3. CORE LOGIC ---
const port = process.env.PORT || 10000;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(getHTML());
});
const wss = new WebSocket.Server({ server });

wss.on('connection', (palaceClient) => {
    console.log('>>> New Client Infiltrated the Palace');
    const remoteSockets = [];

    const broadcast = (exch, symbol, side, value) => {
        const cleanSymbol = normalize(symbol);
        if (TARGET_COINS.has(cleanSymbol) && value > 100) {
            const data = JSON.stringify({ exch, symbol: cleanSymbol, side, value: Math.round(value) });
            if (palaceClient.readyState === WebSocket.OPEN) palaceClient.send(data);
        }
    };

    // --- EXCHANGE HANDLERS ---
    
    // BINANCE
    const startBinance = () => {
        const ws = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr');
        ws.on('message', (msg) => {
            const d = JSON.parse(msg).o;
            broadcast('Binance', d.s, d.S === 'BUY' ? 'short' : 'long', d.q * d.p);
        });
        ws.on('close', () => setTimeout(startBinance, 5000));
        remoteSockets.push(ws);
    };

    // BYBIT
    const startBybit = () => {
        const ws = new WebSocket('wss://stream.bybit.com/v5/public/linear');
        ws.on('open', () => ws.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.BTCUSDT", "liquidation.ETHUSDT", "liquidation.SOLUSDT"]})));
        ws.on('message', (msg) => {
            const d = JSON.parse(msg).data;
            if (d) broadcast('Bybit', d.symbol, d.side === 'Buy' ? 'short' : 'long', d.size * d.price);
        });
        ws.on('close', () => setTimeout(startBybit, 5000));
        remoteSockets.push(ws);
    };

    // OKX
    const startOKX = () => {
        const ws = new WebSocket('wss://wspap.okx.com:8443/ws/v5/public');
        ws.on('open', () => ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "ANY"}]})));
        ws.on('message', (msg) => {
            const d = JSON.parse(msg).data;
            if (d) broadcast('OKX', d[0].instId, d[0].side === 'buy' ? 'short' : 'long', d[0].sz * d[0].bkPx);
        });
        ws.on('close', () => setTimeout(startOKX, 5000));
        remoteSockets.push(ws);
    };

    startBinance();
    startBybit();
    startOKX();

    palaceClient.on('close', () => remoteSockets.forEach(s => s.close()));
});

server.listen(port, () => console.log(`Palace Server LIVE on ${port}`));

// --- 4. THE UI (FORBIDDEN THEME) ---
function getHTML() {
    return `
<!DOCTYPE html><html><head><title>FORBIDDEN PALACE</title>
<style>
    :root { --red: #ff3e3e; --green: #00ff9d; --bg: #030303; }
    body { background: var(--bg); color: #fff; font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-transform: uppercase; overflow: hidden; }
    .header { display: flex; justify-content: space-between; border-bottom: 1px solid #222; padding-bottom: 10px; margin-bottom: 20px; }
    #feed { height: 80vh; overflow: hidden; display: flex; flex-direction: column; gap: 4px; }
    .row { display: grid; grid-template-columns: 100px 120px 120px 80px 1fr; background: #080808; padding: 12px; border-left: 2px solid #333; font-size: 13px; }
    .short { border-left-color: var(--red); color: var(--red); }
    .long { border-left-color: var(--green); color: var(--green); }
    .source { color: #666; font-weight: bold; }
    .val { text-align: right; color: #fff; font-weight: bold; }
</style></head>
<body>
    <div class="header">
        <div style="color: gold; font-weight: bold;">🏰 FORBIDDEN PALACE</div>
        <div style="color: var(--red); font-size: 10px;">YES IT'S LIVE BUT FORBIDDEN</div>
    </div>
    <div id="feed"></div>
    <script>
        const ws = new WebSocket(window.location.origin.replace('http', 'ws'));
        const feed = document.getElementById('feed');
        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            const r = document.createElement('div');
            r.className = 'row ' + d.side;
            r.innerHTML = "<span>" + new Date().toLocaleTimeString([], {hour12:false}) + "</span><span class='source'>[" + d.exch + "]</span><span>" + d.symbol + "</span><span>" + d.side + "</span><span class='val'>$" + d.value.toLocaleString() + "</span>";
            feed.insertBefore(r, feed.firstChild);
            if (feed.children.length > 40) feed.removeChild(feed.lastChild);
        };
    </script>
</body></html>`;
}
