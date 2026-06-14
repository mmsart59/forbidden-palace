const WebSocket = require('ws');
const http = require('http');

const port = process.env.PORT || 3000;

// 1. Create the Web Server
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(getHTML()); // Serves the UI dashboard
});

const wss = new WebSocket.Server({ server });

// 2. The Palace Aggregator
wss.on('connection', (clientSocket) => {
    console.log('New connection to Palace');
    const connections = [];

    const broadcast = (data) => {
        if (clientSocket.readyState === WebSocket.OPEN) {
            clientSocket.send(JSON.stringify(data));
        }
    };

    // --- BINANCE DSTREAM ---
    const bn = new WebSocket('wss://dstream.binance.me/ws/!forceOrder@arr', {
        headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    bn.on('message', (msg) => {
        const d = JSON.parse(msg);
        if (d.e === "forceOrder") {
            broadcast({ source: 'Binance', symbol: d.o.s, side: d.o.S === 'BUY' ? 'short' : 'long', value: d.o.q * d.o.p });
        }
    });
    connections.push(bn);

    // --- BYBIT ---
    const bb = new WebSocket('wss://stream.bybit.com/v5/public/linear');
    bb.on('open', () => bb.send(JSON.stringify({"op": "subscribe", "args": ["liquidation.BTCUSDT"]})));
    bb.on('message', (msg) => {
        const d = JSON.parse(msg).data;
        if (d) broadcast({ source: 'Bybit', symbol: d.symbol, side: d.side === 'Buy' ? 'short' : 'long', value: d.size * d.price });
    });
    connections.push(bb);

    // --- HYPERLIQUID ---
    const hl = new WebSocket('wss://api.hyperliquid.xyz/ws');
    hl.on('open', () => hl.send(JSON.stringify({"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}})));
    hl.on('message', (msg) => {
        const d = JSON.parse(msg);
        if (d.data && d.data[0].liquidation) {
            broadcast({ source: 'Hyperliquid', symbol: d.data[0].coin, side: d.data[0].side === 'B' ? 'short' : 'long', value: d.data[0].sz * d.data[0].px });
        }
    });
    connections.push(hl);

    clientSocket.on('close', () => {
        connections.forEach(c => c.close());
    });
});

server.listen(port, () => console.log(`Palace LIVE on ${port}`));

function getHTML() {
    return `
    <!DOCTYPE html><html><head><title>FORBIDDEN PALACE</title>
    <style>
        body { background: #000; color: #fff; font-family: monospace; padding: 20px; text-transform: uppercase; }
        .row { display: grid; grid-template-columns: 100px 120px 100px 1fr; border-bottom: 1px solid #222; padding: 10px; }
        .short { color: #ff3e3e; } .long { color: #00ff9d; }
    </style></head><body>
    <h2 style="color: gold">🏰 FORBIDDEN PALACE</h2>
    <div id="status" style="color: red">Connecting to local stream...</div>
    <div id="feed"></div>
    <script>
        const ws = new WebSocket(window.location.origin.replace('http', 'ws'));
        const feed = document.getElementById('feed');
        ws.onopen = () => document.getElementById('status').innerText = "YES IT'S LIVE BUT FORBIDDEN";
        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            const r = document.createElement('div');
            r.className = 'row ' + d.side;
            r.innerHTML = "<span>" + new Date().toLocaleTimeString() + "</span><span>[" + d.source + "]</span><span>" + d.symbol + "</span><span style='text-align:right'>$" + Math.round(d.value).toLocaleString() + "</span>";
            feed.insertBefore(r, feed.firstChild);
        };
    </script></body></html>`;
}