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
    if (req.url === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
        res.end('PONG');
        return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(getHTML());
});

const wss = new WebSocket.Server({
    server,
    clientTracking: true,
    maxPayload: 1024 * 1024
});

process.on('uncaughtException', (err) => console.error('FATAL EXCEPTION:', err));
process.on('unhandledRejection', (reason) => console.error('UNHANDLED REJECTION:', reason));

let stats = { Binance: 'OFF', Bybit: 'OFF', OKX: 'OFF', total: 0 };
let clients = new Set();
let engineActive = false;
let remoteSockets = new Map();

const broadcast = (payload) => {
    try {
        const data = JSON.stringify(payload);
        let sentCount = 0;
        clients.forEach(c => {
            if (c.readyState === WebSocket.OPEN) {
                try {
                    c.send(data);
                    sentCount++;
                } catch (e) {
                    clients.delete(c);
                }
            }
        });
        return sentCount;
    } catch (e) {
        return 0;
    }
};

const connectExch = (name, url, onOpen, onMsg) => {
    console.log(`>>> [PALACE] Connecting to ${name}...`);
    const ws = new WebSocket(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    ws.on('open', () => {
        console.log(`>>> [PALACE] ${name} Connection Established.`);
        stats[name] = 'LIVE';
        onOpen(ws);
    });
    ws.on('message', (m) => { try { onMsg(ws, JSON.parse(m)); } catch(e){} });
    ws.on('error', (e) => {
        console.error(`[PALACE ERROR] ${name} Socket Error:`, e.message);
        stats[name] = 'ERROR';
    });
    ws.on('close', () => { 
        console.log(`--- [PALACE] ${name} Connection Closed. Reconnecting... ---`);
        if (ws.pingTimer) clearInterval(ws.pingTimer);
        stats[name] = 'OFF';
        if (engineActive) setTimeout(() => connectExch(name, url, onOpen, onMsg), 5000); 
    });
    remoteSockets.set(name, ws);
};

const startEngines = () => {
    if (engineActive) return;
    engineActive = true;
    console.log('>>> [ENGINES] Starting Purified Palace (Liquidations Only)...');

    // 1. BINANCE
    connectExch('Binance', 'wss://fstream.binance.com/market/ws/!forceOrder@arr',
        (ws) => {
            console.log('>>> [BINANCE] LIQUIDATION GATE OPEN');
            ws.pingTimer = setInterval(() => { if(ws.readyState === WebSocket.OPEN) ws.ping(); }, 30000);
        },
        (ws, d) => {
            const proc = (i) => {
                if(i.e === "forceOrder") {
                    stats.total++;
                    broadcast({
                        exch: 'Binance',
                        symbol: normalize(i.o.s),
                        side: i.o.S === 'BUY' ? 'short' : 'long',
                        price: String(i.o.p),
                        quantity: String(i.o.q),
                        value: Number(i.o.q * i.o.p) || 0
                    });
                }
            };
            if(Array.isArray(d)) d.forEach(proc); else proc(d);
        }
    );

    // 2. BYBIT
    connectExch('Bybit', 'wss://stream.bytick.com/v5/public/linear', 
        (ws) => {
            console.log('>>> [BYBIT] LIQUIDATION GATE OPEN');
            const coins = Array.from(TARGET_COINS);
            for (let i = 0; i < coins.length; i += 10) {
                const chunk = coins.slice(i, i + 10).map(s => `allLiquidation.${s}`);
                ws.send(JSON.stringify({"op": "subscribe", "args": chunk}));
            }
            ws.pingTimer = setInterval(() => ws.send(JSON.stringify({"op": "ping"})), 20000);
        }, 
        (ws, d) => {
            if (d.data) {
                const process = (item) => {
                    stats.total++;
                    broadcast({
                        exch: 'Bybit',
                        symbol: normalize(item.s || item.symbol),
                        side: item.S === 'Buy' ? 'short' : 'long',
                        price: String(item.p || item.price),
                        quantity: String(item.v || item.size),
                        value: Number((item.v || item.size) * (item.p || item.price)) || 0
                    });
                };
                if (Array.isArray(d.data)) d.data.forEach(process); else process(d.data);
            }
        }
    );

    // 3. OKX
    connectExch('OKX', 'wss://ws.okx.com:8443/ws/v5/public', 
        (ws) => {
            console.log('>>> [OKX] LIQUIDATION GATE OPEN');
            ws.send(JSON.stringify({"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]}));
            ws.pingTimer = setInterval(() => ws.send("ping"), 20000);
        }, 
        (ws, d) => {
            if (d.data && Array.isArray(d.data)) {
                d.data.forEach(outer => {
                    const process = (inner) => {
                        stats.total++;
                        broadcast({
                            exch: 'OKX',
                            symbol: normalize(inner.instId || outer.instId),
                            side: inner.side === 'buy' ? 'short' : 'long',
                            price: String(inner.bkPx),
                            quantity: String(inner.sz),
                            value: Number(inner.sz * inner.bkPx) || 0
                        });
                    };
                    if (outer.side) process(outer);
                    else if (outer.data && Array.isArray(outer.data)) outer.data.forEach(process);
                    else if (outer.details && Array.isArray(outer.details)) outer.details.forEach(process);
                });
            }
        }
    );
};

wss.on('connection', (ws) => {
    clients.add(ws);
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    startEngines();
    ws.on('close', () => { clients.delete(ws); });
});

setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
    if (clients.size > 0) {
        broadcast({ type: 'ping' });
        console.log(`--- [PALACE HEARTBEAT] Clients:${clients.size} | B:${stats.Binance} BB:${stats.Bybit} OKX:${stats.OKX} ---`);
    }
}, 30000);

server.listen(port, () => console.log(`Forbidden Palace LIVE on ${port}`));

function getHTML() {
    return `<!DOCTYPE html><html><head><title>PALACE</title><style>body{background:#000;color:#fff;font-family:monospace;padding:20px;text-transform:uppercase;overflow:hidden;} .row{display:grid;grid-template-columns:100px 120px 120px 80px 1fr;background:#080808;padding:12px;border-left:2px solid #333;font-size:13px;margin-bottom:4px;} .short{border-left-color:#f44;color:#f44;} .long{border-left-color:#0f8;color:#0f8;} .dot{height:8px;width:8px;background:#444;border-radius:50%;display:inline-block;margin-right:10px;}</style></head><body><div style="display:flex;justify-content:space-between"><div><span class="dot" id="d"></span>🏰 FORBIDDEN PALACE (LIQUIDATIONS)</div></div><div id="f" style="margin-top:20px;height:80vh"></div><script>const ws=new WebSocket(location.origin.replace('http','ws')),f=document.getElementById('f'),d=document.getElementById('d');ws.onmessage=(e)=>{const j=JSON.parse(e.data);if(j.type==='ping'){d.style.background='#0f8';setTimeout(()=>{d.style.background='#444'},500);return;}const r=document.createElement('div');r.className='row '+j.side;r.innerHTML='<span>'+new Date().toLocaleTimeString([],{hour12:false})+'</span><span style="color:#666">['+j.exch.toUpperCase()+']</span><span>'+j.symbol+'</span><span>'+j.side+'</span><span style="text-align:right;font-weight:bold">$'+j.value.toLocaleString()+'</span>';f.insertBefore(r,f.firstChild);if(f.children.length>40)f.removeChild(f.lastChild);};ws.onclose=()=>{d.style.background='red'};</script></body></html>`;
}
