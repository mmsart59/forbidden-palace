// --- FIXED 2026 STEALTH URLS ---
const BINANCE_LIQ_WS = 'wss://fstream.binance.me/ws/!forceOrder@arr';
const BINANCE_TICKER_WS = 'wss://fstream.binance.me/ws/!ticker@arr';

// 1. BINANCE LIQUIDATIONS
connectExch('BinanceLiq', BINANCE_LIQ_WS, () => {}, (ws, d) => {
    const process = (i) => { if(i.e === "forceOrder") {
        const sym = normalize(i.o.s);
        if (TARGET_COINS.has(sym)) {
            const p = parseFloat(i.o.ap || i.o.p);
            const q = parseFloat(i.o.q);
            trackAndBroadcast({ exch: 'Binance', symbol: sym, side: i.o.S.toLowerCase(), value: p * q, price: p, quantity: q });
        }
    }};
    if(Array.isArray(d)) d.forEach(process); else process(d);
});

// 2. BINANCE TICKERS
connectExch('BinanceTickers', BINANCE_TICKER_WS, () => {}, (ws, d) => {
    if (!Array.isArray(d)) return;
    d.forEach(t => {
        const sym = normalize(t.s);
        if (TARGET_COINS.has(sym)) {
            tickerMap[sym] = { p: parseFloat(t.c), v: parseFloat(t.q), c: parseFloat(t.P) };
        }
    });
});

// --- MANDATORY RENDER BINDING ---
server.listen(port, '0.0.0.0', () => console.log(`2026 ENGINE LIVE ON ${port}`));
