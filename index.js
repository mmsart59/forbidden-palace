function getHTML() {
    return `<!DOCTYPE html>
<html>
<head>
    <title>PALACE LIVE</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            background: #020202;
            color: #e0e0e0;
            font-family: 'Courier New', monospace;
            padding: 20px;
            text-transform: uppercase;
            margin: 0;
            height: 100vh;
            box-sizing: border-box;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding-bottom: 15px;
            border-bottom: 1px solid #222;
        }
        .title {
            font-weight: bold;
            letter-spacing: 1px;
            display: flex;
            align-items: center;
        }
        .dot {
            height: 10px;
            width: 10px;
            background: #444;
            border-radius: 50%;
            display: inline-block;
            margin-right: 10px;
            transition: background 0.3s ease;
        }
        #f {
            margin-top: 15px;
            height: calc(100vh - 70px);
            overflow-y: auto;
            padding-right: 5px;
        }
        /* شخصی‌سازی اسکرول‌بار که زشت نباشد */
        #f::-webkit-scrollbar { width: 4px; }
        #f::-webkit-scrollbar-thumb { background: #222; border-radius: 2px; }

        .row {
            display: grid;
            grid-template-columns: 80px 100px 100px 70px 1fr;
            background: rgba(15, 15, 15, 0.6);
            backdrop-filter: blur(5px);
            padding: 12px;
            border-left: 3px solid #333;
            font-size: 13px;
            margin-bottom: 6px;
            border-radius: 0 6px 6px 0;
            animation: fadeIn 0.2s ease-out;
        }
        .short { border-left-color: #ff4444; color: #ff4444; background: rgba(255, 68, 68, 0.03); }
        .long { border-left-color: #00ff88; color: #00ff88; background: rgba(0, 255, 136, 0.03); }
        
        .time { color: #666; }
        .exch { color: #888; font-size: 11px; font-weight: bold; }
        .sym { color: #fff; font-weight: bold; }
        .val { text-align: right; font-weight: bold; color: #fff; }
        .long .val { color: #00ff88; }
        .short .val { color: #ff4444; }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(-4px); }
            to { opacity: 1; transform: translateY(0); }
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="title"><span class="dot" id="d"></span>🏰 FORBIDDEN PALACE</div>
        <div style="color: #ff4444; font-size: 11px; font-weight: bold; letter-spacing: 1px;">LIVE STREAMING</div>
    </div>
    
    <div id="f"></div>

    <script>
        const ws = new WebSocket(location.origin.replace('http', 'ws'));
        const f = document.getElementById('f');
        const d = document.getElementById('d');

        ws.onmessage = (e) => {
            const j = JSON.parse(e.data);
            
            // مهار کردن چشمک هارت‌بیت
            if (j.type === 'ping') {
                d.style.background = '#00ff88';
                setTimeout(() => { d.style.background = '#444'; }, 400);
                return;
            }

            // ساخت ردیف دیتای جدید به صورت استاندارد
            const r = document.createElement('div');
            r.className = 'row ' + j.side;
            
            // چیدمان ساختار داخلی ستون‌ها بر اساس گرید سیستم
            r.innerHTML = \`
                <span class="time">\${new Date().toLocaleTimeString([], {hour12: false})}</span>
                <span class="exch">[\${j.exch.toUpperCase()}]</span>
                <span class="sym">\${j.symbol}</span>
                <span>\${j.side}</span>
                <span class="val">$\${j.value.toLocaleString()}</span>
            \`;

            // هل دادن به اول لیست
            f.insertBefore(r, f.firstChild);

            // جلوگیری از سنگین شدن صفحه (حداکثر ۵۰ ردیف اخیر)
            if (f.children.length > 50) {
                f.removeChild(f.lastChild);
            }
        };

        ws.onclose = () => {
            d.style.background = '#ff4444';
        };
    </script>
</body>
</html>`;
}
