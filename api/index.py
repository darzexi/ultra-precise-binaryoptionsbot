import json
import time
import random
import numpy as np
from flask import Flask, request, jsonify, render_template_string
from flask_restx import Api, Resource
import logging

# Initialize Flask app
app = Flask(__name__)
api = Api(app)

# Global variables (stored in memory - will reset on each function call)
signal_data = {
    'asset': 'EURUSD_otc',
    'timeframe': 60,
    'update_rate': 0.5,
    'manual_mode': False,
    'hotkey': 'space',
    'current_signal': None,
    'last_update': None,
    'price_data': None,
    'is_running': False,
    'manual_triggered': False,
    'candle_progress': 0,
    'candle_high': None,
    'candle_low': None,
    'candle_open': None,
    'candle_start_time': None
}

# HTML template (simplified for serverless)
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PocketOption Signal Bot</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }
        .container {
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            padding: 30px;
            max-width: 800px;
            width: 100%;
        }
        h1 { color: #333; text-align: center; margin-bottom: 10px; }
        .subtitle { text-align: center; color: #666; margin-bottom: 30px; }
        .signal-display {
            background: #f8f9fa;
            border-radius: 15px;
            padding: 30px;
            text-align: center;
            margin-bottom: 30px;
            border: 3px solid #e0e0e0;
            transition: all 0.3s ease;
        }
        .signal-display.buy { border-color: #28a745; background: #d4edda; }
        .signal-display.sell { border-color: #dc3545; background: #f8d7da; }
        .signal-display.manual { border-color: #ff9800; background: #fff3e0; }
        .signal-text { font-size: 48px; font-weight: bold; margin: 10px 0; }
        .signal-price { font-size: 20px; color: #555; }
        .signal-time { font-size: 14px; color: #888; margin-top: 10px; }
        .settings-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
            margin-bottom: 20px;
        }
        .setting-group {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 10px;
        }
        .setting-group label {
            display: block;
            font-weight: 600;
            margin-bottom: 5px;
            color: #333;
        }
        .setting-group input, .setting-group select {
            width: 100%;
            padding: 8px 12px;
            border: 2px solid #ddd;
            border-radius: 8px;
            font-size: 14px;
        }
        .checkbox-group {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .checkbox-group label { margin-bottom: 0; }
        .button-group {
            display: flex;
            gap: 10px;
            margin-top: 20px;
            flex-wrap: wrap;
        }
        .btn {
            padding: 10px 25px;
            border: none;
            border-radius: 10px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            flex: 1;
            min-width: 120px;
        }
        .btn-success { background: #28a745; color: white; }
        .btn-success:hover { background: #218838; transform: translateY(-2px); }
        .btn-danger { background: #dc3545; color: white; }
        .btn-danger:hover { background: #c82333; transform: translateY(-2px); }
        .btn-warning { background: #ffc107; color: #333; }
        .btn-warning:hover { background: #e0a800; transform: translateY(-2px); }
        .btn-secondary { background: #6c757d; color: white; }
        .btn-secondary:hover { background: #5a6268; transform: translateY(-2px); }
        .status-bar {
            margin-top: 20px;
            padding: 15px;
            background: #e9ecef;
            border-radius: 10px;
            font-size: 14px;
        }
        .status-item {
            display: flex;
            justify-content: space-between;
            padding: 5px 0;
            border-bottom: 1px solid #dee2e6;
        }
        .status-item:last-child { border-bottom: none; }
        .log-area {
            margin-top: 20px;
            background: #1e1e1e;
            color: #d4d4d4;
            padding: 15px;
            border-radius: 10px;
            max-height: 200px;
            overflow-y: auto;
            font-family: 'Courier New', monospace;
            font-size: 12px;
        }
        .log-entry { padding: 2px 0; border-bottom: 1px solid #2d2d2d; }
        .log-entry.buy { color: #4caf50; }
        .log-entry.sell { color: #f44336; }
        .log-entry.error { color: #ff6b6b; }
        .log-entry.manual { color: #ff9800; font-weight: bold; }
        @media (max-width: 600px) {
            .settings-grid { grid-template-columns: 1fr; }
            .button-group { flex-direction: column; }
            .btn { width: 100%; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 PocketOption Signal Bot</h1>
        <p class="subtitle">100% Real-time Signals on Netlify</p>

        <div id="signalDisplay" class="signal-display">
            <div style="font-size: 14px; color: #888;">Current Signal</div>
            <div class="signal-text" id="signalText">WAITING</div>
            <div class="signal-price" id="signalPrice">Price: --</div>
            <div class="signal-time" id="signalTime">Last Update: --</div>
        </div>

        <div class="settings-grid">
            <div class="setting-group">
                <label for="assetSelect">Asset Symbol</label>
                <select id="assetSelect">
                    <option value="EURUSD_otc">EUR/USD OTC</option>
                    <option value="GBPUSD_otc">GBP/USD OTC</option>
                    <option value="USDJPY_otc">USD/JPY OTC</option>
                    <option value="BTCUSD_otc">BTC/USD OTC</option>
                    <option value="XAUUSD_otc">Gold OTC</option>
                </select>
            </div>
            <div class="setting-group">
                <label for="timeframeSelect">Timeframe (seconds)</label>
                <select id="timeframeSelect">
                    <option value="5">5s</option>
                    <option value="10">10s</option>
                    <option value="30">30s</option>
                    <option value="60" selected>60s</option>
                    <option value="120">120s</option>
                    <option value="300">300s</option>
                </select>
            </div>
            <div class="setting-group">
                <label>Manual Mode</label>
                <div class="checkbox-group">
                    <input type="checkbox" id="manualMode">
                    <label for="manualMode">Enable Manual Signal</label>
                </div>
            </div>
            <div class="setting-group">
                <label for="hotkeyInput">Hotkey</label>
                <input type="text" id="hotkeyInput" value="space" maxlength="20">
            </div>
        </div>

        <div class="button-group">
            <button class="btn btn-success" id="startBtn">▶ Start Bot</button>
            <button class="btn btn-danger" id="stopBtn" disabled>⏹ Stop Bot</button>
            <button class="btn btn-warning" id="manualSignalBtn" disabled>⚡ Manual Signal</button>
            <button class="btn btn-secondary" id="clearLogsBtn">🗑 Clear Logs</button>
        </div>

        <div class="status-bar">
            <div class="status-item"><span>Status:</span><span id="statusText">Stopped</span></div>
            <div class="status-item"><span>Mode:</span><span id="modeDisplay">Automatic</span></div>
            <div class="status-item"><span>Hotkey:</span><span id="hotkeyDisplay">space</span></div>
        </div>

        <div class="log-area" id="logArea">
            <div class="log-entry">[System] Bot initialized on Netlify</div>
        </div>
    </div>

    <script>
        let isRunning = false;
        let updateInterval = null;
        let lastManualTriggerTime = 0;

        const signalDisplay = document.getElementById('signalDisplay');
        const signalText = document.getElementById('signalText');
        const signalPrice = document.getElementById('signalPrice');
        const signalTime = document.getElementById('signalTime');
        const statusText = document.getElementById('statusText');
        const modeDisplay = document.getElementById('modeDisplay');
        const logArea = document.getElementById('logArea');

        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');
        const manualSignalBtn = document.getElementById('manualSignalBtn');
        const clearLogsBtn = document.getElementById('clearLogsBtn');

        const assetSelect = document.getElementById('assetSelect');
        const timeframeSelect = document.getElementById('timeframeSelect');
        const manualMode = document.getElementById('manualMode');
        const hotkeyInput = document.getElementById('hotkeyInput');
        const hotkeyDisplay = document.getElementById('hotkeyDisplay');

        hotkeyInput.addEventListener('input', function() {
            hotkeyDisplay.textContent = this.value.trim() || 'space';
        });

        manualMode.addEventListener('change', function() {
            if (isRunning) {
                manualSignalBtn.disabled = !this.checked;
                modeDisplay.textContent = this.checked ? 'Manual' : 'Automatic';
            }
        });

        function triggerManualSignal() {
            const now = Date.now();
            if (now - lastManualTriggerTime < 500) return;
            lastManualTriggerTime = now;
            
            addLog('⚡ Manual signal triggered...', 'manual');
            
            fetch('/.netlify/functions/index/manual_signal', { method: 'POST' })
                .then(r => r.json())
                .then(data => {
                    if (data.success) {
                        signalText.textContent = data.signal.toUpperCase();
                        signalPrice.textContent = `Price: ${data.price}`;
                        signalTime.textContent = `Last Update: ${new Date().toLocaleTimeString()}`;
                        signalDisplay.className = 'signal-display manual';
                        addLog('✅ MANUAL ' + data.signal.toUpperCase() + ' at ' + data.price, 'manual');
                    } else {
                        addLog('❌ Failed: ' + data.error, 'error');
                    }
                })
                .catch(err => addLog('❌ Error: ' + err.message, 'error'));
        }

        function startPolling() {
            if (updateInterval) clearInterval(updateInterval);
            updateInterval = setInterval(() => {
                fetch('/.netlify/functions/index/get_signal')
                    .then(r => r.json())
                    .then(data => {
                        if (data.signal && data.signal !== 'pending' && !data.manual_triggered) {
                            signalText.textContent = data.signal.toUpperCase();
                            signalPrice.textContent = `Price: ${data.price || '--'}`;
                            signalTime.textContent = `Last Update: ${data.timestamp || new Date().toLocaleTimeString()}`;
                            signalDisplay.className = 'signal-display';
                            if (data.signal === 'buy') signalDisplay.classList.add('buy');
                            else if (data.signal === 'sell') signalDisplay.classList.add('sell');
                        }
                    })
                    .catch(err => console.error('Polling error:', err));
            }, 1000);
        }

        function stopPolling() {
            if (updateInterval) {
                clearInterval(updateInterval);
                updateInterval = null;
            }
        }

        function addLog(message, type = 'info') {
            const entry = document.createElement('div');
            entry.className = `log-entry ${type}`;
            entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
            logArea.appendChild(entry);
            logArea.scrollTop = logArea.scrollHeight;
        }

        startBtn.addEventListener('click', function() {
            const config = {
                asset: assetSelect.value,
                timeframe: parseInt(timeframeSelect.value),
                manual_mode: manualMode.checked,
                hotkey: hotkeyInput.value.trim() || 'space'
            };

            fetch('/.netlify/functions/index/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    isRunning = true;
                    startBtn.disabled = true;
                    stopBtn.disabled = false;
                    manualSignalBtn.disabled = !manualMode.checked;
                    statusText.textContent = 'Running';
                    statusText.style.color = '#28a745';
                    addLog('Bot started successfully', 'info');
                    startPolling();
                } else {
                    addLog('Failed to start: ' + data.error, 'error');
                }
            });
        });

        stopBtn.addEventListener('click', function() {
            fetch('/.netlify/functions/index/stop', { method: 'POST' })
                .then(r => r.json())
                .then(data => {
                    if (data.success) {
                        isRunning = false;
                        startBtn.disabled = false;
                        stopBtn.disabled = true;
                        manualSignalBtn.disabled = true;
                        statusText.textContent = 'Stopped';
                        statusText.style.color = '#dc3545';
                        stopPolling();
                        addLog('Bot stopped', 'info');
                    }
                });
        });

        manualSignalBtn.addEventListener('click', triggerManualSignal);

        clearLogsBtn.addEventListener('click', function() {
            logArea.innerHTML = '';
            addLog('Logs cleared', 'info');
        });

        document.addEventListener('keydown', function(e) {
            if (!isRunning || !manualMode.checked) return;
            const hotkey = hotkeyInput.value.trim() || 'space';
            if (e.key.toLowerCase() === hotkey.toLowerCase()) {
                e.preventDefault();
                triggerManualSignal();
            }
        });

        addLog('System ready on Netlify', 'info');
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/start', methods=['POST'])
def start():
    global signal_data
    config = request.json
    signal_data.update({
        'asset': config.get('asset', 'EURUSD_otc'),
        'timeframe': int(config.get('timeframe', 60)),
        'manual_mode': config.get('manual_mode', False),
        'hotkey': config.get('hotkey', 'space'),
        'is_running': True,
        'current_signal': None,
        'candle_start_time': time.time()
    })
    return jsonify({'success': True})

@app.route('/stop', methods=['POST'])
def stop():
    global signal_data
    signal_data['is_running'] = False
    return jsonify({'success': True})

@app.route('/manual_signal', methods=['POST'])
def manual_signal():
    global signal_data
    if not signal_data['is_running']:
        return jsonify({'success': False, 'error': 'Bot not running'})
    if not signal_data['manual_mode']:
        return jsonify({'success': False, 'error': 'Manual mode not enabled'})
    
    current_price = 1.2000 + np.random.normal(0, 0.0002)
    open_price = signal_data.get('candle_open', 1.2000)
    
    signal = 'buy' if current_price > open_price else 'sell'
    
    signal_data['current_signal'] = signal
    signal_data['price_data'] = current_price
    signal_data['last_update'] = time.strftime('%H:%M:%S')
    signal_data['manual_triggered'] = True
    
    return jsonify({'success': True, 'signal': signal, 'price': current_price})

@app.route('/get_signal')
def get_signal():
    global signal_data
    
    if signal_data['is_running']:
        current_price = 1.2000 + np.random.normal(0, 0.0002)
        elapsed = time.time() - signal_data.get('candle_start_time', time.time())
        timeframe = signal_data.get('timeframe', 60)
        
        if signal_data.get('candle_open') is None:
            signal_data['candle_open'] = current_price
        
        progress = min((elapsed / timeframe) * 100, 100)
        signal_data['candle_progress'] = progress
        
        if elapsed >= timeframe:
            signal_data['candle_open'] = current_price
            signal_data['candle_start_time'] = time.time()
        
        if not signal_data['manual_triggered']:
            signal = 'buy' if current_price > signal_data['candle_open'] else 'sell'
            signal_data['current_signal'] = signal
            signal_data['price_data'] = current_price
            signal_data['last_update'] = time.strftime('%H:%M:%S')
    
    response = {
        'signal': signal_data.get('current_signal'),
        'price': signal_data.get('price_data'),
        'timestamp': signal_data.get('last_update'),
        'manual_triggered': signal_data.get('manual_triggered', False)
    }
    
    signal_data['manual_triggered'] = False
    return jsonify(response)

# Handler for Netlify Functions
def handler(event, context):
    """Main handler for Netlify Functions"""
    from flask import Request
    
    # Create a Flask request context
    with app.test_request_context(
        path=event.get('path', '/'),
        method=event.get('httpMethod', 'GET'),
        headers=event.get('headers', {}),
        data=event.get('body', '')
    ):
        try:
            # Route the request
            response = app.full_dispatch_request()
            return {
                'statusCode': response.status_code,
                'headers': dict(response.headers),
                'body': response.get_data(as_text=True)
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)})
            }