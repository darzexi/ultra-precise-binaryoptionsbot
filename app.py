import time
import threading
import os
import logging
import asyncio
import re
import json
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify

# Import PocketOption
try:
    from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync
    POCKET_OPTION_AVAILABLE = True
except ImportError:
    POCKET_OPTION_AVAILABLE = False
    print("⚠️ BinaryOptionsToolsV2 not found. Install: pip install BinaryOptionsToolsV2")

# ==================== CONFIGURATION ====================
logging.getLogger('werkzeug').setLevel(logging.ERROR)

DEBUG_MODE = os.environ.get('DEBUG', 'False').lower() == 'true'
PORT = int(os.environ.get('PORT', 8080))
HOST = os.environ.get('HOST', '0.0.0.0')

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.debug = DEBUG_MODE

# ==================== CORS ====================
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# ==================== GLOBAL STATE ====================
signal_data = {
    'asset': 'EURUSD_otc',
    'timeframe': 60,
    'update_rate': 1,
    'manual_mode': False,
    'hotkey': 'space',
    'current_signal': None,
    'last_update': None,
    'price_data': None,
    'is_running': False,
    'ssid': None,
    'trade_expiration': 60,
    'use_expiration': False,
    'candle_progress': 0,
    'candle_high': None,
    'candle_low': None,
    'candle_open': None,
    'candle_start_time': None,
    'manual_triggered': False,
    'connection_status': 'disconnected',
    'consecutive_failures': 0
}

signal_thread = None
update_lock = threading.Lock()

logging.basicConfig(level=logging.ERROR if not DEBUG_MODE else logging.INFO)
logger = logging.getLogger(__name__)

# ==================== SSID EXTRACTION ====================
def extract_ssid(raw_ssid):
    """Extract SSID from various formats - returns just the session string"""
    if not raw_ssid:
        return None
    
    raw_ssid = raw_ssid.strip()
    
    # If it's the WebSocket message format: 42["auth",{"session":"xxx",...}]
    if raw_ssid.startswith('42["auth"'):
        try:
            # Extract the JSON array
            json_match = re.search(r'42(\[.*\])', raw_ssid)
            if json_match:
                data = json.loads(json_match.group(1))
                if data and len(data) > 1 and isinstance(data[1], dict):
                    if 'session' in data[1]:
                        return data[1]['session']
        except Exception as e:
            logger.debug(f"Failed to parse WebSocket message: {e}")
        
        # Fallback: regex extraction
        match = re.search(r'"session":"([^"]+)"', raw_ssid)
        if match:
            return match.group(1)
    
    # If it's a cookie string
    if 'ssid=' in raw_ssid:
        match = re.search(r'ssid=([^;]+)', raw_ssid)
        if match:
            return match.group(1)
    
    # If it's already the session value
    if re.match(r'^[a-zA-Z0-9_\-]{20,}$', raw_ssid):
        return raw_ssid
    
    # Try to clean up - remove quotes and braces
    cleaned = re.sub(r'[{}"\'\[\]]', '', raw_ssid)
    if re.match(r'^[a-zA-Z0-9_\-]{20,}$', cleaned):
        return cleaned
    
    return raw_ssid

def build_auth_message(ssid):
    """Build the full auth message for PocketOption"""
    return f'42["auth",{{"session":"{ssid}","isDemo":1,"uid":0,"platform":2,"isFastHistory":true,"isOptimized":true}}]'

# ==================== HELPER FUNCTIONS ====================
def run_async(coro):
    """Run async function in a new event loop"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# ==================== HTML TEMPLATE ====================
HTML_TEMPLATE = '''<!DOCTYPE html>
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
            max-width: 900px;
            width: 100%;
            max-height: 98vh;
            overflow-y: auto;
        }
        h1 { color: #333; margin-bottom: 10px; text-align: center; }
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
        .accuracy-badge {
            display: inline-block;
            background: #28a745;
            color: white;
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: bold;
            margin-top: 10px;
        }
        .candle-progress {
            margin-top: 15px;
            padding: 10px;
            background: rgba(0,0,0,0.05);
            border-radius: 10px;
        }
        .candle-progress-bar {
            height: 20px;
            background: #e9ecef;
            border-radius: 10px;
            overflow: hidden;
            margin-top: 5px;
        }
        .candle-progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea, #764ba2);
            transition: width 0.5s ease;
            border-radius: 10px;
        }
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
        .setting-group input[type="checkbox"] { width: auto; margin-top: 5px; }
        .checkbox-group { display: flex; align-items: center; gap: 10px; }
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
        .btn-info { background: #17a2b8; color: white; }
        .btn-info:hover { background: #138496; transform: translateY(-2px); }
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
        .hotkey-indicator {
            display: inline-block;
            background: #333;
            color: white;
            padding: 2px 10px;
            border-radius: 5px;
            font-weight: bold;
            margin-left: 5px;
        }
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
        .log-entry.precise { color: #ffd700; }
        .log-entry.manual { color: #ff9800; font-weight: bold; }
        .log-entry.connection { color: #00bcd4; }
        .log-entry.warning { color: #ff9800; }
        .ssid-input {
            margin-top: 15px;
            padding: 10px;
            background: #fff3cd;
            border-radius: 10px;
            border: 1px solid #ffc107;
        }
        .ssid-input input {
            width: 100%;
            padding: 8px 12px;
            border: 2px solid #ddd;
            border-radius: 8px;
            font-size: 14px;
            font-family: monospace;
        }
        .expiration-input {
            margin-top: 10px;
            padding: 10px;
            background: #e7f3ff;
            border-radius: 10px;
            border: 1px solid #b3d9ff;
        }
        .candle-info {
            display: flex;
            justify-content: space-around;
            margin-top: 10px;
            font-size: 14px;
            flex-wrap: wrap;
        }
        .candle-info span { font-weight: bold; margin: 2px 5px; }
        .candle-high { color: #28a745; }
        .candle-low { color: #dc3545; }
        .candle-open { color: #ffc107; }
        .status-connected { color: #28a745; }
        .status-disconnected { color: #dc3545; }
        .help-text {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 8px;
            margin-top: 10px;
            font-size: 13px;
            border-left: 3px solid #17a2b8;
        }
        .help-text code {
            background: #e9ecef;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 12px;
        }
        .extracted-ssid {
            color: #28a745;
            font-weight: bold;
            margin-top: 5px;
            padding: 5px;
            background: #d4edda;
            border-radius: 5px;
            display: none;
        }
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
        <p class="subtitle">Fixed SSID Format - Works on Replit</p>

        <div id="signalDisplay" class="signal-display">
            <div style="font-size: 14px; color: #888;">Current Signal</div>
            <div class="signal-text" id="signalText">WAITING</div>
            <div class="signal-price" id="signalPrice">Price: --</div>
            <div class="signal-time" id="signalTime">Last Update: --</div>
            <div class="accuracy-badge" id="accuracyBadge">🎯 Live Mode</div>
            
            <div class="candle-progress">
                <div style="display: flex; justify-content: space-between;">
                    <span>Candle Progress</span>
                    <span id="candleProgressText">0%</span>
                </div>
                <div class="candle-progress-bar">
                    <div class="candle-progress-fill" id="candleProgressFill" style="width: 0%;"></div>
                </div>
                <div class="candle-info">
                    <span class="candle-open">Open: <span id="candleOpen">--</span></span>
                    <span class="candle-high">High: <span id="candleHigh">--</span></span>
                    <span class="candle-low">Low: <span id="candleLow">--</span></span>
                    <span>Current: <span id="candleCurrent">--</span></span>
                </div>
                <div style="margin-top: 5px; font-size: 12px; color: #888;">
                    Time Remaining: <span id="candleTimeRemaining">--</span>
                </div>
            </div>
        </div>

        <div class="ssid-input">
            <label style="font-weight: 600; display: block; margin-bottom: 5px;">PocketOption SSID:</label>
            <input type="text" id="ssidInput" placeholder="Paste your SSID value" value="">
            <small style="color: #856404; display: block; margin-top: 5px;">
                💡 Paste only the session value: <strong>r7seffi1r662i33roiengjikcm</strong>
            </small>
            <div class="extracted-ssid" id="extractedSsidDisplay">
                ✅ Extracted: <span id="extractedSsidText"></span>
            </div>
        </div>

        <div class="help-text">
            <strong>🔍 Get your SSID:</strong><br>
            1. Log in to <a href="https://pocketoption.com" target="_blank">pocketoption.com</a><br>
            2. Press <code>F12</code> → <code>Application</code> tab → <code>Cookies</code><br>
            3. Find <code>ssid</code> and copy the value<br>
            4. Paste only the session value (like <code>r7seffi1r662i33roiengjikcm</code>)
        </div>

        <div class="settings-grid">
            <div class="setting-group">
                <label for="assetSelect">Asset Symbol</label>
                <select id="assetSelect">
                    <option value="EURUSD_otc">EUR/USD OTC</option>
                    <option value="GBPUSD_otc">GBP/USD OTC</option>
                    <option value="USDJPY_otc">USD/JPY OTC</option>
                    <option value="AUDUSD_otc">AUD/USD OTC</option>
                    <option value="BTCUSD_otc">BTC/USD OTC</option>
                    <option value="ETHUSD_otc">ETH/USD OTC</option>
                    <option value="XAUUSD_otc">Gold OTC</option>
                    <option value="XAGUSD_otc">Silver OTC</option>
                </select>
            </div>
            <div class="setting-group">
                <label for="timeframeSelect">Candle Timeframe (seconds)</label>
                <select id="timeframeSelect">
                    <option value="5">5s</option>
                    <option value="10">10s</option>
                    <option value="15">15s</option>
                    <option value="30">30s</option>
                    <option value="60" selected>60s</option>
                    <option value="120">120s</option>
                    <option value="300">300s</option>
                </select>
            </div>
            <div class="setting-group">
                <label for="updateRate">Update Rate (seconds)</label>
                <input type="number" id="updateRate" value="1" min="0.5" max="10" step="0.5">
            </div>
            <div class="setting-group">
                <label>Manual Mode</label>
                <div class="checkbox-group">
                    <input type="checkbox" id="manualMode">
                    <label for="manualMode">Enable Manual Signal</label>
                </div>
            </div>
            <div class="setting-group">
                <label>Data Source</label>
                <div class="checkbox-group">
                    <input type="checkbox" id="websocketMode" disabled>
                    <label for="websocketMode" style="color: #999;">Compile Candles</label>
                </div>
            </div>
            <div class="setting-group">
                <label for="hotkeyInput">Hotkey</label>
                <input type="text" id="hotkeyInput" value="space" maxlength="20">
            </div>
        </div>

        <div class="expiration-input">
            <div class="checkbox-group">
                <input type="checkbox" id="useExpiration">
                <label for="useExpiration">Use Trade Expiration</label>
            </div>
            <div style="margin-top: 10px;">
                <label style="font-weight: 600; display: block; margin-bottom: 5px;">Trade Expiration (seconds):</label>
                <input type="number" id="tradeExpiration" value="60" min="3" max="300">
            </div>
        </div>

        <div class="button-group">
            <button class="btn btn-success" id="startBtn">▶ Start Bot</button>
            <button class="btn btn-danger" id="stopBtn" disabled>⏹ Stop</button>
            <button class="btn btn-warning" id="manualSignalBtn" disabled>⚡ Manual</button>
            <button class="btn btn-info" id="testBtn">🔌 Test</button>
            <button class="btn btn-secondary" id="clearLogsBtn">🗑 Clear</button>
        </div>

        <div class="status-bar">
            <div class="status-item"><span>Status:</span><span id="statusText">Stopped</span></div>
            <div class="status-item"><span>Connection:</span><span id="connectionStatus" class="status-disconnected">Disconnected</span></div>
            <div class="status-item"><span>Mode:</span><span id="modeDisplay">Automatic</span></div>
            <div class="status-item"><span>Data Source:</span><span id="dataSourceDisplay">Compile Candles</span></div>
            <div class="status-item"><span>SSID:</span><span id="ssidStatus">Not Set</span></div>
        </div>

        <div class="log-area" id="logArea">
            <div class="log-entry">[System] Bot initialized.</div>
            <div class="log-entry connection">💡 Enter your SSID and click Test</div>
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
        const connectionStatus = document.getElementById('connectionStatus');
        const modeDisplay = document.getElementById('modeDisplay');
        const dataSourceDisplay = document.getElementById('dataSourceDisplay');
        const ssidStatus = document.getElementById('ssidStatus');
        const logArea = document.getElementById('logArea');

        const candleProgressFill = document.getElementById('candleProgressFill');
        const candleProgressText = document.getElementById('candleProgressText');
        const candleOpen = document.getElementById('candleOpen');
        const candleHigh = document.getElementById('candleHigh');
        const candleLow = document.getElementById('candleLow');
        const candleCurrent = document.getElementById('candleCurrent');
        const candleTimeRemaining = document.getElementById('candleTimeRemaining');

        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');
        const manualSignalBtn = document.getElementById('manualSignalBtn');
        const clearLogsBtn = document.getElementById('clearLogsBtn');
        const testBtn = document.getElementById('testBtn');

        const ssidInput = document.getElementById('ssidInput');
        const assetSelect = document.getElementById('assetSelect');
        const timeframeSelect = document.getElementById('timeframeSelect');
        const updateRate = document.getElementById('updateRate');
        const manualMode = document.getElementById('manualMode');
        const hotkeyInput = document.getElementById('hotkeyInput');
        const useExpiration = document.getElementById('useExpiration');
        const tradeExpiration = document.getElementById('tradeExpiration');

        testBtn.addEventListener('click', function() {
            const raw = ssidInput.value.trim();
            if (!raw) {
                addLog('Please enter SSID first', 'error');
                return;
            }
            
            addLog('🔌 Testing connection...', 'connection');
            
            fetch('/test_connection', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ raw: raw })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    connectionStatus.textContent = 'Connected ✓';
                    connectionStatus.className = 'status-connected';
                    ssidStatus.textContent = 'Valid ✓';
                    ssidStatus.style.color = '#28a745';
                    addLog('✅ Connection successful! Price: ' + data.price, 'precise');
                } else {
                    connectionStatus.textContent = 'Failed ✗';
                    connectionStatus.className = 'status-disconnected';
                    ssidStatus.textContent = 'Invalid';
                    ssidStatus.style.color = '#dc3545';
                    addLog('❌ Connection failed: ' + data.error, 'error');
                }
            })
            .catch(err => {
                addLog('❌ Error: ' + err.message, 'error');
            });
        });

        function triggerManualSignal() {
            if (!isRunning || !manualMode.checked) return;
            
            const now = Date.now();
            if (now - lastManualTriggerTime < 500) return;
            lastManualTriggerTime = now;
            
            fetch('/manual_signal', { method: 'POST' })
                .then(r => r.json())
                .then(data => {
                    if (data.success) {
                        signalText.textContent = data.signal.toUpperCase();
                        signalPrice.textContent = `Price: ${data.price || '--'}`;
                        signalDisplay.className = 'signal-display manual';
                        addLog('⚡ Manual ' + data.signal.toUpperCase(), 'manual');
                    }
                });
        }

        manualSignalBtn.onclick = triggerManualSignal;
        document.addEventListener('keydown', function(e) {
            if (!isRunning || !manualMode.checked) return;
            const key = e.key.toLowerCase();
            if (key === (hotkeyInput.value.trim() || 'space').toLowerCase()) {
                e.preventDefault();
                triggerManualSignal();
            }
        });

        startBtn.onclick = function() {
            const raw = ssidInput.value.trim();
            if (!raw) {
                addLog('ERROR: SSID required', 'error');
                return;
            }

            addLog('🚀 Starting bot...', 'connection');

            fetch('/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    raw: raw,
                    asset: assetSelect.value,
                    timeframe: parseInt(timeframeSelect.value),
                    update_rate: parseFloat(updateRate.value) || 1,
                    manual_mode: manualMode.checked,
                    hotkey: hotkeyInput.value.trim() || 'space',
                    use_expiration: useExpiration.checked,
                    trade_expiration: parseInt(tradeExpiration.value) || 60
                })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    isRunning = true;
                    startBtn.disabled = true;
                    stopBtn.disabled = false;
                    if (manualMode.checked) manualSignalBtn.disabled = false;
                    statusText.textContent = 'Running';
                    statusText.style.color = '#28a745';
                    ssidStatus.textContent = 'Active';
                    ssidStatus.style.color = '#28a745';
                    addLog('✅ Bot started successfully!', 'precise');
                    startPolling();
                } else {
                    addLog('❌ Failed to start: ' + data.error, 'error');
                }
            })
            .catch(err => {
                addLog('❌ Error: ' + err.message, 'error');
            });
        };

        stopBtn.onclick = function() {
            fetch('/stop', { method: 'POST' })
                .then(() => {
                    isRunning = false;
                    startBtn.disabled = false;
                    stopBtn.disabled = true;
                    manualSignalBtn.disabled = true;
                    statusText.textContent = 'Stopped';
                    statusText.style.color = '#dc3545';
                    connectionStatus.textContent = 'Disconnected';
                    connectionStatus.className = 'status-disconnected';
                    stopPolling();
                    addLog('⏹ Bot stopped', 'info');
                });
        };

        clearLogsBtn.onclick = function() {
            logArea.innerHTML = '';
            addLog('🗑 Logs cleared', 'info');
        };

        function startPolling() {
            if (updateInterval) clearInterval(updateInterval);
            updateInterval = setInterval(() => {
                fetch('/get_signal')
                    .then(r => r.json())
                    .then(data => {
                        if (data.signal && data.signal !== 'pending' && !data.manual_triggered) {
                            signalText.textContent = data.signal.toUpperCase();
                            signalPrice.textContent = `Price: ${data.price || '--'}`;
                            signalTime.textContent = `Last Update: ${data.timestamp || new Date().toLocaleTimeString()}`;
                            signalDisplay.className = 'signal-display ' + (data.signal === 'buy' ? 'buy' : data.signal === 'sell' ? 'sell' : '');
                        }
                        if (data.candle_data) {
                            const cd = data.candle_data;
                            candleProgressFill.style.width = cd.progress + '%';
                            candleProgressText.textContent = Math.round(cd.progress) + '%';
                            candleOpen.textContent = cd.open?.toFixed(5) || '--';
                            candleHigh.textContent = cd.high?.toFixed(5) || '--';
                            candleLow.textContent = cd.low?.toFixed(5) || '--';
                            candleCurrent.textContent = cd.current?.toFixed(5) || '--';
                            candleTimeRemaining.textContent = cd.time_remaining || '--';
                        }
                        if (data.connection_status) {
                            if (data.connection_status === 'connected') {
                                connectionStatus.textContent = 'Connected ✓';
                                connectionStatus.className = 'status-connected';
                            } else {
                                connectionStatus.textContent = 'Disconnected ✗';
                                connectionStatus.className = 'status-disconnected';
                            }
                        }
                    })
                    .catch(err => console.error('Polling error:', err));
            }, 500);
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
            const timestamp = new Date().toLocaleTimeString();
            entry.textContent = `[${timestamp}] ${message}`;
            logArea.appendChild(entry);
            logArea.scrollTop = logArea.scrollHeight;
        }
    </script>
</body>
</html>
'''

# ==================== ROUTES ====================
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/test_connection', methods=['POST'])
def test_connection():
    """Test connection with auto-extracted SSID"""
    try:
        data = request.json
        raw = data.get('raw', '').strip()
        
        if not raw:
            return jsonify({'success': False, 'error': 'SSID required'})
        
        # Extract SSID
        ssid = extract_ssid(raw)
        
        if not ssid or len(ssid) < 10:
            return jsonify({'success': False, 'error': f'Could not extract valid SSID from input'})
        
        if not POCKET_OPTION_AVAILABLE:
            return jsonify({'success': False, 'error': 'BinaryOptionsToolsV2 not installed'})
        
        # Test connection with the auth message
        auth_message = build_auth_message(ssid)
        
        async def test():
            try:
                client = PocketOptionAsync(ssid=auth_message)
                # Try to get candles
                candles = await asyncio.wait_for(
                    client.compile_candles('EURUSD_otc', 60, 5),
                    timeout=15
                )
                await client.disconnect()
                if candles and len(candles) > 0:
                    return {
                        'success': True, 
                        'price': candles[-1].get('close'),
                        'extracted': ssid,
                        'candle_count': len(candles)
                    }
                return {'success': False, 'error': 'No candle data returned'}
            except asyncio.TimeoutError:
                return {'success': False, 'error': 'Connection timeout - SSID may be invalid or expired'}
            except Exception as e:
                return {'success': False, 'error': str(e)}
        
        result = run_async(test())
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/start', methods=['POST'])
def start_bot():
    global signal_thread, signal_data
    
    with update_lock:
        if signal_data['is_running']:
            return jsonify({'success': False, 'error': 'Already running'})
        
        config = request.json
        raw = config.get('raw', '').strip()
        
        if not raw:
            return jsonify({'success': False, 'error': 'SSID required'})
        
        # Extract SSID
        ssid = extract_ssid(raw)
        
        if not ssid or len(ssid) < 10:
            return jsonify({'success': False, 'error': 'Could not extract valid SSID'})
        
        if not POCKET_OPTION_AVAILABLE:
            return jsonify({'success': False, 'error': 'BinaryOptionsToolsV2 not installed'})
        
        # Build auth message
        auth_message = build_auth_message(ssid)
        
        # Validate SSID
        async def validate():
            try:
                client = PocketOptionAsync(ssid=auth_message)
                candles = await asyncio.wait_for(
                    client.compile_candles('EURUSD_otc', 60, 1),
                    timeout=10
                )
                await client.disconnect()
                return candles is not None and len(candles) > 0
            except:
                return False
        
        if not run_async(validate()):
            return jsonify({'success': False, 'error': 'Invalid SSID or connection failed'})
        
        signal_data.update({
            'ssid': auth_message,
            'asset': config.get('asset', 'EURUSD_otc'),
            'timeframe': int(config.get('timeframe', 60)),
            'update_rate': float(config.get('update_rate', 1)),
            'manual_mode': config.get('manual_mode', False),
            'hotkey': config.get('hotkey', 'space'),
            'is_running': True,
            'use_expiration': config.get('use_expiration', False),
            'trade_expiration': int(config.get('trade_expiration', 60)),
            'current_signal': None,
            'price_data': None,
            'consecutive_failures': 0,
            'connection_status': 'connected'
        })
        
        signal_thread = threading.Thread(target=run_signal_bot, daemon=True)
        signal_thread.start()
        
        return jsonify({'success': True, 'extracted_ssid': ssid})

@app.route('/stop', methods=['POST'])
def stop_bot():
    global signal_data
    with update_lock:
        signal_data['is_running'] = False
        signal_data['connection_status'] = 'disconnected'
    return jsonify({'success': True})

@app.route('/manual_signal', methods=['POST'])
def manual_signal():
    global signal_data
    if not signal_data['is_running']:
        return jsonify({'success': False, 'error': 'Bot not running'})
    if not signal_data['manual_mode']:
        return jsonify({'success': False, 'error': 'Manual mode disabled'})
    
    price_data = fetch_current_price()
    if not price_data:
        return jsonify({'success': False, 'error': 'No price data'})
    
    current_price = price_data.get('price')
    open_price = signal_data.get('candle_open', current_price)
    
    signal = 'buy' if current_price > open_price else 'sell' if current_price < open_price else 'hold'
    
    with update_lock:
        signal_data['current_signal'] = signal
        signal_data['price_data'] = current_price
        signal_data['last_update'] = datetime.now().strftime('%H:%M:%S')
        signal_data['manual_triggered'] = True
    
    return jsonify({'success': True, 'signal': signal, 'price': current_price})

@app.route('/get_signal')
def get_signal():
    global signal_data
    with update_lock:
        response = {
            'signal': signal_data.get('current_signal'),
            'price': signal_data.get('price_data'),
            'timestamp': signal_data.get('last_update'),
            'manual_triggered': signal_data.get('manual_triggered', False),
            'connection_status': signal_data.get('connection_status', 'disconnected'),
            'candle_data': {
                'progress': signal_data.get('candle_progress', 0),
                'open': signal_data.get('candle_open'),
                'high': signal_data.get('candle_high'),
                'low': signal_data.get('candle_low'),
                'current': signal_data.get('price_data'),
                'time_remaining': signal_data.get('candle_time_remaining', '--')
            }
        }
        if signal_data.get('manual_triggered'):
            signal_data['manual_triggered'] = False
        return jsonify(response)

# ==================== CORE LOGIC ====================
def fetch_current_price():
    """Fetch price using compile_candles"""
    global signal_data
    
    auth_message = signal_data.get('ssid')
    asset = signal_data.get('asset', 'EURUSD_otc')
    
    if not auth_message:
        return None
    
    try:
        async def get_price():
            try:
                client = PocketOptionAsync(ssid=auth_message)
                candles = await asyncio.wait_for(
                    client.compile_candles(asset, 5, 1),
                    timeout=8
                )
                await client.disconnect()
                if candles and len(candles) > 0:
                    latest = candles[-1]
                    return {
                        'price': float(latest.get('close', 0)),
                        'timestamp': time.time()
                    }
                return None
            except asyncio.TimeoutError:
                logger.debug("Price fetch timeout")
                return None
            except Exception as e:
                logger.debug(f"Price fetch error: {e}")
                return None
        
        result = run_async(get_price())
        return result
        
    except Exception as e:
        logger.error(f"Price fetch error: {e}")
        return None

def run_signal_bot():
    global signal_data
    
    logger.info("Signal bot started")
    
    candle_start_time = time.time()
    candle_open_price = None
    candle_high_price = None
    candle_low_price = None
    current_candle_data = []
    last_signal_time = 0
    consecutive_failures = 0
    
    while signal_data['is_running']:
        try:
            current_time = time.time()
            
            if current_time - last_signal_time < signal_data['update_rate']:
                time.sleep(0.1)
                continue
            
            # Fetch current price
            price_data = fetch_current_price()
            
            if price_data is None:
                consecutive_failures += 1
                signal_data['consecutive_failures'] = consecutive_failures
                if consecutive_failures > 5:
                    signal_data['connection_status'] = 'disconnected'
                    logger.warning(f"Connection lost ({consecutive_failures} failures)")
                time.sleep(1)
                continue
            
            consecutive_failures = 0
            signal_data['connection_status'] = 'connected'
            signal_data['consecutive_failures'] = 0
            
            current_price = price_data.get('price', 0)
            timestamp = price_data.get('timestamp', current_time)
            
            if candle_open_price is None:
                candle_open_price = current_price
                candle_high_price = current_price
                candle_low_price = current_price
                candle_start_time = timestamp
                logger.info(f"New candle started at {current_price:.5f}")
            
            candle_high_price = max(candle_high_price, current_price)
            candle_low_price = min(candle_low_price, current_price)
            
            timeframe = signal_data['timeframe']
            elapsed = current_time - candle_start_time
            progress = min((elapsed / timeframe) * 100, 100)
            
            if elapsed >= timeframe:
                current_candle_data.append({
                    'open': candle_open_price,
                    'high': candle_high_price,
                    'low': candle_low_price,
                    'close': current_price,
                    'time': candle_start_time
                })
                if len(current_candle_data) > 50:
                    current_candle_data.pop(0)
                
                logger.info(f"Candle: O={candle_open_price:.5f} H={candle_high_price:.5f} L={candle_low_price:.5f} C={current_price:.5f}")
                
                candle_open_price = current_price
                candle_high_price = current_price
                candle_low_price = current_price
                candle_start_time = current_time
                progress = 0
            
            with update_lock:
                signal_data['candle_open'] = candle_open_price
                signal_data['candle_high'] = candle_high_price
                signal_data['candle_low'] = candle_low_price
                signal_data['candle_progress'] = progress
                signal_data['candle_time_remaining'] = f"{max(0, timeframe - elapsed):.1f}s"
                signal_data['price_data'] = current_price
            
            signal = generate_signal(
                current_price, 
                candle_open_price, 
                candle_high_price, 
                candle_low_price, 
                progress, 
                current_candle_data
            )
            
            if signal != 'hold':
                logger.info(f"Signal: {signal.upper()} at {current_price:.5f}")
            
            with update_lock:
                signal_data['current_signal'] = signal
                signal_data['last_update'] = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            
            last_signal_time = current_time
            
        except Exception as e:
            logger.error(f"Signal bot error: {e}")
            time.sleep(1)
    
    logger.info("Signal bot stopped")

def generate_signal(current_price, open_price, high_price, low_price, progress, candle_history):
    """Generate signal based on price action"""
    try:
        if signal_data.get('use_expiration', False):
            timeframe = signal_data['timeframe']
            trade_exp = signal_data.get('trade_expiration', 60)
            remaining = timeframe - (progress / 100) * timeframe
            if remaining < trade_exp:
                return 'hold'
        
        if progress < 10 and len(candle_history) >= 2:
            prev_close = candle_history[-2].get('close', current_price)
            if current_price > prev_close * 1.0005:
                return 'buy'
            elif current_price < prev_close * 0.9995:
                return 'sell'
        
        if high_price > open_price * 1.002 and current_price > open_price:
            return 'buy'
        elif low_price < open_price * 0.998 and current_price < open_price:
            return 'sell'
        
        if len(candle_history) >= 3:
            prev_candle = candle_history[-2]
            if prev_candle:
                if (prev_candle.get('close', 0) < prev_candle.get('open', 0) and 
                    current_price > prev_candle.get('high', 0)):
                    return 'buy'
                if (prev_candle.get('close', 0) > prev_candle.get('open', 0) and 
                    current_price < prev_candle.get('low', 0)):
                    return 'sell'
        
        threshold = 0.0005
        if current_price > open_price * (1 + threshold):
            return 'buy'
        elif current_price < open_price * (1 - threshold):
            return 'sell'
        
        return 'hold'
        
    except Exception as e:
        logger.error(f"Signal generation error: {e}")
        return 'hold'

# ==================== MAIN ====================
if __name__ == '__main__':
    print("\n" + "="*50)
    print("🚀 PocketOption Signal Bot")
    print("="*50)
    print(f"Server: http://{HOST}:{PORT}")
    print(f"Library: {'Available' if POCKET_OPTION_AVAILABLE else 'Not Available'}")
    print("="*50 + "\n")
    print("💡 Enter ONLY the SSID value (not the full message):")
    print("   Example: r7seffi1r662i33roiengjikcm")
    print("="*50 + "\n")
    
    app.run(host=HOST, port=PORT, debug=DEBUG_MODE, threaded=True)
