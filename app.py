import asyncio
import json
import time
import threading
import queue
import os
import logging
import gc
import requests
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify
import numpy as np

# Try to import PocketOption for data processing
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
    'update_rate': 0.5,
    'manual_mode': False,
    'websocket_mode': False,  # Disabled by default
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
    'consecutive_failures': 0,
    'candle_history': []
}

trading_client = None
signal_thread = None
update_lock = threading.Lock()

logging.basicConfig(level=logging.ERROR if not DEBUG_MODE else logging.INFO)
logger = logging.getLogger(__name__)

# ==================== POCKETOPTION REST API ====================
class PocketOptionREST:
    """Pure REST API client for PocketOption - No WebSocket"""
    
    def __init__(self, ssid):
        self.ssid = ssid
        self.session = requests.Session()
        self.session.cookies.set('ssid', ssid)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://pocketoption.com/en/',
            'Origin': 'https://pocketoption.com',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        })
        self.base_url = 'https://pocketoption.com'
        self.last_price = None
        self.last_candles = []
    
    def get_current_price(self, asset):
        """Get current price via REST API"""
        try:
            # Try main endpoint
            response = self.session.get(
                f'{self.base_url}/api/trade/current-price',
                params={'asset': asset},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('price'):
                    price = float(data['price'])
                    self.last_price = price
                    return price
        except Exception as e:
            logger.debug(f"Main price endpoint failed: {e}")
        
        # Try alternative endpoint
        try:
            response = self.session.get(
                f'{self.base_url}/api/assets/current-price',
                params={'asset': asset},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('price'):
                    price = float(data['price'])
                    self.last_price = price
                    return price
        except Exception as e:
            logger.debug(f"Alt price endpoint failed: {e}")
        
        # Try quotes endpoint
        try:
            response = self.session.get(
                f'{self.base_url}/api/trade/quotes',
                params={'asset': asset},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and data['data'].get('price'):
                    price = float(data['data']['price'])
                    self.last_price = price
                    return price
        except Exception as e:
            logger.debug(f"Quotes endpoint failed: {e}")
        
        return None
    
    def get_candles(self, asset, timeframe, count=100):
        """Get candle data via REST API"""
        try:
            # Try candles endpoint
            response = self.session.get(
                f'{self.base_url}/api/trade/candles',
                params={
                    'asset': asset,
                    'timeframe': timeframe,
                    'count': count
                },
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('candles'):
                    self.last_candles = data['candles']
                    return data['candles']
        except Exception as e:
            logger.debug(f"Candles endpoint failed: {e}")
        
        # Try history endpoint
        try:
            response = self.session.get(
                f'{self.base_url}/api/trade/history',
                params={
                    'asset': asset,
                    'timeframe': timeframe,
                    'limit': count
                },
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    self.last_candles = data['data']
                    return data['data']
        except Exception as e:
            logger.debug(f"History endpoint failed: {e}")
        
        # Try OTC candles
        try:
            response = self.session.get(
                f'{self.base_url}/api/trade/otc-candles',
                params={
                    'asset': asset,
                    'timeframe': timeframe,
                    'count': count
                },
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('candles'):
                    self.last_candles = data['candles']
                    return data['candles']
        except Exception as e:
            logger.debug(f"OTC candles endpoint failed: {e}")
        
        return None
    
    def get_asset_info(self, asset):
        """Get asset information"""
        try:
            response = self.session.get(
                f'{self.base_url}/api/assets/info',
                params={'asset': asset},
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
        except:
            pass
        return None

# ==================== HTML TEMPLATE ====================
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PocketOption Signal Bot - REST API</title>
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
        @media (max-width: 600px) {
            .settings-grid { grid-template-columns: 1fr; }
            .button-group { flex-direction: column; }
            .btn { width: 100%; }
        }
        .method-badge {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 10px;
            font-size: 11px;
            font-weight: bold;
            margin-left: 5px;
        }
        .method-rest { background: #ff9800; color: white; }
        .method-websocket { background: #4caf50; color: white; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 PocketOption Signal Bot</h1>
        <p class="subtitle">REST API Mode - No WebSocket Required</p>

        <div id="signalDisplay" class="signal-display">
            <div style="font-size: 14px; color: #888;">Current Signal</div>
            <div class="signal-text" id="signalText">WAITING</div>
            <div class="signal-price" id="signalPrice">Price: --</div>
            <div class="signal-time" id="signalTime">Last Update: --</div>
            <div class="accuracy-badge" id="accuracyBadge">🎯 REST API Mode</div>
            
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
            <input type="text" id="ssidInput" placeholder="Enter your SSID from cookies" value="">
            <small style="color: #856404; display: block; margin-top: 5px;">Get SSID from browser cookies (ssid value)</small>
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
                <input type="number" id="updateRate" value="0.5" min="0.1" max="5" step="0.1">
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
                    <label for="websocketMode" style="color: #999;">REST API Only</label>
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
            <div class="status-item"><span>Data Source:</span><span id="dataSourceDisplay">REST API <span class="method-badge method-rest">REST</span></span></div>
            <div class="status-item"><span>SSID:</span><span id="ssidStatus">Not Set</span></div>
        </div>

        <div class="log-area" id="logArea">
            <div class="log-entry">[System] Bot initialized. REST API Mode.</div>
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

        hotkeyInput.addEventListener('input', function() {
            const key = this.value.trim() || 'space';
            document.getElementById('hotkeyDisplay').innerHTML = `<span class="hotkey-indicator">${key}</span>`;
        });

        testBtn.addEventListener('click', function() {
            const ssid = ssidInput.value.trim();
            if (!ssid) {
                addLog('Please enter SSID first', 'error');
                return;
            }
            addLog('Testing REST API connection...', 'connection');
            fetch('/test_connection', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ssid: ssid })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    connectionStatus.textContent = 'Connected ✓';
                    connectionStatus.className = 'status-connected';
                    ssidStatus.textContent = 'Valid';
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
            .catch(err => addLog('Error: ' + err.message, 'error'));
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
            const ssid = ssidInput.value.trim();
            if (!ssid) {
                addLog('ERROR: SSID required', 'error');
                return;
            }

            fetch('/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    ssid: ssid,
                    asset: assetSelect.value,
                    timeframe: parseInt(timeframeSelect.value),
                    update_rate: parseFloat(updateRate.value) || 0.5,
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
                    ssidStatus.textContent = 'Set';
                    ssidStatus.style.color = '#28a745';
                    addLog('✅ Bot started (REST API)', 'precise');
                    startPolling();
                } else {
                    addLog('❌ Failed: ' + data.error, 'error');
                }
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
                    addLog('Bot stopped', 'info');
                });
        };

        clearLogsBtn.onclick = function() {
            logArea.innerHTML = '';
            addLog('Logs cleared', 'info');
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
            entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
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
    """Test PocketOption REST API connection"""
    try:
        data = request.json
        ssid = data.get('ssid', '').strip()
        
        if not ssid:
            return jsonify({'success': False, 'error': 'SSID required'})
        
        # Test REST API
        rest = PocketOptionREST(ssid)
        price = rest.get_current_price('EURUSD_otc')
        
        if price:
            return jsonify({'success': True, 'price': price, 'method': 'REST API'})
        else:
            # Try to get candles as fallback
            candles = rest.get_candles('EURUSD_otc', 60, 1)
            if candles and len(candles) > 0:
                return jsonify({'success': True, 'price': candles[-1].get('close'), 'method': 'REST API (candles)'})
        
        return jsonify({'success': False, 'error': 'Could not fetch data. Check SSID.'})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/start', methods=['POST'])
def start_bot():
    global signal_thread, signal_data, trading_client
    
    with update_lock:
        if signal_data['is_running']:
            return jsonify({'success': False, 'error': 'Already running'})
        
        config = request.json
        ssid = config.get('ssid', '').strip()
        
        if not ssid:
            return jsonify({'success': False, 'error': 'SSID required'})
        
        signal_data.update({
            'ssid': ssid,
            'asset': config.get('asset', 'EURUSD_otc'),
            'timeframe': int(config.get('timeframe', 60)),
            'update_rate': float(config.get('update_rate', 0.5)),
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
        
        # Initialize REST client
        trading_client = PocketOptionREST(ssid)
        
        # Test connection
        test_price = trading_client.get_current_price(signal_data['asset'])
        if not test_price:
            signal_data['is_running'] = False
            return jsonify({'success': False, 'error': 'Could not connect. Check SSID.'})
        
        signal_thread = threading.Thread(target=run_signal_bot, daemon=True)
        signal_thread.start()
        
        return jsonify({'success': True})

@app.route('/stop', methods=['POST'])
def stop_bot():
    global signal_data, trading_client
    with update_lock:
        signal_data['is_running'] = False
        signal_data['connection_status'] = 'disconnected'
        trading_client = None
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
def run_signal_bot():
    global signal_data, trading_client
    
    logger.info("Signal bot started (REST API mode)")
    
    candle_start_time = time.time()
    candle_open_price = None
    candle_high_price = None
    candle_low_price = None
    current_candle_data = []
    last_signal_time = 0
    consecutive_failures = 0
    last_candle_refresh = 0
    
    # Get initial candles
    refresh_candles()
    
    while signal_data['is_running']:
        try:
            current_time = time.time()
            
            if current_time - last_signal_time < signal_data['update_rate']:
                time.sleep(0.05)
                continue
            
            # Refresh candles periodically
            if current_time - last_candle_refresh > 30:
                refresh_candles()
                last_candle_refresh = current_time
            
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
            
            # Initialize candle if needed
            if candle_open_price is None:
                candle_open_price = current_price
                candle_high_price = current_price
                candle_low_price = current_price
                candle_start_time = timestamp
                logger.info(f"New candle started at {current_price:.5f}")
            
            # Update candle extremes
            candle_high_price = max(candle_high_price, current_price)
            candle_low_price = min(candle_low_price, current_price)
            
            timeframe = signal_data['timeframe']
            elapsed = current_time - candle_start_time
            progress = min((elapsed / timeframe) * 100, 100)
            
            # Check for candle completion
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
            
            # Update state
            with update_lock:
                signal_data['candle_open'] = candle_open_price
                signal_data['candle_high'] = candle_high_price
                signal_data['candle_low'] = candle_low_price
                signal_data['candle_progress'] = progress
                signal_data['candle_time_remaining'] = f"{max(0, timeframe - elapsed):.1f}s"
                signal_data['price_data'] = current_price
            
            # Generate signal
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

def refresh_candles():
    """Refresh candle history"""
    global signal_data, trading_client
    
    if not trading_client:
        return
    
    try:
        asset = signal_data['asset']
        timeframe = signal_data['timeframe']
        
        candles = trading_client.get_candles(asset, timeframe, 50)
        if candles:
            with update_lock:
                signal_data['candle_history'] = candles
                if len(candles) > 0:
                    latest = candles[-1]
                    signal_data['candle_open'] = latest.get('open')
                    signal_data['candle_high'] = latest.get('max')
                    signal_data['candle_low'] = latest.get('min')
            logger.info(f"Refreshed {len(candles)} candles")
    except Exception as e:
        logger.debug(f"Candle refresh failed: {e}")

def generate_signal(current_price, open_price, high_price, low_price, progress, candle_history):
    """Generate signal based on price action"""
    try:
        # Check expiration
        if signal_data.get('use_expiration', False):
            timeframe = signal_data['timeframe']
            trade_exp = signal_data.get('trade_expiration', 60)
            remaining = timeframe - (progress / 100) * timeframe
            if remaining < trade_exp:
                return 'hold'
        
        # Early candle signal (first 10%)
        if progress < 10 and len(candle_history) >= 2:
            prev_close = candle_history[-2].get('close', current_price)
            if current_price > prev_close * 1.0005:
                return 'buy'
            elif current_price < prev_close * 0.9995:
                return 'sell'
        
        # Strong breakout signals (0.2% move)
        if high_price > open_price * 1.002 and current_price > open_price:
            return 'buy'
        elif low_price < open_price * 0.998 and current_price < open_price:
            return 'sell'
        
        # Previous candle breakout
        if len(candle_history) >= 3:
            prev_candle = candle_history[-2]
            if prev_candle:
                if (prev_candle.get('close', 0) < prev_candle.get('open', 0) and 
                    current_price > prev_candle.get('high', 0)):
                    return 'buy'
                if (prev_candle.get('close', 0) > prev_candle.get('open', 0) and 
                    current_price < prev_candle.get('low', 0)):
                    return 'sell'
        
        # Direction based on current vs open
        threshold = 0.0005
        if current_price > open_price * (1 + threshold):
            return 'buy'
        elif current_price < open_price * (1 - threshold):
            return 'sell'
        
        return 'hold'
        
    except Exception as e:
        logger.error(f"Signal generation error: {e}")
        return 'hold'

def fetch_current_price():
    """Fetch price using REST API only"""
    global trading_client, signal_data
    
    if not trading_client:
        return None
    
    asset = signal_data.get('asset', 'EURUSD_otc')
    
    try:
        # Get current price
        price = trading_client.get_current_price(asset)
        if price:
            return {'price': price, 'timestamp': time.time()}
        
        # Fallback to candles
        candles = trading_client.get_candles(asset, 5, 1)
        if candles and len(candles) > 0:
            latest = candles[-1]
            close_price = float(latest.get('close', 0))
            if close_price > 0:
                return {'price': close_price, 'timestamp': time.time()}
        
        return None
        
    except Exception as e:
        logger.error(f"Price fetch error: {e}")
        return None

# ==================== MAIN ====================
if __name__ == '__main__':
    print("\n" + "="*50)
    print("🚀 PocketOption Signal Bot - REST API Mode")
    print("="*50)
    print(f"Server: http://{HOST}:{PORT}")
    print("Mode: REST API (No WebSocket)")
    print("="*50 + "\n")
    
    app.run(host=HOST, port=PORT, debug=DEBUG_MODE, threaded=True)
