import asyncio
import json
import time
import threading
import queue
import os
import logging
import gc
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync
import numpy as np

# ==================== CONFIGURATION ====================
# Disable Flask development logging
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# Production settings
DEBUG_MODE = os.environ.get('DEBUG', 'False').lower() == 'true'
PORT = int(os.environ.get('PORT', 8080))
HOST = os.environ.get('HOST', '0.0.0.0')

# Initialize Flask app
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.debug = DEBUG_MODE
app.env = 'production' if not DEBUG_MODE else 'development'

# ==================== CORS HEADERS ====================
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
    'websocket_mode': True,
    'hotkey': 'space',
    'current_signal': None,
    'last_update': None,
    'price_data': None,
    'is_running': False,
    'ssid': None,
    'accuracy_mode': 'precise',
    'trade_expiration': 60,
    'use_expiration': False,
    'current_candle': None,
    'candle_progress': 0,
    'candle_high': None,
    'candle_low': None,
    'candle_open': None,
    'candle_start_time': None,
    'manual_triggered': False,
    'connection_status': 'disconnected',
    'last_price_fetch': None,
    'consecutive_failures': 0
}

trading_client = None
signal_thread = None
signal_queue = queue.Queue()
update_lock = threading.Lock()
event_loop = None

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.ERROR if not DEBUG_MODE else logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== HTML TEMPLATE ====================
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PocketOption Signal Bot - Production</title>
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
        .log-entry.manual-buy { color: #ff9800; font-weight: bold; background: rgba(255, 152, 0, 0.2); }
        .log-entry.manual-sell { color: #ff9800; font-weight: bold; background: rgba(255, 152, 0, 0.2); }
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
            .candle-info { flex-direction: column; align-items: center; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 PocketOption Signal Bot</h1>
        <p class="subtitle">Production-Ready Real-time Analysis</p>

        <div id="signalDisplay" class="signal-display">
            <div style="font-size: 14px; color: #888;">Current Signal</div>
            <div class="signal-text" id="signalText">WAITING</div>
            <div class="signal-price" id="signalPrice">Price: --</div>
            <div class="signal-time" id="signalTime">Last Update: --</div>
            <div class="accuracy-badge" id="accuracyBadge">🎯 Production Mode</div>
            
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
                    <option value="3">3s</option>
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
                    <label for="manualMode">Enable Manual Signal Update</label>
                </div>
            </div>
            <div class="setting-group">
                <label>WebSocket Mode</label>
                <div class="checkbox-group">
                    <input type="checkbox" id="websocketMode" checked>
                    <label for="websocketMode">Use Real-time Data</label>
                </div>
            </div>
            <div class="setting-group">
                <label for="hotkeyInput">Hotkey for Manual Signal</label>
                <input type="text" id="hotkeyInput" value="space" maxlength="20" placeholder="e.g., space, enter, s">
            </div>
        </div>

        <div class="expiration-input">
            <div class="checkbox-group">
                <input type="checkbox" id="useExpiration">
                <label for="useExpiration">Use Trade Expiration Time for Signal Decisions</label>
            </div>
            <div style="margin-top: 10px;">
                <label style="font-weight: 600; display: block; margin-bottom: 5px;">Trade Expiration (seconds):</label>
                <input type="number" id="tradeExpiration" value="60" min="3" max="300">
                <small style="color: #666; display: block; margin-top: 5px;">Minimum: 3 seconds</small>
            </div>
        </div>

        <div class="button-group">
            <button class="btn btn-success" id="startBtn">▶ Start Bot</button>
            <button class="btn btn-danger" id="stopBtn" disabled>⏹ Stop Bot</button>
            <button class="btn btn-warning" id="manualSignalBtn" disabled>⚡ Manual Signal</button>
            <button class="btn btn-info" id="testBtn">🔌 Test Connection</button>
            <button class="btn btn-secondary" id="clearLogsBtn">🗑 Clear Logs</button>
        </div>

        <div class="status-bar">
            <div class="status-item"><span>Status:</span><span id="statusText">Stopped</span></div>
            <div class="status-item"><span>Connection:</span><span id="connectionStatus" class="status-disconnected">Disconnected</span></div>
            <div class="status-item"><span>Hotkey:</span><span id="hotkeyDisplay"><span class="hotkey-indicator">space</span></span></div>
            <div class="status-item"><span>Mode:</span><span id="modeDisplay">Automatic</span></div>
            <div class="status-item"><span>Data Source:</span><span id="dataSourceDisplay">WebSocket</span></div>
            <div class="status-item"><span>SSID Status:</span><span id="ssidStatus">Not Set</span></div>
            <div class="status-item"><span>Signal Quality:</span><span id="signalQuality">Production</span></div>
            <div class="status-item"><span>Expiration Mode:</span><span id="expirationStatus">Disabled</span></div>
        </div>

        <div class="log-area" id="logArea">
            <div class="log-entry">[System] Bot initialized. Ready to start.</div>
            <div class="log-entry precise">[System] Production Mode Active - No Mock Data</div>
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
        const hotkeyDisplay = document.getElementById('hotkeyDisplay');
        const ssidStatus = document.getElementById('ssidStatus');
        const logArea = document.getElementById('logArea');
        const signalQuality = document.getElementById('signalQuality');
        const expirationStatus = document.getElementById('expirationStatus');

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
        const websocketMode = document.getElementById('websocketMode');
        const hotkeyInput = document.getElementById('hotkeyInput');
        const useExpiration = document.getElementById('useExpiration');
        const tradeExpiration = document.getElementById('tradeExpiration');

        hotkeyInput.addEventListener('input', function() {
            const key = this.value.trim() || 'space';
            hotkeyDisplay.innerHTML = `<span class="hotkey-indicator">${key}</span>`;
        });

        useExpiration.addEventListener('change', function() {
            tradeExpiration.disabled = !this.checked;
            if (this.checked) {
                expirationStatus.textContent = 'Enabled (' + tradeExpiration.value + 's)';
                expirationStatus.style.color = '#28a745';
                addLog('Trade expiration mode enabled - ' + tradeExpiration.value + 's', 'precise');
            } else {
                expirationStatus.textContent = 'Disabled';
                expirationStatus.style.color = '#888';
                addLog('Trade expiration mode disabled', 'info');
            }
        });

        tradeExpiration.addEventListener('input', function() {
            if (useExpiration.checked) {
                let val = parseInt(this.value) || 60;
                if (val < 3) val = 3;
                expirationStatus.textContent = 'Enabled (' + val + 's)';
                addLog('Trade expiration updated to ' + val + 's', 'info');
            }
        });

        // Test connection button
        testBtn.addEventListener('click', function() {
            const ssid = ssidInput.value.trim();
            if (!ssid) {
                addLog('ERROR: Please enter your SSID first!', 'error');
                return;
            }
            
            addLog('🔌 Testing connection...', 'connection');
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
                    ssidStatus.textContent = 'Valid ✓';
                    ssidStatus.style.color = '#28a745';
                    addLog('✅ Connection successful!', 'precise');
                } else {
                    connectionStatus.textContent = 'Failed ✗';
                    connectionStatus.className = 'status-disconnected';
                    ssidStatus.textContent = 'Invalid';
                    ssidStatus.style.color = '#dc3545';
                    addLog('❌ Connection failed: ' + data.error, 'error');
                }
            })
            .catch(err => {
                addLog('❌ Test error: ' + err.message, 'error');
            });
        });

        manualSignalBtn.addEventListener('click', function() {
            if (!isRunning) {
                addLog('Bot is not running!', 'error');
                return;
            }
            if (!manualMode.checked) {
                addLog('⚠️ Manual mode is not enabled!', 'error');
                return;
            }
            triggerManualSignal();
        });

        function triggerManualSignal() {
            const now = Date.now();
            if (now - lastManualTriggerTime < 500) {
                addLog('⏱️ Please wait...', 'manual');
                return;
            }
            lastManualTriggerTime = now;
            
            addLog('⚡ Triggering manual signal...', 'manual');
            
            fetch('/manual_signal', { 
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    const signal = data.signal;
                    const price = data.price || '--';
                    const timestamp = new Date().toLocaleTimeString();
                    
                    signalText.textContent = signal.toUpperCase();
                    signalPrice.textContent = `Price: ${price}`;
                    signalTime.textContent = `Last Update: ${timestamp}`;
                    
                    signalDisplay.className = 'signal-display manual';
                    modeDisplay.textContent = 'Manual (Triggered)';
                    modeDisplay.style.color = '#ff9800';
                    
                    addLog('✅ MANUAL ' + signal.toUpperCase() + ' at ' + price, 'manual-' + signal);
                } else {
                    addLog('❌ Manual signal failed: ' + data.error, 'error');
                }
            })
            .catch(err => {
                addLog('❌ Error: ' + err.message, 'error');
            });
        }

        startBtn.addEventListener('click', function() {
            const ssid = ssidInput.value.trim();
            if (!ssid) {
                addLog('ERROR: Please enter your SSID first!', 'error');
                ssidStatus.textContent = 'Missing!';
                ssidStatus.style.color = '#dc3545';
                return;
            }

            const config = {
                ssid: ssid,
                asset: assetSelect.value,
                timeframe: parseInt(timeframeSelect.value),
                update_rate: parseFloat(updateRate.value) || 0.5,
                manual_mode: manualMode.checked,
                websocket_mode: websocketMode.checked,
                hotkey: hotkeyInput.value.trim() || 'space',
                use_expiration: useExpiration.checked,
                trade_expiration: parseInt(tradeExpiration.value) || 60
            };

            fetch('/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    isRunning = true;
                    updateUI(true);
                    addLog('Bot started successfully', 'precise');
                    startPolling();
                    ssidStatus.textContent = 'Set ✓';
                    ssidStatus.style.color = '#28a745';
                    signalQuality.textContent = 'Production';
                    signalQuality.style.color = '#28a745';
                    if (useExpiration.checked) {
                        expirationStatus.textContent = 'Enabled (' + tradeExpiration.value + 's)';
                        expirationStatus.style.color = '#28a745';
                    }
                    if (manualMode.checked) {
                        addLog('🟡 Manual mode enabled', 'manual');
                        manualSignalBtn.disabled = false;
                    } else {
                        manualSignalBtn.disabled = true;
                    }
                } else {
                    addLog('Failed to start: ' + data.error, 'error');
                }
            })
            .catch(err => {
                addLog('Error: ' + err.message, 'error');
            });
        });

        stopBtn.addEventListener('click', function() {
            fetch('/stop', { method: 'POST' })
                .then(r => r.json())
                .then(data => {
                    if (data.success) {
                        isRunning = false;
                        updateUI(false);
                        stopPolling();
                        addLog('Bot stopped', 'info');
                        connectionStatus.textContent = 'Disconnected';
                        connectionStatus.className = 'status-disconnected';
                    }
                });
        });

        clearLogsBtn.addEventListener('click', function() {
            logArea.innerHTML = '';
            addLog('Logs cleared', 'info');
        });

        function updateUI(running) {
            startBtn.disabled = running;
            stopBtn.disabled = !running;
            if (running && manualMode.checked) {
                manualSignalBtn.disabled = false;
            } else {
                manualSignalBtn.disabled = true;
            }
            ssidInput.disabled = running;
            assetSelect.disabled = running;
            timeframeSelect.disabled = running;
            updateRate.disabled = running;
            manualMode.disabled = running;
            websocketMode.disabled = running;
            hotkeyInput.disabled = running;
            useExpiration.disabled = running;
            tradeExpiration.disabled = running || !useExpiration.checked;

            if (running) {
                statusText.textContent = 'Running';
                statusText.style.color = '#28a745';
                modeDisplay.textContent = manualMode.checked ? 'Manual' : 'Automatic';
                dataSourceDisplay.textContent = websocketMode.checked ? 'WebSocket' : 'Historical';
                hotkeyDisplay.innerHTML = `<span class="hotkey-indicator">${hotkeyInput.value.trim() || 'space'}</span>`;
            } else {
                statusText.textContent = 'Stopped';
                statusText.style.color = '#dc3545';
                signalDisplay.className = 'signal-display';
                signalText.textContent = 'WAITING';
                signalPrice.textContent = 'Price: --';
                signalTime.textContent = 'Last Update: --';
                signalQuality.textContent = 'Production';
                signalQuality.style.color = '#28a745';
                
                candleProgressFill.style.width = '0%';
                candleProgressText.textContent = '0%';
                candleOpen.textContent = '--';
                candleHigh.textContent = '--';
                candleLow.textContent = '--';
                candleCurrent.textContent = '--';
                candleTimeRemaining.textContent = '--';
            }
        }

        manualMode.addEventListener('change', function() {
            if (isRunning) {
                if (this.checked) {
                    manualSignalBtn.disabled = false;
                    addLog('🟡 Manual mode enabled', 'manual');
                } else {
                    manualSignalBtn.disabled = true;
                    addLog('Manual mode disabled', 'info');
                }
                modeDisplay.textContent = this.checked ? 'Manual' : 'Automatic';
            }
        });

        function startPolling() {
            if (updateInterval) clearInterval(updateInterval);
            updateInterval = setInterval(() => {
                fetch('/get_signal')
                    .then(r => r.json())
                    .then(data => {
                        if (data.signal && data.signal !== 'pending') {
                            if (!data.manual_triggered) {
                                updateSignalDisplay(data);
                            }
                        }
                        if (data.candle_data) {
                            updateCandleDisplay(data.candle_data);
                        }
                        if (data.connection_status) {
                            updateConnectionStatus(data.connection_status);
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

        function updateSignalDisplay(data) {
            const signal = data.signal;
            const price = data.price || '--';
            const timestamp = data.timestamp || new Date().toLocaleTimeString();

            if (!data.manual_triggered) {
                signalText.textContent = signal.toUpperCase();
                signalPrice.textContent = `Price: ${price}`;
                signalTime.textContent = `Last Update: ${timestamp}`;

                signalDisplay.className = 'signal-display';
                if (signal === 'buy') {
                    signalDisplay.classList.add('buy');
                } else if (signal === 'sell') {
                    signalDisplay.classList.add('sell');
                }
                
                modeDisplay.textContent = manualMode.checked ? 'Manual (Waiting)' : 'Automatic';
                modeDisplay.style.color = '#333';
            }
            
            if (data.use_expiration) {
                expirationStatus.textContent = 'Enabled (' + data.trade_expiration + 's)';
                expirationStatus.style.color = '#28a745';
            }
        }

        function updateCandleDisplay(candleData) {
            if (!candleData) return;
            
            const progress = candleData.progress || 0;
            const open = candleData.open || '--';
            const high = candleData.high || '--';
            const low = candleData.low || '--';
            const current = candleData.current || '--';
            const timeRemaining = candleData.time_remaining || '--';
            
            candleProgressFill.style.width = progress + '%';
            candleProgressText.textContent = Math.round(progress) + '%';
            candleOpen.textContent = typeof open === 'number' ? open.toFixed(5) : open;
            candleHigh.textContent = typeof high === 'number' ? high.toFixed(5) : high;
            candleLow.textContent = typeof low === 'number' ? low.toFixed(5) : low;
            candleCurrent.textContent = typeof current === 'number' ? current.toFixed(5) : current;
            candleTimeRemaining.textContent = timeRemaining;
        }

        function updateConnectionStatus(status) {
            if (status === 'connected') {
                connectionStatus.textContent = 'Connected ✓';
                connectionStatus.className = 'status-connected';
            } else {
                connectionStatus.textContent = 'Disconnected ✗';
                connectionStatus.className = 'status-disconnected';
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

        document.addEventListener('keydown', function(e) {
            if (!isRunning) return;
            if (!manualMode.checked) return;
            const hotkey = hotkeyInput.value.trim() || 'space';
            const key = e.key.toLowerCase();

            if (key === hotkey.toLowerCase()) {
                e.preventDefault();
                addLog('⌨️ Hotkey pressed: ' + hotkey, 'manual');
                triggerManualSignal();
            }
        });

        updateUI(false);
        addLog('System ready. Enter your SSID and click Start.', 'info');
        addLog('🚀 Production Mode - No Mock Data', 'precise');
        addLog('Hotkey: space (change in settings)', 'info');
        addLog('Minimum trade expiration: 3 seconds', 'info');
        ssidStatus.textContent = 'Not Set';
        ssidStatus.style.color = '#dc3545';
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
    """Test PocketOption connection with SSID"""
    try:
        data = request.json
        ssid = data.get('ssid', '').strip()
        
        if not ssid:
            return jsonify({'success': False, 'error': 'SSID required'})
        
        # Quick test connection
        test_client = PocketOptionAsync(ssid=ssid)
        
        # Try to fetch a price
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            price = loop.run_until_complete(test_client.get_current_price('EURUSD_otc'))
            loop.close()
            
            if price:
                return jsonify({'success': True, 'price': price})
            else:
                return jsonify({'success': False, 'error': 'Could not fetch price'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/start', methods=['POST'])
def start_bot():
    global signal_thread, signal_data, trading_client, event_loop
    
    with update_lock:
        if signal_data['is_running']:
            return jsonify({'success': False, 'error': 'Bot already running'})
        
        config = request.json
        ssid = config.get('ssid', '').strip()
        
        if not ssid:
            return jsonify({'success': False, 'error': 'SSID is required'})
        
        if len(ssid) < 10:
            return jsonify({'success': False, 'error': 'Invalid SSID format'})
        
        trade_exp = int(config.get('trade_expiration', 60))
        if trade_exp < 3:
            trade_exp = 3
        
        signal_data.update({
            'ssid': ssid,
            'asset': config.get('asset', 'EURUSD_otc'),
            'timeframe': int(config.get('timeframe', 60)),
            'update_rate': float(config.get('update_rate', 0.5)),
            'manual_mode': config.get('manual_mode', False),
            'websocket_mode': config.get('websocket_mode', True),
            'hotkey': config.get('hotkey', 'space'),
            'is_running': True,
            'current_signal': None,
            'last_update': None,
            'accuracy_mode': 'precise',
            'use_expiration': config.get('use_expiration', False),
            'trade_expiration': trade_exp,
            'current_candle': None,
            'candle_progress': 0,
            'candle_high': None,
            'candle_low': None,
            'candle_open': None,
            'candle_start_time': None,
            'manual_triggered': False,
            'consecutive_failures': 0
        })
        
        # Initialize client
        try:
            trading_client = PocketOptionAsync(ssid=ssid)
            signal_data['connection_status'] = 'connected'
            logger.info("PocketOption client initialized successfully")
        except Exception as e:
            signal_data['is_running'] = False
            signal_data['connection_status'] = 'disconnected'
            return jsonify({'success': False, 'error': f'Failed to connect: {str(e)}'})
        
        # Clear queue
        while not signal_queue.empty():
            try:
                signal_queue.get_nowait()
            except:
                break
        
        signal_thread = threading.Thread(target=run_signal_bot, daemon=True)
        signal_thread.start()
        
        return jsonify({'success': True})

@app.route('/stop', methods=['POST'])
def stop_bot():
    global signal_data, trading_client
    
    with update_lock:
        signal_data['is_running'] = False
        signal_data['connection_status'] = 'disconnected'
        
        if trading_client:
            try:
                trading_client = None
            except:
                pass
        
        return jsonify({'success': True})

@app.route('/manual_signal', methods=['POST', 'OPTIONS'])
def manual_signal():
    global signal_data
    
    if request.method == 'OPTIONS':
        return jsonify({'success': True})
    
    if not signal_data['is_running']:
        return jsonify({'success': False, 'error': 'Bot not running'})
    
    if not signal_data['manual_mode']:
        return jsonify({'success': False, 'error': 'Manual mode not enabled'})
    
    # Get REAL current price (NO MOCK!)
    current_price = None
    
    # Try to get real price
    price_data = fetch_current_price()
    if price_data:
        current_price = price_data.get('price')
    
    if current_price is None:
        return jsonify({'success': False, 'error': 'Could not fetch real price data'})
    
    # Get candle data
    open_price = signal_data.get('candle_open')
    if open_price is None:
        open_price = current_price
    
    # Generate signal from REAL data
    if current_price > open_price:
        signal = 'buy'
    elif current_price < open_price:
        signal = 'sell'
    else:
        signal = 'buy' if current_price > open_price * 0.999 else 'sell'
    
    # Update signal data
    with update_lock:
        signal_data['current_signal'] = signal
        signal_data['price_data'] = current_price
        signal_data['last_update'] = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        signal_data['manual_triggered'] = True
    
    logger.info(f"Manual signal generated: {signal} at {current_price}")
    
    return jsonify({'success': True, 'signal': signal, 'price': current_price})

@app.route('/get_signal')
def get_signal():
    global signal_data
    
    with update_lock:
        signal = signal_data.get('current_signal')
        manual_triggered = signal_data.get('manual_triggered', False)
        
        response = {
            'signal': signal,
            'price': signal_data.get('price_data'),
            'timestamp': signal_data.get('last_update'),
            'manual_triggered': manual_triggered,
            'use_expiration': signal_data.get('use_expiration', False),
            'trade_expiration': signal_data.get('trade_expiration', 60),
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
        
        if manual_triggered:
            signal_data['manual_triggered'] = False
            
        return jsonify(response)

# ==================== CORE BOT LOGIC ====================
def run_signal_bot():
    global signal_data, trading_client, signal_queue
    
    logger.info("Signal bot thread started")
    
    ssid = signal_data.get('ssid')
    if not ssid:
        logger.error("No SSID provided")
        signal_data['is_running'] = False
        return
    
    last_signal_time = 0
    candle_start_time = time.time()
    candle_open_price = None
    candle_high_price = None
    candle_low_price = None
    current_candle_data = []
    consecutive_failures = 0
    last_successful_fetch = time.time()
    
    logger.info("Starting main signal loop - Production Mode (No Mock Data)")
    
    while signal_data['is_running']:
        try:
            current_time = time.time()
            
            # Only update at the specified rate
            if current_time - last_signal_time < signal_data['update_rate']:
                time.sleep(0.05)
                continue
            
            # Get REAL price data (NO MOCK!)
            price_data = fetch_current_price()
            
            if price_data is None:
                consecutive_failures += 1
                signal_data['consecutive_failures'] = consecutive_failures
                signal_data['connection_status'] = 'disconnected'
                
                if consecutive_failures > 10:
                    logger.error("Too many consecutive failures, attempting to reconnect...")
                    try:
                        if trading_client:
                            trading_client = PocketOptionAsync(ssid=signal_data.get('ssid'))
                            signal_data['connection_status'] = 'connected'
                            consecutive_failures = 0
                            logger.info("Reconnected successfully")
                    except Exception as e:
                        logger.error(f"Reconnection failed: {e}")
                
                time.sleep(1)
                continue
            
            # Reset failures on success
            consecutive_failures = 0
            signal_data['consecutive_failures'] = 0
            signal_data['connection_status'] = 'connected'
            last_successful_fetch = current_time
            
            current_price = price_data.get('price', 0)
            timestamp = price_data.get('timestamp', current_time)
            
            # Initialize candle data if needed
            if candle_open_price is None:
                candle_open_price = current_price
                candle_high_price = current_price
                candle_low_price = current_price
                candle_start_time = timestamp
                logger.info(f"New candle started at {current_price}")
            
            # Update candle extremes
            if current_price > candle_high_price:
                candle_high_price = current_price
            if current_price < candle_low_price:
                candle_low_price = current_price
            
            # Calculate progress
            timeframe = signal_data['timeframe']
            elapsed = current_time - candle_start_time
            progress = min((elapsed / timeframe) * 100, 100)
            
            # Check for candle completion
            if elapsed >= timeframe:
                # Save completed candle
                current_candle_data.append({
                    'open': candle_open_price,
                    'high': candle_high_price,
                    'low': candle_low_price,
                    'close': current_price,
                    'time': candle_start_time
                })
                
                # Keep only last 50 candles
                if len(current_candle_data) > 50:
                    current_candle_data.pop(0)
                
                logger.info(f"Candle completed: Open={candle_open_price:.5f}, High={candle_high_price:.5f}, Low={candle_low_price:.5f}, Close={current_price:.5f}")
                
                # Reset for new candle
                candle_open_price = current_price
                candle_high_price = current_price
                candle_low_price = current_price
                candle_start_time = current_time
                progress = 0
            
            # Update candle display data
            with update_lock:
                signal_data['candle_open'] = candle_open_price
                signal_data['candle_high'] = candle_high_price
                signal_data['candle_low'] = candle_low_price
                signal_data['candle_progress'] = progress
                signal_data['candle_time_remaining'] = f"{max(0, timeframe - elapsed):.1f}s"
                signal_data['price_data'] = current_price
                signal_data['last_price_fetch'] = datetime.now().isoformat()
            
            # Generate signal from REAL data
            signal = generate_signal_from_candle(
                current_price,
                candle_open_price,
                candle_high_price,
                candle_low_price,
                progress,
                current_candle_data
            )
            
            # Update signal
            with update_lock:
                signal_data['current_signal'] = signal.get('signal')
                signal_data['price_data'] = current_price
                signal_data['last_update'] = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            
            # Log signal changes
            if signal.get('signal') in ['buy', 'sell']:
                logger.info(f"Signal: {signal.get('signal')} at {current_price:.5f}")
            
            last_signal_time = current_time
            
            # Force garbage collection periodically
            if int(current_time) % 60 == 0:
                gc.collect()
            
        except Exception as e:
            logger.error(f"Signal bot error: {e}")
            time.sleep(1)
    
    logger.info("Signal bot thread stopped")

def generate_signal_from_candle(current_price, open_price, high_price, low_price, progress, candle_history):
    global signal_data
    
    try:
        use_expiration = signal_data.get('use_expiration', False)
        trade_expiration = signal_data.get('trade_expiration', 60)
        timeframe = signal_data['timeframe']
        
        # Check expiration
        if use_expiration:
            elapsed = (progress / 100) * timeframe
            remaining = timeframe - elapsed
            
            if remaining < trade_expiration:
                return {'signal': 'hold', 'price': current_price}
        
        # Early candle signal (first 10%)
        if progress < 10:
            if len(candle_history) >= 2:
                prev_close = candle_history[-2].get('close', current_price)
                if current_price > prev_close * 1.0001:
                    return {'signal': 'buy', 'price': current_price}
                elif current_price < prev_close * 0.9999:
                    return {'signal': 'sell', 'price': current_price}
        
        # Strong breakout signals (0.2% move)
        if high_price > open_price * 1.002 and current_price > open_price:
            return {'signal': 'buy', 'price': current_price}
        elif low_price < open_price * 0.998 and current_price < open_price:
            return {'signal': 'sell', 'price': current_price}
        
        # Previous candle breakout
        if len(candle_history) >= 3:
            prev_candle = candle_history[-2]
            
            if prev_candle:
                # Bullish breakout
                if (prev_candle.get('close', 0) < prev_candle.get('open', 0) and 
                    current_price > prev_candle.get('high', 0)):
                    return {'signal': 'buy', 'price': current_price}
                
                # Bearish breakout
                if (prev_candle.get('close', 0) > prev_candle.get('open', 0) and 
                    current_price < prev_candle.get('low', 0)):
                    return {'signal': 'sell', 'price': current_price}
        
        # Direction based on current vs open
        if current_price > open_price * 1.0005:
            return {'signal': 'buy', 'price': current_price}
        elif current_price < open_price * 0.9995:
            return {'signal': 'sell', 'price': current_price}
        
        # Neutral - hold
        return {'signal': 'hold', 'price': current_price}
            
    except Exception as e:
        logger.error(f"Signal generation error: {e}")
        if current_price > open_price:
            return {'signal': 'buy', 'price': current_price}
        else:
            return {'signal': 'sell', 'price': current_price}

def fetch_current_price():
    """Fetch real price data ONLY - NO MOCK DATA"""
    global trading_client, signal_data
    
    if trading_client is None:
        logger.error("Trading client not initialized")
        return None
    
    try:
        asset = signal_data.get('asset', 'EURUSD_otc')
        
        # Try get_current_price first
        try:
            if hasattr(trading_client, 'get_current_price'):
                price = asyncio.run(trading_client.get_current_price(asset))
                if price and float(price) > 0:
                    return {'price': float(price), 'timestamp': time.time()}
        except Exception as e:
            logger.debug(f"get_current_price failed: {e}")
        
        # Try get_candles
        try:
            if hasattr(trading_client, 'get_candles'):
                candles = asyncio.run(trading_client.get_candles(asset, 1, 1))
                if candles and len(candles) > 0:
                    latest = candles[-1]
                    close_price = float(latest.get('close', 0))
                    if close_price > 0:
                        return {
                            'price': close_price,
                            'timestamp': float(latest.get('time', time.time()))
                        }
        except Exception as e:
            logger.debug(f"get_candles failed: {e}")
        
        # Try history
        try:
            if hasattr(trading_client, 'history'):
                history = asyncio.run(trading_client.history(asset, 1))
                if history and len(history) > 0:
                    latest = history[-1]
                    close_price = float(latest.get('close', 0))
                    if close_price > 0:
                        return {
                            'price': close_price,
                            'timestamp': float(latest.get('time', time.time()))
                        }
        except Exception as e:
            logger.debug(f"history failed: {e}")
        
        # All methods failed
        logger.error("All price fetch methods failed")
        return None
        
    except Exception as e:
        logger.error(f"Price fetch error: {e}")
        return None

# ==================== PRODUCTION SERVER ====================
if __name__ == '__main__':
    # Production settings
    if not DEBUG_MODE:
        print("\n" + "="*50)
        print("🚀 PocketOption Signal Bot - PRODUCTION MODE")
        print("="*50)
        print(f"Server: http://{HOST}:{PORT}")
        print("Status: Running in Production Mode")
        print("Note: No mock data will be used")
        print("="*50 + "\n")
    
    app.run(
        host=HOST,
        port=PORT,
        debug=DEBUG_MODE,
        threaded=True,
        use_reloader=False  # Disable reloader for production
    )
