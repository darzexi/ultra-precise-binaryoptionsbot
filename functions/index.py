import json
import time
import asyncio
import threading
import queue
import numpy as np
from flask import Flask, request, jsonify
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync
import logging
import os
import sys

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state
state = {
    'is_running': False,
    'current_signal': None,
    'price_data': None,
    'last_update': None,
    'manual_triggered': False,
    'candle_open': None,
    'candle_high': None,
    'candle_low': None,
    'candle_start_time': None,
    'candle_progress': 0,
    'candle_time_remaining': '--',
    'manual_mode': False,
    'timeframe': 60,
    'asset': 'EURUSD_otc',
    'update_rate': 0.5,
    'use_expiration': False,
    'trade_expiration': 60,
    'ssid': None,
    'websocket_mode': True,
    'candle_history': [],
    'client': None,
    'signal_thread': None,
    'signal_queue': queue.Queue(),
    'hotkey': 'space'
}

# Lock for thread safety
state_lock = threading.Lock()

def run_async(coro):
    """Run async coroutine in a new event loop"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

def get_client(ssid):
    """Get or create PocketOption client"""
    if state.get('client') is None and ssid:
        try:
            client = PocketOptionAsync(ssid=ssid)
            state['client'] = client
            return client
        except Exception as e:
            logger.error(f"Failed to create client: {e}")
            return None
    return state.get('client')

def fetch_price(client, asset):
    """Fetch current price using the client"""
    try:
        candles = run_async(client.get_candles(asset, 1, 1))
        if candles and len(candles) > 0:
            latest = candles[-1]
            return {
                'price': latest.get('close', 0),
                'open': latest.get('open', 0),
                'high': latest.get('high', 0),
                'low': latest.get('low', 0),
                'time': latest.get('time', time.time())
            }
    except Exception as e:
        logger.error(f"Error fetching price: {e}")
    return None

def get_candles(client, asset, timeframe, count=30):
    """Fetch candles from the API"""
    try:
        candles = run_async(client.get_candles(asset, timeframe, count))
        if candles and len(candles) > 0:
            return candles
    except Exception as e:
        logger.error(f"Error fetching candles: {e}")
    return None

def generate_signal_from_candle(current_price, open_price, high_price, low_price, progress, candle_history, is_manual=False):
    """Generate signal based on candle analysis"""
    try:
        if is_manual:
            if current_price > open_price:
                return 'buy'
            elif current_price < open_price:
                return 'sell'
            else:
                return 'buy' if int(time.time()) % 2 == 0 else 'sell'

        use_expiration = state.get('use_expiration', False)
        trade_expiration = state.get('trade_expiration', 60)
        timeframe = state.get('timeframe', 60)

        if use_expiration:
            elapsed = (progress / 100) * timeframe
            remaining = timeframe - elapsed
            if remaining < trade_expiration:
                return 'hold'

        if progress < 10 and len(candle_history) >= 2:
            prev_close = candle_history[-2].get('close', current_price)
            if current_price > prev_close:
                return 'buy'
            elif current_price < prev_close:
                return 'sell'

        if high_price > open_price * 1.002 and current_price > open_price:
            return 'buy'
        elif low_price < open_price * 0.998 and current_price < open_price:
            return 'sell'

        if len(candle_history) >= 3:
            last_candle = candle_history[-1]
            prev_candle = candle_history[-2]
            if last_candle and prev_candle:
                if (prev_candle.get('close', 0) < prev_candle.get('open', 0) and 
                    current_price > prev_candle.get('high', 0)):
                    return 'buy'
                if (prev_candle.get('close', 0) > prev_candle.get('open', 0) and 
                    current_price < prev_candle.get('low', 0)):
                    return 'sell'

        if current_price > open_price:
            return 'buy'
        elif current_price < open_price:
            return 'sell'
        else:
            return 'buy' if int(time.time()) % 2 == 0 else 'sell'
            
    except Exception as e:
        logger.error(f"Signal generation error: {e}")
        return 'buy' if current_price > open_price else 'sell'

def run_signal_bot():
    """Background thread for signal generation"""
    logger.info("Signal bot thread started")
    
    ssid = state.get('ssid')
    if not ssid:
        logger.error("No SSID provided")
        return
    
    client = get_client(ssid)
    if not client:
        logger.error("Failed to initialize client")
        return
    
    last_signal_time = 0
    candle_start_time = time.time()
    candle_open_price = None
    candle_high_price = None
    candle_low_price = None
    current_candle_data = []
    manual_triggered = False
    
    logger.info("Starting main signal loop")
    
    while state.get('is_running', False):
        try:
            current_time = time.time()
            
            manual_trigger_from_queue = False
            try:
                while not state['signal_queue'].empty():
                    queue_item = state['signal_queue'].get_nowait()
                    if queue_item.get('manual') and state.get('manual_mode', False):
                        manual_trigger_from_queue = True
                        logger.info("Manual trigger received from queue")
            except queue.Empty:
                pass
            
            if manual_trigger_from_queue:
                manual_triggered = True
                logger.info("Manual trigger flag set to True")
            
            should_update = False
            is_manual_update = False
            
            if state.get('manual_mode', False) and manual_triggered:
                should_update = True
                is_manual_update = True
                manual_triggered = False
                logger.info("Processing manual signal update")
            elif not state.get('manual_mode', False):
                if current_time - last_signal_time >= state.get('update_rate', 0.5):
                    should_update = True
                    is_manual_update = False
            
            if should_update:
                price_data = fetch_price(client, state['asset'])
                
                if price_data:
                    current_price = price_data.get('price', 0)
                    timestamp = price_data.get('time', current_time)
                    
                    if candle_open_price is None:
                        candle_open_price = price_data.get('open', current_price)
                        candle_high_price = price_data.get('high', current_price)
                        candle_low_price = price_data.get('low', current_price)
                        candle_start_time = timestamp
                        logger.info(f"Candle initialized at {candle_open_price}")
                    
                    if current_price > candle_high_price:
                        candle_high_price = current_price
                    if current_price < candle_low_price:
                        candle_low_price = current_price
                    
                    timeframe = state.get('timeframe', 60)
                    elapsed = current_time - candle_start_time
                    progress = min((elapsed / timeframe) * 100, 100)
                    
                    if elapsed >= timeframe:
                        candle_open_price = current_price
                        candle_high_price = current_price
                        candle_low_price = current_price
                        candle_start_time = current_time
                        progress = 0
                        
                        current_candle_data.append({
                            'open': candle_open_price,
                            'high': candle_high_price,
                            'low': candle_low_price,
                            'close': current_price,
                            'time': candle_start_time
                        })
                        
                        if len(current_candle_data) > 50:
                            current_candle_data.pop(0)
                        
                        logger.info(f"Candle completed at {current_price}")
                    
                    with state_lock:
                        state['candle_open'] = candle_open_price
                        state['candle_high'] = candle_high_price
                        state['candle_low'] = candle_low_price
                        state['candle_progress'] = progress
                        state['candle_time_remaining'] = f"{max(0, timeframe - elapsed):.1f}s"
                        state['candle_history'] = current_candle_data
                    
                    signal = generate_signal_from_candle(
                        current_price,
                        candle_open_price,
                        candle_high_price,
                        candle_low_price,
                        progress,
                        current_candle_data,
                        is_manual_update
                    )
                    
                    with state_lock:
                        state['current_signal'] = signal
                        state['price_data'] = current_price
                        state['last_update'] = time.strftime('%H:%M:%S.%f')[:-3]
                        if is_manual_update:
                            state['manual_triggered'] = True
                            logger.info(f"MANUAL signal set: {signal} at {current_price}")
                        else:
                            state['manual_triggered'] = False
                            
                last_signal_time = current_time
            
            time.sleep(0.05)
            
        except Exception as e:
            logger.error(f"Signal bot error: {e}")
            time.sleep(0.5)
    
    logger.info("Signal bot thread stopped")

# Flask Routes
@app.route('/start', methods=['POST'])
def start():
    global state
    
    with state_lock:
        if state.get('is_running', False):
            return jsonify({'success': False, 'error': 'Bot already running'})
        
        config = request.json
        ssid = config.get('ssid', '').strip()
        
        if not ssid:
            return jsonify({'success': False, 'error': 'SSID is required'})
        
        client = get_client(ssid)
        if not client:
            return jsonify({'success': False, 'error': 'Failed to connect to PocketOption'})
        
        state.update({
            'ssid': ssid,
            'asset': config.get('asset', 'EURUSD_otc'),
            'timeframe': int(config.get('timeframe', 60)),
            'update_rate': float(config.get('update_rate', 0.5)),
            'manual_mode': config.get('manual_mode', False),
            'websocket_mode': config.get('websocket_mode', True),
            'hotkey': config.get('hotkey', 'space'),
            'use_expiration': config.get('use_expiration', False),
            'trade_expiration': int(config.get('trade_expiration', 60)),
            'is_running': True,
            'client': client,
            'candle_open': None,
            'candle_high': None,
            'candle_low': None,
            'candle_start_time': None,
            'candle_history': []
        })
        
        while not state['signal_queue'].empty():
            try:
                state['signal_queue'].get_nowait()
            except:
                break
        
        signal_thread = threading.Thread(target=run_signal_bot, daemon=True)
        signal_thread.start()
        state['signal_thread'] = signal_thread
        
        logger.info("Bot started successfully")
        return jsonify({'success': True})

@app.route('/stop', methods=['POST'])
def stop():
    global state
    
    with state_lock:
        state['is_running'] = False
        state['client'] = None
    
    return jsonify({'success': True})

@app.route('/manual', methods=['POST'])
def manual():
    global state
    
    if not state.get('is_running', False):
        return jsonify({'success': False, 'error': 'Bot not running'})
    
    if not state.get('manual_mode', False):
        return jsonify({'success': False, 'error': 'Manual mode not enabled'})
    
    state['signal_queue'].put({'manual': True, 'timestamp': time.time()})
    logger.info(f"Manual signal queued")
    
    return jsonify({'success': True})

@app.route('/signal')
def get_signal():
    global state
    
    with state_lock:
        signal = state.get('current_signal')
        manual_triggered = state.get('manual_triggered', False)
        
        response = {
            'signal': signal,
            'price': state.get('price_data'),
            'timestamp': state.get('last_update'),
            'manual_triggered': manual_triggered,
            'use_expiration': state.get('use_expiration', False),
            'trade_expiration': state.get('trade_expiration', 60),
            'candle_data': {
                'progress': state.get('candle_progress', 0),
                'open': state.get('candle_open'),
                'high': state.get('candle_high'),
                'low': state.get('candle_low'),
                'current': state.get('price_data'),
                'time_remaining': state.get('candle_time_remaining', '--')
            }
        }
        
        if manual_triggered:
            state['manual_triggered'] = False
            
        return jsonify(response)

@app.route('/')
def serve_index():
    """Serve the main HTML page"""
    try:
        static_path = os.path.join(os.path.dirname(__file__), 'static', 'index.html')
        if os.path.exists(static_path):
            with open(static_path, 'r') as f:
                html_content = f.read()
            return html_content, 200, {'Content-Type': 'text/html'}
    except Exception as e:
        logger.error(f"Error reading static file: {e}")
    
    # Fallback HTML
    return """
    <!DOCTYPE html>
    <html>
    <head><title>PocketOption Signal Bot</title></head>
    <body>
        <h1>PocketOption Signal Bot</h1>
        <p>Please ensure static/index.html exists</p>
        <p>Current path: {}</p>
    </body>
    </html>
    """.format(os.path.dirname(__file__)), 200, {'Content-Type': 'text/html'}

# Netlify Functions Handler
def handler(event, context):
    """Main handler for Netlify Functions"""
    method = event.get('httpMethod', 'GET')
    path = event.get('path', '/')
    headers = event.get('headers', {})
    body = event.get('body', '')
    
    # Handle OPTIONS for CORS
    if method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type'
            },
            'body': ''
        }
    
    # Log the request
    logger.info(f"Request: {method} {path}")
    
    # Determine the actual Flask route path
    flask_path = path
    
    # Remove function prefix if present
    if path.startswith('/.netlify/functions/index'):
        flask_path = path[len('/.netlify/functions/index'):] or '/'
    elif path.startswith('/api/'):
        flask_path = path[4:] or '/'
    
    # If path is empty, use root
    if not flask_path:
        flask_path = '/'
    
    logger.info(f"Flask path: {flask_path}")
    
    # Create request context for Flask
    with app.test_request_context(
        path=flask_path,
        method=method,
        headers=headers,
        data=body if body else None
    ):
        try:
            # Dispatch the request to Flask
            response = app.full_dispatch_request()
            
            # Get response data
            response_data = response.get_data(as_text=True)
            status_code = response.status_code
            
            logger.info(f"Response status: {status_code}, content type: {response.content_type}")
            
            return {
                'statusCode': status_code,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': response.content_type or 'application/json'
                },
                'body': response_data
            }
        except Exception as e:
            logger.error(f"Handler error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'statusCode': 500,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({'error': str(e)})
            }