# PocketOption Signal Bot

A Flask web app that connects to PocketOption via WebSocket and generates real-time buy/sell trading signals based on live price data.

## Stack
- **Backend:** Python / Flask
- **Trading API:** BinaryOptionsToolsV2 (PocketOption async client)
- **Entry point:** `main.py` → imports `app` from `app.py`

## How to run
```
python main.py
```
The server starts on port 5000.

## Configuration
- **SSID** — your PocketOption session cookie. Enter it in the web UI after starting the bot; it is not required at startup.
- No other environment variables are needed.

## User preferences
- Keep the existing project structure and stack.
