import websocket
import json
import threading
import time
from datetime import datetime, timedelta
from collections import defaultdict
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PSXWebSocketMonitor:
    """Real-time stock monitor using PSX WebSocket API"""
    
    def __init__(self):
        # WebSocket connection
        self.ws_url = "wss://psxterminal.com/"
        self.ws = None
        self.connected = False
        
        # Headers for REST API
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json'
        }
        
        # Data storage
        self.price_cache = {}  # {symbol: {price, timestamp, ...}}
        self.price_history = defaultdict(list) # {symbol: [(timestamp, price), ...]}
        self.alerts = {}       # {symbol: [alert1, alert2, ...]}
        self.last_update = {}
        
        # Statistics
        self.stats = {
            'total_updates': 0,
            'alerts_triggered': 0,
            'websocket_errors': 0,
            'connected_at': None
        }
        
        # Reconnection settings
        self.reconnect_attempts = 0
        self.max_reconnect = 10
        
        # Initial data fetch
        self._fetch_initial_data()
        
        # Start in background thread
        self.start()
    
    def start(self):
        """Start WebSocket connection in background thread"""
        logger.info("Starting WebSocket monitor...")
        self.ws_thread = threading.Thread(target=self._connect_websocket, daemon=True)
        self.ws_thread.start()
        
        # Start heartbeat monitor
        self.heartbeat_thread = threading.Thread(target=self._monitor_connection, daemon=True)
        self.heartbeat_thread.start()
        
        # Periodic REST refresh (every 5 minutes as fallback)
        self.refresh_thread = threading.Thread(target=self._periodic_refresh, daemon=True)
        self.refresh_thread.start()
    
    def _fetch_initial_data(self):
        """Fetch initial symbol list and start background warmer"""
        try:
            logger.info("Fetching symbol list from PSX Terminal...")
            
            # 1. Fetch all symbols
            resp = requests.get("https://psxterminal.com/api/symbols", headers=self.headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('success'):
                    symbols = data.get('data', [])
                    logger.info(f"✅ Found {len(symbols)} symbols. Starting background cache warmer...")
                    
                    # Initialize cache with empty values
                    for sym in symbols:
                        if sym not in self.price_cache:
                            self.price_cache[sym] = {'price': None, 'symbol': sym}

                    # Start background warmer thread
                    self.warmer_thread = threading.Thread(target=self._warm_cache, args=(symbols,), daemon=True)
                    self.warmer_thread.start()
            
            # 2. Also fetch stats/REG as a quick seed for top movers (immediate data)
            stats_resp = requests.get("https://psxterminal.com/api/stats/REG", headers=self.headers, timeout=10)
            if stats_resp.status_code == 200:
                stats_data = stats_resp.json()
                if stats_data.get('success'):
                    reg_data = stats_data.get('data', {})
                    for category in ['topGainers', 'topLosers', 'topVolume']:
                        for stock in reg_data.get(category, []):
                            sym = stock.get('symbol')
                            if sym:
                                self.price_cache[sym] = {
                                    'price': stock.get('price'),
                                    'change': stock.get('change', 0),
                                    'change_percent': stock.get('changePercent', 0) * 100,
                                    'volume': stock.get('volume', 0),
                                    'value': stock.get('value', 0),
                                    'updated_at': datetime.now().isoformat()
                                }
                                
        except Exception as e:
            logger.error(f"Sync error: {e}")

    def _warm_cache(self, symbols):
        """Background thread to fetch prices for ALL symbols one by one"""
        logger.info(f"🔥 Cache warmer started for {len(symbols)} symbols...")
        count = 0
        
        for sym in symbols:
            # Skip if we already have a price (e.g. from stats/REG or live socket)
            if self.price_cache.get(sym, {}).get('price'):
                continue
                
            try:
                # Fetch single symbol price
                # Limit: 100 req/min = 1.66 req/sec = 0.6s delay
                time.sleep(0.65) 
                
                resp = requests.get(f"https://psxterminal.com/api/ticks/REG/{sym}", headers=self.headers, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('success'):
                        tick = data.get('data', {})
                        price = tick.get('price')
                        if price:
                            self.price_cache[sym] = {
                                'price': price,
                                'change': tick.get('change', 0),
                                'change_percent': tick.get('changePercent', 0) * 100,
                                'volume': tick.get('volume', 0),
                                'value': tick.get('value', 0),
                                'updated_at': datetime.now().isoformat()
                            }
                            count += 1
            except Exception:
                pass # Silent fail to keep moving
            
            # periodic log
            if (symbols.index(sym) + 1) % 10 == 0:
                 logger.info(f"🔥 Warming cache: Processed {symbols.index(sym) + 1}/{len(symbols)} symbols ({count} prices found)")
        
        logger.info(f"✅ Cache warmer finished. Total prices found: {count}")

    def _periodic_refresh(self):
        """Fallback to keep data updated if WebSocket is silent"""
        while True:
            time.sleep(300)  # Every 5 minutes
            self._fetch_initial_data()
    
    def _connect_websocket(self):
        """Connect to PSX WebSocket"""
        while self.reconnect_attempts < self.max_reconnect:
            try:
                logger.info(f"Connecting to {self.ws_url}...")
                
                # Configure WebSocket
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                
                # Run forever (with reconnect)
                self.ws.run_forever(
                    ping_interval=30,
                    ping_timeout=10,
                    reconnect=5  # Auto-reconnect
                )
                
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                self.reconnect_attempts += 1
                time.sleep(min(30, 2 ** self.reconnect_attempts))  # Exponential backoff
    
    def _on_open(self, ws):
        """WebSocket connection opened"""
        logger.info("WebSocket connected to PSX Terminal")
        self.connected = True
        self.reconnect_attempts = 0
        self.stats['connected_at'] = datetime.now()
        
        # Subscribe to common market data streams
        # We'll send separate subscriptions for REG and IDX to be safe
        for m_type in ['REG', 'IDX']:
            subscribe_message = {
                "type": "subscribe",
                "subscriptionType": "marketData",
                "params": {
                    "marketType": m_type
                },
                "requestId": f"sub-{m_type}-{int(time.time())}"
            }
            
            try:
                ws.send(json.dumps(subscribe_message))
                logger.info(f"Subscribed to {m_type} market symbols")
            except Exception as e:
                logger.error(f"Failed to subscribe to {m_type}: {e}")
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            msg_type = data.get('type')
            
            # Handle different message types
            if msg_type == 'welcome':
                logger.info(f"Server: {data.get('message')}")
                
            elif msg_type == 'tickUpdate':
                self._process_tick_update(data)
                
            elif msg_type == 'ping':
                # Respond to keep connection alive
                pong_response = {
                    "type": "pong",
                    "timestamp": data.get('timestamp')
                }
                ws.send(json.dumps(pong_response))
                
            elif msg_type == 'error':
                logger.error(f"WebSocket error: {data.get('message')}")
                self.stats['websocket_errors'] += 1
                
            self.stats['total_updates'] += 1
            
            # Log every 100 messages
            if self.stats['total_updates'] % 100 == 0:
                logger.info(f"📡 Updates: {self.stats['total_updates']}, Symbols: {len(self.price_cache)}")
                
        except json.JSONDecodeError:
            logger.error(f"Failed to parse message: {message[:100]}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _process_tick_update(self, data):
        """Process real-time price update"""
        tick = data.get('tick', {})
        # Documentation shows symbol at top level, but tick has 's'
        symbol = data.get('symbol') or tick.get('s')
        price = tick.get('c')  # Current price
        
        if not symbol and 'data' in data: # Some variants use data: { symbol: ... }
             symbol = data.get('data', {}).get('symbol') or data.get('data', {}).get('s')
             price = price or data.get('data', {}).get('c')
        
        if symbol and price:
            # Normalize change percent (API often sends decimals like 0.019 for 1.9%)
            raw_pch = tick.get('pch', 0)
            if -1.0 < raw_pch < 1.0 and raw_pch != 0:
                # Likely decimal, convert to percentage
                raw_pch = raw_pch * 100
                
            # Update cache
            self.price_cache[symbol] = {
                'price': price,
                'change': tick.get('ch', 0),
                'change_percent': raw_pch,
                'volume': tick.get('v', 0),
                'trades': tick.get('tr', 0),
                'value': tick.get('val', 0),
                'high': tick.get('h', price),
                'low': tick.get('l', price),
                'timestamp': data.get('timestamp', int(time.time() * 1000)),
                'updated_at': datetime.now().isoformat(),
                'market_status': tick.get('st', 'UNKNOWN')
            }
            
            now = time.time()
            self.last_update[symbol] = now
            
            # Update price history for rolling window (20 minutes = 1200 seconds)
            self.price_history[symbol].append((now, price))
            # Clean up history older than 20 minutes
            self.price_history[symbol] = [p for p in self.price_history[symbol] if now - p[0] <= 1200]
            
            # Check alerts for this symbol
            self._check_alerts(symbol, price)
    
    def _check_alerts(self, symbol, current_price):
        """Check if price triggers any alerts (logic: 4% up in 20 mins AND value >= 2,000,000)"""
        if symbol not in self.alerts:
            return
            
        # 1. Traded Value condition (current_price * volume >= 2,000,000)
        stock_data = self.price_cache.get(symbol, {})
        volume = stock_data.get('volume', 0)
        traded_value = current_price * volume
        
        # Only proceed if stock has at least 2M in traded value
        if traded_value < 2000000:
            return
            
        # 2. Timeframe Condition: 4% gain in 20 minute rolling window
        history = self.price_history.get(symbol, [])
        if not history:
            return
            
        # Get the oldest price within the last 20 minutes
        old_price = history[0][1]
        
        if old_price <= 0:
            return
            
        price_gain = ((current_price - old_price) / old_price) * 100
        
        # Check if it meets the 4% rise criteria (Upward only)
        if price_gain >= 4.0:
            for alert in self.alerts[symbol]:
                if not alert.get('active', True):
                    continue
                
                # Signal alert trigger
                self._trigger_alert(alert, current_price, price_gain, 'up')
    
    def _trigger_alert(self, alert, current_price, change_percent, direction):
        """Trigger an alert - notification handled by app.py via SocketIO"""
        symbol = alert.get('symbol')
        
        logger.info(f"🚨 ALERT: {symbol} {change_percent:+.2f}% (Rs. {current_price:.2f})")
        
        # Update alert status
        alert['active'] = False
        alert['triggered_at'] = datetime.now().isoformat()
        alert['trigger_price'] = current_price
        alert['trigger_percent'] = change_percent
        
        self.stats['alerts_triggered'] += 1
    

    
    def _on_error(self, ws, error):
        """WebSocket error"""
        logger.error(f"WebSocket error: {error}")
        self.connected = False
        self.stats['websocket_errors'] += 1
    
    def _on_close(self, ws, close_status_code, close_msg):
        """WebSocket closed"""
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.connected = False
        
        # Try to reconnect
        if self.reconnect_attempts < self.max_reconnect:
            time.sleep(5)
            self._connect_websocket()
    
    def _monitor_connection(self):
        """Monitor connection and restart if needed"""
        while True:
            time.sleep(30)
            
            # Check if connection is alive
            if not self.connected:
                logger.warning("WebSocket disconnected, attempting to reconnect...")
                self._connect_websocket()
    
    # ===== PUBLIC API METHODS =====
    
    def add_alert(self, symbol, phone, base_price, threshold=5.0, direction='both'):
        """Add a new price alert"""
        symbol = symbol.upper()
        
        if symbol not in self.alerts:
            self.alerts[symbol] = []
        
        alert = {
            'id': f"{symbol}-{int(time.time())}",
            'symbol': symbol,
            'phone': phone,
            'base_price': float(base_price),
            'threshold': float(threshold),
            'direction': direction,
            'active': True,
            'created_at': datetime.now().isoformat(),
            'last_checked': datetime.now().isoformat()
        }
        
        self.alerts[symbol].append(alert)
        logger.info(f"Alert added: {symbol} at Rs. {base_price} (±{threshold}%) for {phone}")
        
        return alert
    
    def get_price(self, symbol):
        """Get current price from cache, fetch from REST if missing with fallback"""
        symbol = symbol.upper()
        data = self.price_cache.get(symbol)
        
        # If we have the symbol but no price, try one-time fetch
        if not data or data.get('price') is None:
            try:
                # Fallback 1: Market Ticks API (Fastest)
                logger.info(f"Price for {symbol} not in cache, trying Ticks API...")
                for m_type in ['REG', 'IDX']:
                    try:
                        resp = requests.get(f"https://psxterminal.com/api/ticks/{m_type}/{symbol}", 
                                          headers=self.headers, timeout=5)
                        if resp.status_code == 200:
                            res_data = resp.json()
                            if res_data.get('success'):
                                tick_data = res_data.get('data', {})
                                price = tick_data.get('price')
                                if price:
                                    pch = tick_data.get('changePercent', 0)
                                    if -1.0 < pch < 1.0 and pch != 0: pch *= 100
                                    self.price_cache[symbol] = {
                                        'price': price,
                                        'change_percent': pch,
                                        'symbol': symbol,
                                        'updated_at': datetime.now().isoformat()
                                    }
                                    return self.price_cache[symbol]
                        else:
                            logger.warning(f"Ticks API {m_type} for {symbol} failed with status {resp.status_code}")
                    except Exception as e:
                        logger.error(f"Ticks error for {symbol}: {e}")
                
                # Fallback 2: Fundamentals API
                try:
                    resp = requests.get(f"https://psxterminal.com/api/fundamentals/{symbol}", 
                                      headers=self.headers, timeout=5)
                    if resp.status_code == 200:
                        res_data = resp.json()
                        if res_data.get('success'):
                            f_data = res_data.get('data', {})
                            price = f_data.get('price')
                            if price:
                                pch = f_data.get('changePercent', 0)
                                if -1.0 < pch < 1.0 and pch != 0: pch *= 100
                                self.price_cache[symbol] = {
                                    'price': price,
                                    'change_percent': pch,
                                    'symbol': symbol,
                                    'updated_at': datetime.now().isoformat()
                                }
                                return self.price_cache[symbol]
                except Exception as e:
                    logger.error(f"Fundamentals error for {symbol}: {e}")
                            
                logger.warning(f"All REST fallbacks failed for {symbol}")
            except Exception as e:
                logger.error(f"Error fetching manual price for {symbol}: {e}")
                
        return data
    
    def get_all_prices(self):
        """Get all cached prices"""
        return self.price_cache
    
    def get_alerts(self, symbol=None):
        """Get alerts for a symbol or all alerts"""
        if symbol:
            return self.alerts.get(symbol.upper(), [])
        return self.alerts
    
    def deactivate_alert(self, alert_id):
        """Deactivate an alert by ID"""
        for symbol, alerts in self.alerts.items():
            for alert in alerts:
                if alert.get('id') == alert_id:
                    alert['active'] = False
                    logger.info(f"Alert deactivated: {alert_id}")
                    return True
        return False
    
    def get_stats(self):
        """Get monitoring statistics"""
        return {
            **self.stats,
            'connected': self.connected,
            'cached_symbols': len(self.price_cache),
            'active_alerts': sum(len([a for a in alerts if a.get('active')]) 
                               for alerts in self.alerts.values()),
            'uptime': (datetime.now() - (self.stats['connected_at'] or datetime.now())).seconds 
                     if self.stats['connected_at'] else 0
        }