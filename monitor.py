"""
PSX Stock Monitor - Railway Deployment
Automatically monitors all PSX stocks and sends email alerts
"""

import time
import threading
import logging
import sys
import os
from datetime import datetime, timedelta
from websocket_monitor import PSXWebSocketMonitor
from email_sender import send_alert_email

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Configuration — all values from environment variables
TARGET_EMAIL          = os.environ.get("TARGET_EMAIL", "ammarilyas343@gmail.com")
ALERT_THRESHOLD       = float(os.environ.get("ALERT_THRESHOLD", "4.0"))        # % surge to trigger alert
MONITOR_WINDOW        = int(os.environ.get("MONITOR_WINDOW", "1200"))           # rolling window in seconds (default 20 min)
MIN_VALUE_THRESHOLD   = float(os.environ.get("MIN_VALUE_THRESHOLD", "20000000")) # minimum traded value (Rs.)
RESEND_API_KEY        = os.environ.get("RESEND_API_KEY", "")                    # Resend API key
SECRET_KEY            = os.environ.get("SECRET_KEY", "psx-stock-tracker-secret")
DATABASE_URL          = os.environ.get("DATABASE_URL", "sqlite:///alerts.db")

# PSX Market close time in Pakistan Standard Time (UTC+5)
# After this time, reset all 'alerted' flags so stocks can be alerted again next day
MARKET_CLOSE_HOUR   = int(os.environ.get("MARKET_CLOSE_HOUR",   "15"))   # 3 PM PKT
MARKET_CLOSE_MINUTE = int(os.environ.get("MARKET_CLOSE_MINUTE", "30"))   # :30

# Stock tracking dictionary
# {symbol: {base_price, last_update, alerted, alerted_date}}
stock_tracker = {}
_last_reset_date = None  # track the last date we reset alerted flags

def check_and_send_alerts(ws_monitor):
    """Check all stocks and send alerts if threshold met"""
    try:
        # Get all current prices from WebSocket cache
        # Copy the dictionary to avoid "dictionary changed size during iteration" errors
        # when the websocket thread adds new symbols
        prices = ws_monitor.get_all_prices().copy()
        
        active_stocks = 0
        alerts_sent = 0
        
        for symbol, data in prices.items():
            current_price = data.get('price')
            if not current_price:
                continue
            
            active_stocks += 1
            
            # Initialize tracking for new symbols
            if symbol not in stock_tracker:
                stock_tracker[symbol] = {
                    'base_price': current_price,
                    'alerted': False,
                    'alerted_date': None,
                    'history': []  # Store (timestamp, price) for rolling window
                }
                logger.info(f"Tracking new stock: {symbol} @ Rs. {current_price:.2f}")
                
            tracker = stock_tracker[symbol]
            
            # Track price history for rolling window
            now = datetime.now()
            tracker['history'].append((now, current_price))
            
            # Clean up history older than the defined monitoring window
            tracker['history'] = [p for p in tracker['history'] if (now - p[0]).total_seconds() <= MONITOR_WINDOW]
            
            if len(tracker['history']) < 2:
                continue
                
            # Compare current price to the price at the start of the window
            old_price = tracker['history'][0][1]
            if old_price <= 0:
                continue
                
            change_pct = ((current_price - old_price) / old_price) * 100
            
            # Value Calculation (Current Price * Volume)
            volume = data.get('volume', 0)
            traded_value = current_price * volume
            
            # Condition: threshold % surge AND minimum traded value AND not already alerted today
            if change_pct >= ALERT_THRESHOLD and traded_value >= MIN_VALUE_THRESHOLD and not tracker['alerted']:
                direction = "UP"
                logger.info(f"🚨 SURGE ALERT: {symbol} surged +{change_pct:.2f}% in {MONITOR_WINDOW//60} mins (Value: Rs. {traded_value:,.0f})")

                try:
                    # Send email alert
                    send_alert_email(
                        to_email=TARGET_EMAIL,
                        symbol=symbol,
                        current_price=current_price,
                        base_price=old_price,  # Price from the start of the rolling window
                        change_percent=change_pct,
                        direction=direction,
                        traded_value=traded_value
                    )

                    # Mark as alerted for today — resets at market close, NOT after interval
                    tracker['alerted'] = True
                    tracker['alerted_date'] = now.date()
                    alerts_sent += 1
                    logger.info(f"✅ Alert email sent to {TARGET_EMAIL}")

                except Exception as e:
                    logger.error(f"Failed to send alert for {symbol}: {e}")
        
        # Periodic status update
        if active_stocks > 0:
            logger.info(f"📊 Monitoring {active_stocks} stocks, {len([t for t in stock_tracker.values() if t['alerted']])} alerted today")

        return alerts_sent

    except Exception as e:
        logger.error(f"Error in check_and_send_alerts: {e}")
        return 0


def reset_daily_alerts():
    """
    Reset the 'alerted' flag for every stock once per day — after PSX market close.
    This runs in its own thread and checks the time every minute.
    PSX closes at 15:30 PKT (UTC+5).  The reset fires once the clock first
    passes that time on a given calendar date.
    """
    global _last_reset_date

    import pytz
    PKT = pytz.timezone("Asia/Karachi")

    logger.info(f"Daily reset scheduler started — resets alerted flags after "
                f"{MARKET_CLOSE_HOUR:02d}:{MARKET_CLOSE_MINUTE:02d} PKT each day")

    while True:
        try:
            now_pkt = datetime.now(PKT)
            today  = now_pkt.date()

            # Fire once per day, after market close time
            if (
                now_pkt.hour > MARKET_CLOSE_HOUR
                or (now_pkt.hour == MARKET_CLOSE_HOUR and now_pkt.minute >= MARKET_CLOSE_MINUTE)
            ) and _last_reset_date != today:

                # Use list() to avoid dictionary size change exception during thread execution
                count = sum(1 for t in list(stock_tracker.values()) if t.get('alerted'))
                for tracker in list(stock_tracker.values()):
                    tracker['alerted']      = False
                    tracker['alerted_date'] = None

                _last_reset_date = today
                logger.info(f"🔄 Daily reset: cleared 'alerted' flag for {count} stocks "
                            f"after market close ({now_pkt.strftime('%Y-%m-%d %H:%M %Z')})")

        except Exception as e:
            logger.error(f"Error in daily reset scheduler: {e}")

        time.sleep(60)  # Check every minute

def main():
    """Main monitoring loop"""
    logger.info("=" * 70)
    logger.info(f"Alert Email        : {TARGET_EMAIL}")
    logger.info(f"Alert Threshold    : +{ALERT_THRESHOLD}%")
    logger.info(f"Min Traded Value   : Rs. {MIN_VALUE_THRESHOLD:,.0f}")
    logger.info(f"Monitoring Window  : {MONITOR_WINDOW // 60} minutes")
    logger.info(f"Market Close (reset): {MARKET_CLOSE_HOUR:02d}:{MARKET_CLOSE_MINUTE:02d} PKT")
    logger.info("=" * 70)
    
    # Initialize WebSocket monitor
    logger.info("Starting WebSocket monitor...")
    ws_monitor = PSXWebSocketMonitor()

    # Start daily reset scheduler in background
    reset_thread = threading.Thread(target=reset_daily_alerts, daemon=True)
    reset_thread.start()
    logger.info("Daily alert reset scheduler started")

    # Wait for initial data to populate
    logger.info("Waiting for initial price data...")
    time.sleep(15)

    # Check how many stocks we have
    initial_prices = ws_monitor.get_all_prices()
    logger.info(f"✅ Monitoring {len(initial_prices)} stocks from PSX Terminal")

    # Main monitoring loop
    loop_count = 0
    last_status = datetime.now()
    
    while True:
        try:
            loop_count += 1
            
            # Check and send alerts
            check_and_send_alerts(ws_monitor)
            
            # Print status every 5 minutes
            if (datetime.now() - last_status).total_seconds() >= 300:
                ws_stats = ws_monitor.get_stats()
                logger.info(f"Status: WebSocket={'Connected' if ws_stats.get('connected') else 'Disconnected'}, "
                          f"Tracked={len(stock_tracker)}, Loops={loop_count}")
                last_status = datetime.now()
            
            # Sleep for 5 seconds before next check
            time.sleep(5)
            
        except KeyboardInterrupt:
            logger.info("\n👋 Shutting down monitor...")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(10)  # Wait before retrying

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)
