"""
Email notification system using Resend API
Replaced SMTP because Railway blocks SMTP ports for unverified accounts.
"""

import os
import logging
import resend
from datetime import datetime

logger = logging.getLogger(__name__)

# Configure Resend API Key — MUST be set as environment variable RESEND_API_KEY
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
if not RESEND_API_KEY:
    logger.warning("⚠️  RESEND_API_KEY environment variable is not set — emails will fail!")
resend.api_key = RESEND_API_KEY

def send_alert_email(to_email, symbol, current_price, base_price, change_percent, direction, traded_value=0):
    """
    Send stock alert email via Resend API
    """
    
    # Resend Free Tier note: 
    # If the domain is not verified, you can only send from 'onboarding@resend.dev'
    # and only to the email you used to sign up.
    from_email = "PSX Alert <onboarding@resend.dev>"
    
    subject = f"🚨 PSX Alert: {symbol} {direction} {abs(change_percent):.1f}%"
    
    # Email body
    arrow = "📈" if direction == "UP" else "📉"
    body_html = f"""
    <div style="font-family: sans-serif; padding: 20px; border: 1px solid #eee; border-radius: 10px;">
        <h2 style="color: #333;">PSX Stock Alert</h2>
        <hr>
        <p style="font-size: 1.2em;">{arrow} <strong>Stock:</strong> {symbol}</p>
        <p><strong>Direction:</strong> {direction}</p>
        <p><strong>Change:</strong> <span style="color: {'green' if direction == 'UP' else 'red'};">{change_percent:+.2f}%</span></p>
        
            <p>💰 <strong>Current Price:</strong> Rs. {current_price:.2f}</p>
            <p>📊 <strong>Base Price:</strong> Rs. {base_price:.2f}</p>
            <p>📈 <strong>Price Difference:</strong> Rs. {abs(current_price - base_price):.2f}</p>
            <p>💸 <strong>Traded Value:</strong> Rs. {traded_value:,.0f}</p>
        </div>
        
        <p style="color: #888; font-size: 0.8em; margin-top: 20px;">
            ⏰ Triggered at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
            ---<br>
            Alert Threshold: +4%<br>
            Monitoring Window: 20 minutes<br>
            Min Value: Rs. 20,000,000 (2 Crore)<br>
            Automated by PSX Stock Monitor
        </p>
    </div>
    """
    
    try:
        params = {
            "from": from_email,
            "to": [to_email],
            "subject": subject,
            "html": body_html,
        }
        
        email = resend.Emails.send(params)
        logger.info(f"✅ Email sent via Resend API to {to_email} for {symbol}. ID: {email['id']}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Resend API error: {e}")
        # If there's an error, check if the API key is set
        if not RESEND_API_KEY or RESEND_API_KEY == "your_resend_api_key":
            logger.error("Missing RESEND_API_KEY environment variable")
        raise

def test_email_config():
    """Test email configuration by sending a test message"""
    try:
        # Get target email from monitor.py configuration or fallback
        target = os.environ.get("TARGET_EMAIL", "ammarilyas343@gmail.com")
        send_alert_email(
            to_email=target,
            symbol="TEST",
            current_price=100.00,
            base_price=95.24,
            change_percent=5.0,
            direction="UP",
            traded_value=25000000
        )
        print("Test email sent successfully via Resend!")
        return True
    except Exception as e:
        print(f"Test email failed: {e}")
        return False

if __name__ == '__main__':
    # Run test when executed directly
    print("Testing Resend API configuration...")
    test_email_config()
