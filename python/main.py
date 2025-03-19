import ccxt
import redis
import time
from dotenv import load_dotenv
import os
import json
import requests
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional

# ログ設定
logger = logging.getLogger('arbitrage')
logger.setLevel(logging.INFO)
file_handler = RotatingFileHandler('arbitrage.log', maxBytes=1_048_576, backupCount=5)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 環境変数読み込み
load_dotenv()
api_key: Optional[str] = os.getenv("BINANCE_API_KEY")
api_secret: Optional[str] = os.getenv("BINANCE_API_SECRET")
slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")

if not api_key or not api_secret or not slack_webhook_url:
    missing = []
    if not api_key:
        missing.append("BINANCE_API_KEY")
    if not api_secret:
        missing.append("BINANCE_API_SECRET")
    if not slack_webhook_url:
        missing.append("SLACK_WEBHOOK_URL")
    error_msg = f"環境変数が設定されていません: {', '.join(missing)}"
    logger.error(error_msg)
    exit(1)

# Slack通知関数
def send_slack_notification(message: str) -> None:
    payload = {"text": message}
    if slack_webhook_url:
        response = requests.post(slack_webhook_url, json=payload)
    else:
        logger.error("Slack Webhook URL is not set.")
        return
    if response.status_code != 200:
        logger.error(f"Slack通知失敗: {response.text}")

# Binance本番環境接続
exchange = ccxt.binance({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'urls': {'api': 'https://api.binance.com/api/v3'}
})
r = redis.Redis(host='localhost', port=6379, db=0)
exchange.set_sandbox_mode(True)
def triangular_arbitrage():
    pairs = ['BTC/USDT', 'ETH/BTC', 'ETH/USDT']
    while True:
        try:
            prices = {}
            for pair in pairs:
                ticker = exchange.fetch_ticker(pair)
                bid = ticker.get('bid')
                ask = ticker.get('ask')
                if bid is None or ask is None:
                    logger.warning(f"{pair} のデータが不正: bid={bid}, ask={ask}")
                    continue
                prices[pair] = {'bid': float(bid), 'ask': float(ask)}
                logger.info(f"{pair}: bid={bid}, ask={ask}")

            # 順方向
            start_usdt = 100000
            fee = 0.999
            btc = start_usdt / prices['BTC/USDT']['ask'] * fee
            eth = btc / prices['ETH/BTC']['ask'] * fee
            final_usdt = eth * prices['ETH/USDT']['bid'] * fee
            profit_rate = (final_usdt - start_usdt) / start_usdt * 100
            logger.info(f"順方向の利益率: {profit_rate:.2f}%")

            # 逆方向
            start_usdt_reverse = 100000
            eth_reverse = start_usdt_reverse / prices['ETH/USDT']['ask'] * fee
            btc_reverse = eth_reverse * prices['ETH/BTC']['bid'] * fee
            final_usdt_reverse = btc_reverse * prices['BTC/USDT']['bid'] * fee
            profit_rate_reverse = (final_usdt_reverse - start_usdt_reverse) / start_usdt_reverse * 100
            logger.info(f"逆方向の利益率: {profit_rate_reverse:.2f}%")

            # シグナル送信
            if profit_rate > 0.1 or profit_rate_reverse > 0.1:
                direction = "順方向" if profit_rate > profit_rate_reverse else "逆方向"
                selected_rate = max(profit_rate, profit_rate_reverse)
                signal = {
                    'prices': {
                        'BTC/USDT': prices['BTC/USDT']['ask' if direction == "順方向" else 'bid'],
                        'ETH/BTC': prices['ETH/BTC']['ask' if direction == "順方向" else 'bid'],
                        'ETH/USDT': prices['ETH/USDT']['bid' if direction == "順方向" else 'ask']
                    },
                    'profit_rate': selected_rate,
                    'direction': direction
                }
                r.set('arbitrage_signal', json.dumps(signal))
                message = f"シグナル検出: {direction} 利益率 {selected_rate:.2f}%"
                logger.info(message)
                send_slack_notification(message)
            time.sleep(0.1)  # 100ms間隔
        except Exception as e:
            error_msg = f"エラー: {e}"
            logger.error(error_msg)
            time.sleep(1)

if __name__ == "__main__":
    triangular_arbitrage()