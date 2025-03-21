import ccxt
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import time
import websocket
import json
import threading
from typing import Dict, Optional, List, cast
import requests

logger = logging.getLogger('arbitrage_live')
logger.setLevel(logging.INFO)
file_handler = RotatingFileHandler('arbitrage_live.log', maxBytes=1_048_576, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(file_handler)
logger.addHandler(console_handler)

load_dotenv()
api_key = os.getenv("BINANCE_API_KEY", "")
api_secret = os.getenv("BINANCE_API_SECRET", "")

if not all([api_key, api_secret]):
    missing = [k for k, v in {"API_KEY": api_key, "API_SECRET": api_secret}.items() if not v]
    logger.error(f"環境変数が設定されていません: {', '.join(missing)}")
    exit(1)

binance = ccxt.binance({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
})
binance.set_sandbox_mode(False)  # 本番環境用

# WebSocket データを保持するグローバル変数
order_books: Dict[str, Dict[str, float]] = {}
recent_prices: Dict[str, pd.DataFrame] = {}
lock = threading.Lock()

def on_message(ws, message):
    data = json.loads(message)
    if 'e' not in data:
        return

    symbol = data['s'].replace('/', '').lower()  # 例: btcusdt
    if data['e'] == 'depthUpdate':
        with lock:
            bids = data.get('b', [])
            asks = data.get('a', [])
            if bids and asks:
                bid_price = bids[0][0]
                ask_price = asks[0][0]
                if bid_price is None or ask_price is None:
                    logger.error(f"WebSocket: オーダーブックの価格が None です {symbol}: bid={bid_price}, ask={ask_price}")
                    return
                bid_price = cast(float, bid_price)
                ask_price = cast(float, ask_price)
                order_books[symbol] = {
                    'bid': float(bid_price),
                    'ask': float(ask_price)
                }
    elif data['e'] == 'kline':
        kline = data['k']
        if kline['x']:  # 確定足のみ処理
            with lock:
                df = recent_prices.get(symbol, pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']))
                new_row = pd.DataFrame({
                    'timestamp': [pd.to_datetime(kline['t'], unit='ms', utc=True)],
                    'open': [float(kline['o'])],
                    'high': [float(kline['h'])],
                    'low': [float(kline['l'])],
                    'close': [float(kline['c'])],
                    'volume': [float(kline['v'])]
                })
                df = pd.concat([df, new_row], ignore_index=True)
                df = df.tail(10)  # 最新10件のみ保持
                df['bid'] = df['close'] * 0.999
                recent_prices[symbol] = df

def on_error(ws, error):
    logger.error(f"WebSocket エラー: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket 接続が閉じました")

def on_open(ws):
    logger.info("WebSocket 接続が開きました")

def start_websocket():
    unique_pairs = ['BTC/USDT', 'ETH/BTC', 'ETH/USDT', 'BNB/BTC', 'BNB/USDT']
    streams = []
    for pair in unique_pairs:
        symbol = pair.replace('/', '').lower()
        streams.append(f"{symbol}@depth")  # オーダーブックストリーム
        streams.append(f"{symbol}@kline_1m")  # 1分足K線ストリーム
    stream_path = '/'.join(streams)
    ws_url = f"wss://stream.binance.com:9443/ws/{stream_path}"
    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close, on_open=on_open)
    ws.run_forever()

def fetch_order_book(symbol: str) -> Optional[Dict[str, float]]:
    try:
        order_book = binance.fetch_order_book(symbol, limit=5)
        if order_book is None:
            logger.error(f"オーダーブックが None です {symbol}")
            return None
        used_weight = 'N/A'
        if binance.last_response_headers is not None:
            used_weight = binance.last_response_headers.get('x-mbx-used-weight-1m', 'N/A')
        logger.info(f"リクエストウェイト使用量: {used_weight} (symbol={symbol})")
        if not order_book.get('bids') or not order_book.get('asks'):
            logger.error(f"オーダーブックが空です {symbol}: bids={order_book.get('bids')}, asks={order_book.get('asks')}")
            return None
        bid_price = order_book['bids'][0][0]
        ask_price = order_book['asks'][0][0]
        if bid_price is None or ask_price is None:
            logger.error(f"オーダーブックの価格が None です {symbol}: bid={bid_price}, ask={ask_price}")
            return None
        bid_price = cast(float, bid_price)
        ask_price = cast(float, ask_price)
        return {
            'bid': float(bid_price),
            'ask': float(ask_price)
        }
    except Exception as e:
        logger.error(f"オーダーブック取得エラー {symbol}: {e}")
        return None

def fetch_recent_prices(symbol: str, timeframe: str = '1m', limit: int = 10) -> pd.DataFrame:
    try:
        ohlcv = binance.fetch_ohlcv(symbol, timeframe, limit=limit)
        used_weight = 'N/A'
        if binance.last_response_headers is not None:
            used_weight = binance.last_response_headers.get('x-mbx-used-weight-1m', 'N/A')
        logger.info(f"リクエストウェイト使用量: {used_weight} (symbol={symbol})")
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df['bid'] = df['close'] * 0.999
        return df
    except Exception as e:
        logger.error(f"価格取得エラー {symbol}: {e}")
        return pd.DataFrame()

def check_volatility(df: pd.DataFrame, index: int) -> float:
    start = max(0, index - 4)
    prices = df['close'][start:index + 1]
    return (max(prices) - min(prices)) / min(prices) * 100 if len(prices) > 1 else 0

def get_trend(df: pd.DataFrame, index: int) -> str:
    start = max(0, index - 9)
    sma = df['close'][start:index + 1].mean()
    current_price = df['bid'][index]
    return "up" if current_price > sma else "down"

def calculate_slippage(amount: float, volatility: float) -> float:
    base_slippage = 0.0015 * (amount / 666.67)
    volatility_factor = 1 + (volatility / 10)
    return min(base_slippage * volatility_factor, 0.01)

def calculate_dynamic_threshold(profit_rates: List[float], volatility: float) -> float:
    base = 0.12
    positive_rates = [r for r in profit_rates[-10:] if r > 0]
    if positive_rates:
        recent_mean = sum(positive_rates) / len(positive_rates)
        base += recent_mean * 0.3
    return max(base, 0.12) + (0.2 if volatility > 1 else 0.05)

def notify_go_server(triangle: str, direction: str, profit_rate: float, profit_usdt: float, volatility: float, slippage: float, trend: str, threshold: float) -> None:
    url = "http://localhost:8080/notify"
    data = {
        "triangle": triangle,
        "direction": direction,
        "profit_rate": profit_rate,
        "profit_usdt": profit_usdt,
        "volatility": volatility,
        "slippage": slippage,
        "trend": trend,
        "threshold": threshold
    }
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=data, timeout=5)
            if response.status_code == 200:
                logger.info("Go サーバーに通知成功")
                return
            else:
                logger.error(f"Go サーバーへの通知失敗: ステータスコード {response.status_code}")
        except Exception as e:
            logger.error(f"Go サーバーへの通知エラー (試行 {attempt + 1}/{max_retries}): {e}")
        time.sleep(1)
    logger.error("Go サーバーへの通知が最大試行回数に達しました")

def execute_trade(triangle: str, direction: str, prices: Dict[str, Dict[str, float]], amount: float) -> None:
    try:
        if triangle == "BTC-ETH-USDT":
            if direction == "順方向":
                binance.create_market_sell_order('BTC/USDT', amount / prices['BTC/USDT']['ask'])
                binance.create_market_sell_order('ETH/BTC', amount / prices['BTC/USDT']['ask'] / prices['ETH/BTC']['ask'])
                binance.create_market_buy_order('ETH/USDT', amount / prices['BTC/USDT']['ask'] / prices['ETH/BTC']['ask'])
            else:
                binance.create_market_sell_order('ETH/USDT', amount / prices['ETH/USDT']['ask'])
                binance.create_market_buy_order('ETH/BTC', amount / prices['ETH/USDT']['ask'])
                binance.create_market_buy_order('BTC/USDT', amount / prices['ETH/USDT']['ask'] * prices['ETH/BTC']['bid'])
        else:  # BNB-BTC-USDT
            if direction == "順方向":
                binance.create_market_sell_order('BNB/BTC', amount / prices['BNB/BTC']['ask'])
                binance.create_market_sell_order('BTC/USDT', amount / prices['BNB/BTC']['ask'] / prices['BTC/USDT']['ask'])
                binance.create_market_buy_order('BNB/USDT', amount / prices['BNB/BTC']['ask'] / prices['BTC/USDT']['ask'])
            else:
                binance.create_market_sell_order('BNB/USDT', amount / prices['BNB/USDT']['ask'])
                binance.create_market_buy_order('BTC/USDT', amount / prices['BNB/USDT']['ask'])
                binance.create_market_buy_order('BNB/BTC', amount / prices['BNB/USDT']['ask'] * prices['BTC/USDT']['bid'])
        logger.info(f"取引実行: {triangle} {direction} 成功")
    except Exception as e:
        logger.error(f"取引実行エラー {triangle} {direction}: {e}")

def live_triangular_arbitrage() -> None:
    triangles = {
        "BTC-ETH-USDT": ['BTC/USDT', 'ETH/BTC', 'ETH/USDT'],
        "BNB-BTC-USDT": ['BNB/BTC', 'BTC/USDT', 'BNB/USDT']
    }
    unique_pairs = list(set(pair for triangle in triangles.values() for pair in triangle))
    unique_pairs_lower = [pair.replace('/', '').lower() for pair in unique_pairs]

    binance_fee_rate = 0.001  # 0.1%
    slippage_tolerance = 0.01
    max_profit_threshold = 5.0
    profit_rates_history: List[float] = []

    # 初期データを取得（WebSocket がデータを取得するまでの準備）
    for pair in unique_pairs:
        symbol = pair.replace('/', '').lower()
        try:
            order_book = fetch_order_book(pair)
            if order_book:
                order_books[symbol] = order_book
            ohlcv = binance.fetch_ohlcv(pair, '1m', limit=10)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df['bid'] = df['close'] * 0.999
            recent_prices[symbol] = df
        except Exception as e:
            logger.error(f"初期データ取得エラー {pair}: {e}")

    # WebSocket スレッドを開始
    ws_thread = threading.Thread(target=start_websocket)
    ws_thread.daemon = True
    ws_thread.start()

    # WebSocket データが揃うまで待機
    logger.info("WebSocket データが揃うのを待機中...")
    while True:
        with lock:
            if all(pair in order_books and pair in recent_prices for pair in unique_pairs_lower):
                logger.info("WebSocket データが揃いました")
                break
        time.sleep(1)

    while True:
        for triangle, pairs in triangles.items():
            prices: Dict[str, Dict[str, float]] = {}
            recent_data: Dict[str, pd.DataFrame] = {}
            for pair in pairs:
                symbol = pair.replace('/', '').lower()
                with lock:
                    if symbol not in order_books or symbol not in recent_prices:
                        logger.warning(f"{pair}: WebSocket データがまだありません")
                        continue
                    prices[pair] = order_books[symbol]
                    recent_data[pair] = recent_prices[symbol]
                    # 価格をログに出力
                    logger.info(f"{pair}: bid={prices[pair]['bid']:.2f}, ask={prices[pair]['ask']:.2f}")

            if not recent_data[pairs[0]].empty:
                volatility = check_volatility(recent_data[pairs[0]], len(recent_data[pairs[0]]) - 1)
                if volatility > 5:
                    logger.info(f"{triangle}: ボラティリティが高い ({volatility:.2f}%)、スキップ")
                    continue

                trend = get_trend(recent_data[pairs[0]], len(recent_data[pairs[0]]) - 1)
                min_profit_rate = calculate_dynamic_threshold(profit_rates_history, volatility)

                start_usdt = 666.67
                slippage = calculate_slippage(start_usdt, volatility)
                if slippage > slippage_tolerance:
                    logger.info(f"{triangle}: スリッページが大きすぎる ({slippage:.4f})、スキップ")
                    continue
                fee = 1 - binance_fee_rate

                # 順方向の計算
                if triangle == "BTC-ETH-USDT":
                    btc = start_usdt / (prices['BTC/USDT']['ask'] * (1 + slippage)) * fee
                    eth = btc / (prices['ETH/BTC']['ask'] * (1 + slippage)) * fee
                    final_usdt = eth * prices['ETH/USDT']['bid'] * (1 - slippage) * fee
                else:
                    bnb = start_usdt / (prices['BNB/BTC']['ask'] * (1 + slippage)) * fee
                    btc = bnb / (prices['BTC/USDT']['ask'] * (1 + slippage)) * fee
                    final_usdt = btc * prices['BNB/USDT']['bid'] * (1 - slippage) * fee
                profit_rate = (final_usdt - start_usdt) / start_usdt * 100
                if trend == "up":
                    profit_rate += 0.2
                logger.info(f"{triangle} 順方向の利益率: {profit_rate:.2f}%")

                # 逆方向の計算
                if triangle == "BTC-ETH-USDT":
                    eth_reverse = start_usdt / (prices['ETH/USDT']['ask'] * (1 + slippage)) * fee
                    btc_reverse = eth_reverse * prices['ETH/BTC']['bid'] * (1 - slippage) * fee
                    final_usdt_reverse = btc_reverse * prices['BTC/USDT']['bid'] * (1 - slippage) * fee
                else:
                    bnb_reverse = start_usdt / (prices['BNB/USDT']['ask'] * (1 + slippage)) * fee
                    btc_reverse = bnb_reverse * prices['BTC/USDT']['bid'] * (1 - slippage) * fee
                    final_usdt_reverse = btc_reverse * prices['BNB/BTC']['bid'] * (1 - slippage) * fee
                profit_rate_reverse = (final_usdt_reverse - start_usdt) / start_usdt * 100
                if trend == "down":
                    profit_rate_reverse += 0.2
                logger.info(f"{triangle} 逆方向の利益率: {profit_rate_reverse:.2f}%")

                profit_rates_history.append(max(profit_rate, profit_rate_reverse))

                if (trend == "up" and profit_rate > min_profit_rate) or (trend == "down" and profit_rate_reverse > min_profit_rate):
                    direction = "順方向" if profit_rate > profit_rate_reverse else "逆方向"
                    selected_rate = max(profit_rate, profit_rate_reverse)
                    if selected_rate > max_profit_threshold:
                        logger.debug(f"{triangle}: 異常値としてスキップ (profit_rate={selected_rate:.2f}%)")
                        continue
                    if selected_rate <= 0:
                        logger.warning(f"{triangle}: マイナス利益検出 (profit_rate={selected_rate:.2f}%)、スキップ")
                        continue
                    profit_usdt = start_usdt * selected_rate / 100
                    logger.info(f"利益検出: {triangle} {direction} 利益率 {selected_rate:.2f}% ({profit_usdt:.2f} USDT) "
                               f"(volatility={volatility:.2f}%, slippage={slippage:.4f}, trend={trend}, threshold={min_profit_rate:.2f}%)")
                    notify_go_server(triangle, direction, selected_rate, profit_usdt, volatility, slippage, trend, min_profit_rate)
                    execute_trade(triangle, direction, prices, start_usdt)

        time.sleep(0.01)

if __name__ == "__main__":
    live_triangular_arbitrage()