import ccxt

print("スクリプト開始")
exchange = ccxt.binance({
    'enableRateLimit': True,
    'urls': {'api': 'https://testnet.binance.vision/api/v3'}
})
print("取引所初期化完了")
exchange.set_sandbox_mode(True)
print("Testnetモード有効")

try:
    print("Ticker取得開始")
    ticker = exchange.fetch_ticker('BTC/USDT')
    print("Ticker取得成功:", ticker)
except Exception as e:
    print("エラー:", e)