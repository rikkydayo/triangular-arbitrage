import ccxt
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from datetime import datetime, timedelta

logger = logging.getLogger('arbitrage_backtest')
logger.setLevel(logging.INFO)
file_handler = RotatingFileHandler('arbitrage_backtest.log', maxBytes=1_048_576, backupCount=5)
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
binance.set_sandbox_mode(True)

def fetch_historical_data(symbol: str, timeframe: str, since: str, end: str, limit: int = 1000) -> pd.DataFrame:
    since_timestamp = binance.parse8601(since)
    end_timestamp = binance.parse8601(end)
    if since_timestamp is None or end_timestamp is None:
        logger.error("日付パースに失敗")
        return pd.DataFrame()
    
    logger.info(f"データ取得開始: {symbol}, 開始 {since}, 終了 {end}")
    all_data = []
    current_timestamp = since_timestamp

    while current_timestamp < end_timestamp:
        try:
            ohlcv = binance.fetch_ohlcv(symbol, timeframe, since=current_timestamp, limit=limit)
            if not ohlcv:
                logger.warning(f"{symbol}: データが取得できず終了")
                break
            all_data.extend(ohlcv)
            current_timestamp = ohlcv[-1][0] + 1
            logger.debug(f"{symbol}: 取得済みデータポイント {len(all_data)}, 最新タイムスタンプ {pd.to_datetime(ohlcv[-1][0], unit='ms')}")
            if len(ohlcv) < limit:
                break
        except Exception as e:
            logger.error(f"データ取得エラー {symbol}: {e}")
            break
    
    if not all_data:
        logger.error(f"{symbol}: データが全く取得できませんでした")
        return pd.DataFrame()

    df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['bid'] = df['close'] * 0.999
    df['ask'] = df['close'] * 1.001
    df = df[(df['timestamp'] >= pd.to_datetime(since, utc=True)) & (df['timestamp'] <= pd.to_datetime(end, utc=True))]
    logger.info(f"{symbol}: 総データポイント数 {len(df)}, 範囲 {df['timestamp'].min()} - {df['timestamp'].max()}")
    return df

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
    return min(base_slippage * volatility_factor, 0.005)

def calculate_dynamic_threshold(profit_rates: list[float], volatility: float) -> float:
    base = 0.12
    positive_rates = [r for r in profit_rates[-10:] if r > 0]
    if positive_rates:
        recent_mean = sum(positive_rates) / len(positive_rates)
        base += recent_mean * 0.3
    return max(base, 0.12) + (0.2 if volatility > 1 else 0.05)

def filter_outliers(rates: list[float]) -> list[float]:
    if not rates:
        return []
    mean = sum(rates) / len(rates)
    std_dev = (sum((x - mean) ** 2 for x in rates) / len(rates)) ** 0.5
    return [r for r in rates if abs(r - mean) <= 2 * std_dev]

def backtest_triangular_arbitrage(start_date: str, end_date: str):
    logger.info(f"バックテスト呼び出し: 開始 {start_date}, 終了 {end_date}")
    triangles = {
        "BTC-ETH-USDT": ['BTC/USDT', 'ETH/BTC', 'ETH/USDT'],
        "BNB-BTC-USDT": ['BNB/BTC', 'BTC/USDT', 'BNB/USDT']
    }
    
    data = {}
    for triangle, pairs in triangles.items():
        data[triangle] = {}
        for pair in pairs:
            df = fetch_historical_data(pair, '1m', start_date, end_date, limit=1000)
            if df.empty:
                logger.error(f"{pair} のデータが空です。バックテストをスキップします")
                return
            data[triangle][pair] = df

    binance_fee_rate = 0.00075
    slippage_tolerance = 0.005
    max_profit_threshold = 5.0

    results = []
    profit_rates_log = []
    skipped_trades = []

    for triangle, pairs in triangles.items():
        df_length = min(len(data[triangle][pair]) for pair in pairs)
        logger.info(f"バックテスト開始: {triangle}, データポイント数: {df_length}")

        forward_rates_history = []
        reverse_rates_history = []

        for i in range(df_length):
            prices = {pair: {'bid': data[triangle][pair]['bid'][i], 'ask': data[triangle][pair]['ask'][i]} for pair in pairs}
            volatility = check_volatility(data[triangle][pairs[0]], i)
            if volatility > 5:
                continue

            trend = get_trend(data[triangle][pairs[0]], i)
            min_profit_rate = calculate_dynamic_threshold(forward_rates_history + reverse_rates_history, volatility)

            start_usdt = 666.67
            slippage = calculate_slippage(start_usdt, volatility)
            if slippage > slippage_tolerance:
                continue
            fee = 1 - binance_fee_rate
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
                profit_rate += 0.15

            start_usdt_reverse = 666.67
            if triangle == "BTC-ETH-USDT":
                eth_reverse = start_usdt_reverse / (prices['ETH/USDT']['ask'] * (1 + slippage)) * fee
                btc_reverse = eth_reverse * prices['ETH/BTC']['bid'] * (1 - slippage) * fee
                final_usdt_reverse = btc_reverse * prices['BTC/USDT']['bid'] * (1 - slippage) * fee
            else:
                bnb_reverse = start_usdt_reverse / (prices['BNB/USDT']['ask'] * (1 + slippage)) * fee
                btc_reverse = bnb_reverse * prices['BTC/USDT']['bid'] * (1 - slippage) * fee
                final_usdt_reverse = btc_reverse * prices['BNB/BTC']['bid'] * (1 - slippage) * fee
            profit_rate_reverse = (final_usdt_reverse - start_usdt_reverse) / start_usdt_reverse * 100
            if trend == "down":
                profit_rate_reverse += 0.15

            forward_rates_history.append(profit_rate)
            reverse_rates_history.append(profit_rate_reverse)

            profit_rates_log.append({
                'timestamp': data[triangle][pairs[0]]['timestamp'][i],
                'triangle': triangle,
                'profit_rate_forward': profit_rate,
                'profit_rate_reverse': profit_rate_reverse,
                'volatility': volatility,
                'slippage': slippage,
                'prices': prices,
                'trend': trend,
                'min_profit_rate': min_profit_rate
            })

            if (trend == "up" and profit_rate <= min_profit_rate) or (trend == "down" and profit_rate_reverse <= min_profit_rate):
                if profit_rate > 0 or profit_rate_reverse > 0:
                    skipped_trades.append({
                        'triangle': triangle,
                        'profit_rate': max(profit_rate, profit_rate_reverse),
                        'timestamp': data[triangle][pairs[0]]['timestamp'][i]
                    })
                continue

            if profit_rate > min_profit_rate or profit_rate_reverse > min_profit_rate:
                direction = "順方向" if profit_rate > profit_rate_reverse else "逆方向"
                selected_rate = max(profit_rate, profit_rate_reverse)
                if selected_rate > max_profit_threshold:
                    logger.debug(f"{triangle} {i}: 異常値としてスキップ (profit_rate={selected_rate:.2f}%)")
                    continue
                if selected_rate <= 0:
                    logger.warning(f"{triangle} {i}: マイナス利益検出 (profit_rate={selected_rate:.2f}%)、スキップ")
                    continue
                profit_usdt = start_usdt * selected_rate / 100
                result = {
                    'timestamp': data[triangle][pairs[0]]['timestamp'][i],
                    'triangle': triangle,
                    'direction': direction,
                    'profit_rate': selected_rate,
                    'profit_usdt': profit_usdt,
                    'slippage': slippage * start_usdt,
                    'fee': binance_fee_rate * 3 * start_usdt,
                    'volatility': volatility,
                    'prices': prices,
                    'trend': trend,
                    'min_profit_rate': min_profit_rate
                }
                results.append(result)
                logger.info(f"利益検出: {triangle} {direction} 利益率 {selected_rate:.2f}% ({profit_usdt:.2f} USDT) at {result['timestamp']} "
                           f"(volatility={volatility:.2f}%, slippage={slippage:.4f}, trend={trend}, threshold={min_profit_rate:.2f}%)")

        if results:
            avg_profit = sum(r['profit_rate'] for r in results) / len(results)
            total_profit = sum(r['profit_usdt'] for r in results)
            logger.info(f"バックテスト結果 {triangle}: 平均利益率 {avg_profit:.2f}%, 取引回数 {len(results)}, 純利益 {total_profit:.2f} USDT")
        else:
            logger.info(f"{triangle}: 利益機会なし")
            avg_slippage = sum(log['slippage'] for log in profit_rates_log if log['triangle'] == triangle) / len(profit_rates_log)
            avg_threshold = sum(log['min_profit_rate'] for log in profit_rates_log if log['triangle'] == triangle) / len(profit_rates_log)
            logger.info(f"{triangle} 統計: 平均スリッページ={avg_slippage:.4f}, 平均閾値={avg_threshold:.2f}%")

        if skipped_trades:
            skipped_for_triangle = [t for t in skipped_trades if t['triangle'] == triangle]
            skipped_count = len(skipped_for_triangle)
            filtered_skipped = filter_outliers([t['profit_rate'] for t in skipped_for_triangle])
            avg_skipped_profit = sum(filtered_skipped) / len(filtered_skipped) if filtered_skipped else 0
            max_skipped_profit = max(filtered_skipped) if filtered_skipped else 0
            logger.info(f"{triangle} スキップされた取引: 件数={skipped_count}, 平均利益率={avg_skipped_profit:.2f}%, 最大利益率={max_skipped_profit:.2f}%")

        forward_rates = filter_outliers([log['profit_rate_forward'] for log in profit_rates_log if log['triangle'] == triangle])
        reverse_rates = filter_outliers([log['profit_rate_reverse'] for log in profit_rates_log if log['triangle'] == triangle])
        if forward_rates:
            logger.info(f"{triangle} 順方向平均利益率: {sum(forward_rates)/len(forward_rates):.4f}%, 最大: {max(forward_rates):.4f}%")
        if reverse_rates:
            logger.info(f"{triangle} 逆方向平均利益率: {sum(reverse_rates)/len(reverse_rates):.4f}%, 最大: {max(reverse_rates):.4f}%")
        
        profit_df = pd.DataFrame(profit_rates_log)
        profit_df.to_csv(f"profit_rates_{triangle}.csv", index=False)
        logger.info(f"{triangle} の利益率データを profit_rates_{triangle}.csv に保存しました")

if __name__ == "__main__":
    # 現在日付（2025年3月20日）から過去30日間をテスト
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"直近30日間のバックテストを開始: {start_date} - {end_date}")
    backtest_triangular_arbitrage(start_date, end_date)