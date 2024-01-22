import ccxt
import pandas as pd
import time
from datetime import datetime

def fetch_historical_data(exchange, symbol, timeframe, start_date, end_date):
    since = exchange.parse8601(start_date)
    all_ohlcv = []
    end = exchange.parse8601(end_date)

    while since < end:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit=1000)
            if not ohlcv:
                break
            since = ohlcv[-1][0] + exchange.parse_timeframe(timeframe) * 1000
            all_ohlcv.extend(ohlcv)
            time.sleep(exchange.rateLimit / 1000)  # Respect the rate limit
        except ccxt.NetworkError as e:
            print(f"Network error: {e}")
            time.sleep(10)
        except ccxt.ExchangeError as e:
            print(f"Exchange error: {e}")
            time.sleep(10)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            time.sleep(10)

    return pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

# Initialize Exchange
exchange = ccxt.binance({
    'rateLimit': 1200,
    'enableRateLimit': True,
})

# Symbol and Timeframe
symbol = 'BTC/USDT'
timeframe = '1m'  # 1 minute timeframe

# Start and End Dates
start_date = '2020-01-01T00:00:00Z'
end_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

# Fetch Data
historical_data = fetch_historical_data(exchange, symbol, timeframe, start_date, end_date)
historical_data['timestamp'] = pd.to_datetime(historical_data['timestamp'], unit='ms')

# Display the data
print(historical_data)

# Save to CSV
historical_data.to_csv('historical_data.csv', index=False)
