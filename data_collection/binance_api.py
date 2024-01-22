import ccxt
import pandas as pd
from datetime import datetime, timedelta
import time
from sqlalchemy.exc import SQLAlchemyError
from database.db_operations import get_last_fetched_time, add_historical_data, add_real_time_order_book_data
from tqdm import tqdm

class BinanceAPI:
    def __init__(self, session, HistoricalData, RealTimeOrderBookData):
        self.session = session
        self.HistoricalData = HistoricalData
        self.RealTimeOrderBookData = RealTimeOrderBookData
        self.exchange = ccxt.binance({'enableRateLimit': True})
        self.default_batch_size = 1000
        self.min_batch_size = 100

    def fetch_historical_data(self, symbol, start_date='2017-01-01T00:00:00Z'):
        since = self.exchange.parse8601(start_date)
        end = int(datetime.utcnow().timestamp() * 1000)
        batch_size = self.default_batch_size

        print(f"Starting historical data fetch for {symbol} from {start_date}...")
        progress_bar = tqdm(total=(end - since) // (60 * 1000))

        while since < end:
            try:
                candles = self.exchange.fetch_ohlcv(symbol, '1m', since, limit=batch_size)
                if not candles:
                    break
                df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['symbol'] = symbol 
                add_historical_data(self.session, self.HistoricalData, df)
                fetched_count = (candles[-1][0] - since) // (60 * 1000)
                progress_bar.update(fetched_count)
                since = candles[-1][0] + 60000
            except ccxt.NetworkError as e:
                print(f"Network error: {e}")
                time.sleep(10)
            except ccxt.ExchangeError as e:
                print(f"Exchange error: {e}")
                if batch_size > self.min_batch_size:
                    batch_size = max(batch_size // 2, self.min_batch_size)
                time.sleep(10)
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                time.sleep(10)

        progress_bar.close()
        print("Data fetch complete.")

    def fetch_order_book(self, symbol, limit=1000):
        try:
            order_book = self.exchange.fetch_order_book(symbol, limit)
            timestamp = datetime.utcnow()
            return {
                'symbol': symbol,
                'timestamp': timestamp,
                'bids': order_book['bids'],
                'asks': order_book['asks']
            }
        except Exception as e:
            print(f"Error fetching order book: {e}")
            return None

    def continuous_data_update(self, symbol):
        try:
            while True:
                current_time = datetime.utcnow()
                next_minute = (current_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
                remaining_seconds = (next_minute - datetime.utcnow()).total_seconds()

                last_time = get_last_fetched_time(self.session, self.HistoricalData, symbol)
                if last_time:
                    last_time += timedelta(minutes=1)
                    formatted_last_time = last_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                    print(f"Fetching historical data from {formatted_last_time}")
                    self.fetch_historical_data(symbol, start_date=formatted_last_time)
                else:
                    self.fetch_historical_data(symbol)

                time.sleep(max(0, remaining_seconds))

                real_time_data = self.fetch_order_book(symbol)
                if real_time_data:
                    add_real_time_order_book_data(self.session, self.RealTimeOrderBookData, real_time_data)
                    print(f"Real-time order book data fetched for {symbol} at {real_time_data['timestamp']}")

        except SQLAlchemyError as e:
            print(f"Database error: {e}")
