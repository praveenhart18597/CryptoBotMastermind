from data_collection.binance_api import BinanceAPI
from database.models import create_historical_data_class, create_real_time_order_book_data_class, Session, Base, engine

def main():
    coin_pair = 'BTC/USDT'  # Set your coin pair here

    # Dynamically create classes
    HistoricalData = create_historical_data_class(coin_pair)
    RealTimeOrderBookData = create_real_time_order_book_data_class(coin_pair)

    # Ensure tables are created
    Base.metadata.create_all(bind=engine)

    # Create a new session
    session = Session()

    # Initialize BinanceAPI with the session and dynamic classes
    binance_api = BinanceAPI(session, HistoricalData, RealTimeOrderBookData)
    binance_api.continuous_data_update(coin_pair)

if __name__ == "__main__":
    main()
