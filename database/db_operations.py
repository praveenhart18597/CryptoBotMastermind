from datetime import datetime 
from sqlalchemy.exc import SQLAlchemyError
import json

def get_last_fetched_time(session, HistoricalData, symbol):
    try:
        last_record = session.query(HistoricalData)\
                             .filter(HistoricalData.symbol == symbol)\
                             .order_by(HistoricalData.timestamp.desc())\
                             .first()
        return last_record.timestamp if last_record else None
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return None

def add_historical_data(session, HistoricalData, data):
    try:
        records_to_add = [
            HistoricalData(
                symbol=record['symbol'],
                timestamp=datetime.utcfromtimestamp(record['timestamp'] / 1000),
                open=record['open'],
                high=record['high'],
                low=record['low'],
                close=record['close'],
                volume=record['volume']
            ) for record in data.to_dict('records')
        ]
        session.bulk_save_objects(records_to_add)
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Database error during bulk insert: {e}")

def add_real_time_order_book_data(session, RealTimeOrderBookData, data):
    try:
        order_book_record = RealTimeOrderBookData(
            symbol=data['symbol'],
            timestamp=data['timestamp'],
            bids=json.dumps(data['bids']), 
            asks=json.dumps(data['asks'])
        )
        session.add(order_book_record)
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Database error during insert: {e}")
