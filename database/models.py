from sqlalchemy import JSON, create_engine, Column, Integer, DECIMAL, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('mssql+pyodbc://CMMDBSERVER')
Base = declarative_base()

def create_historical_data_class(symbol):
    class_name = f'HistoricalData_{symbol.replace("/", "_")}'
    table_name = f'hist_data_{symbol.lower().replace("/", "_")}'
    
    return type(class_name, (Base,), {
        '__tablename__': table_name,
        'id': Column(Integer, primary_key=True),
        'symbol': Column(String),
        'timestamp': Column(DateTime),
        'open': Column(DECIMAL(precision=18, scale=8)),
        'high': Column(DECIMAL(precision=18, scale=8)),
        'low': Column(DECIMAL(precision=18, scale=8)),
        'close': Column(DECIMAL(precision=18, scale=8)),
        'volume': Column(DECIMAL(precision=18, scale=8))
    })

def create_real_time_order_book_data_class(symbol):
    class_name = f'RealTimeOrderBookData_{symbol.replace("/", "_")}'
    table_name = f'rt_order_book_{symbol.lower().replace("/", "_")}'
    
    return type(class_name, (Base,), {
        '__tablename__': table_name,
        'id': Column(Integer, primary_key=True),
        'symbol': Column(String),
        'timestamp': Column(DateTime),
        'bids': Column(JSON),
        'asks': Column(JSON)
    })

# Database setup
Session = sessionmaker(bind=engine)
