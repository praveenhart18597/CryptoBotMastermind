from sqlalchemy import create_engine
engine = create_engine('mssql+pyodbc://JUVINPCDBSERVER')

try:
    connection = engine.connect()
    print("Connection successful!")
    connection.close()
except Exception as e:
    print("Connection failed:", e)
