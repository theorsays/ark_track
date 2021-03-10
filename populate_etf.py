import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras
import asyncpg
import aiohttp
import csv
import os

connection=psycopg2.connect(host=config.DB_HOST,  database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

cursor=connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute("select * from stock WHERE is_etf = TRUE")

etfs=cursor.fetchall()
os.chdir('/Users/theolee/PycharmProjects/ark-track/data')

#dates=['2021-02-05', '2021-02-08', '2021-02-09', '2021-02-10', '2021-02-11', '2021-02-12', '2021-02-16', '2021-02-17', '2021-02-18']
dates=['2021-02-22', '2021-02-23', '2021-02-24', '2021-02-26', '2021-03-01', '2021-03-02']
for date in dates:
    for etf in etfs:
        print(etf)
        with open(f"{date}/{etf['stock_id']}.csv") as f:
            reader=csv.reader(f)
            next(reader)
            for row in reader:
                ticker = row[3]
                if ticker:
                    shares=row[5]
                    weight=row[7]

                    cursor.execute("""
                    select * from stock where stock_id= %s
                    """, (ticker,))
                    stock=cursor.fetchone()
                    if stock:
                        cursor.execute("""
                            INSERT INTO etf_holding (etf_id, holding_id, dt, shares, weight)
                            values (%s, %s, %s, %s, %s)                    
                        """, (etf[0], stock['stock_id'], date, shares, weight))

connection.commit()