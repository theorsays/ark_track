import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras
import asyncpg
import aiohttp
names=[]


connection=psycopg2.connect(host=config.DB_HOST,  database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

cursor=connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

api=tradeapi.REST(config.API_KEY, config.API_SECRET, base_url=config.API_URL)

assets=api.list_assets()


for asset in assets:
    if asset.symbol not in names:
        if asset.symbol!='XL' and asset.symbol!='CZR'and asset.symbol!='KOR'and asset.symbol!='MUDSU':

            cursor.execute("""
                insert into stock (stock_id, name, exchange, is_etf) 
                values(%s, %s, %s, false)
            """, (asset.symbol, asset.name, asset.exchange))
        names.append(asset.symbol)
connection.commit()
