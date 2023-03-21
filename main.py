import asyncio
from parse import parse_sber_async, parse_eubank_async, parse_allbanks_async
from sqlite_operator import sqlite_write
from urllib.parse import urlparse

DATABASE = './exchange_rate.sql'
CHROMEDRIVER = "./chromedriver"
URL_LIST = ['https://sber.kz/', 'https://eubank.kz/exchange-rates/', 'https://allbanks.kz/exchange_rates']

async def main():
    async with aiohttp.ClientSession() as session:
        df_list = [
            await parse_sber_async(CHROMEDRIVER, URL_LIST[0], session),
            await parse_eubank_async(session, URL_LIST[1]),
            await parse_allbanks_async(session, URL_LIST[2])
        ]
        for frame, url in zip(df_list, URL_LIST):
            frame['rate'] = frame['rate'].astype(float)
            frame['ts'] = pd.to_datetime(frame['ts'])
            sqlite_write(DATABASE, frame, urlparse(url).netloc.split(".", 1)[0])

if __name__ == '__main__':
    asyncio.run(main())
