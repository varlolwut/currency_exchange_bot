import aiohttp
import asyncio
import re
from bs4 import BeautifulSoup as bs
from datetime import datetime
import pandas as pd

async def get_currency_rate_page(session, url):
    async with session.get(url) as response:
        return await response.text()

def parse_eubank(html_page):
    non_decimal = re.compile(r'[^\d.]+')
    soup = bs(html_page, "html.parser")
    exchange_types_tmp = ['В обменных пунктах', 'В Smartbank', 'Для юридических лиц', 'Золотые слитки']
    exchange_types = [val for val in exchange_types_tmp for _ in (0, 1)]
    tmp_result = []
    for tables, et in zip(soup.select('div.exchange__col'), exchange_types):
        operation_type = tables.h3.text.strip()  # тип операции
        titles = [title.text for title in tables.select('span.exchange-table__title')]  # валюта
        values = [value.text for value in tables.select('span.exchange-table__value')]  # курс
        tmp_result.extend([{'transaction_source': et, 'transaction_type': operation_type,
                            'currency': currency, 'rate': non_decimal.sub('', rate)} for currency, rate in
                           zip(titles, values)])
    final_df = pd.DataFrame(tmp_result)
    final_df['ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    final_df['bank_name'] = 'Евразийский Банк'
    return final_df

async def parse_eubank_async(session, url):
    html_page = await get_currency_rate_page(session, url)
    return parse_eubank(html_page)

async def parse_sber(sel_driver, url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html_page = await response.text()
            non_decimal = re.compile(r'[^\d.]+')
            soup = bs(html_page, "html.parser")
            news_element = soup.select('.rates__table')[0].text
            words_to_list = news_element.split()
            count = 0
            for _ in words_to_list:
                count = count + 1
            newstring = words_to_list[0:2] + words_to_list[count - 2: count]
            tmp_list = [{'transaction_source': 'unspecified', 'transaction_type': newstring[0],
                         'currency': 'RUB', 'rate': newstring[-2]},
                        {'transaction_source': 'unspecified', 'transaction_type': newstring[1],
                         'currency': 'RUB', 'rate': newstring[-1]}]
            final_df = pd.DataFrame(tmp_list)
            final_df['ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            final_df['bank_name'] = 'Сбербанк Казахстан'
            return final_df

async def parse_allbanks(html_page):
    soup = bs(html_page, "html.parser")
    table = soup.find('table', class_='top15')
    df = pd.DataFrame(
        columns=['bank_name', 'USD_Покупка', 'USD_Продажа', 'EUR_Покупка', 'EUR_Продажа', 'RUB_Покупка', 'RUB_Продажа'])
    for row in table.tbody.find_all('tr'):
        columns = row.find_all('td')
        if (columns != []):
            bank_name = columns[0].text
            usd_buy = columns[1].text
            usd_sell = columns[2].text
            eur_buy = columns[3].text
            eur_sell = columns[4].text
            rub_buy = columns[5].text
            rub_sell = columns[6].text
            df = df.append(
                {'bank_name': bank_name, 'USD_Покупка': usd_buy, 'USD_Продажа': usd_sell, 'EUR_Покупка': eur_buy,
                 'EUR_Продажа': eur_sell, 'RUB_Покупка': rub_buy, 'RUB_Продажа': rub_sell}, ignore_index=True)
    df = df.melt(id_vars=["bank_name"],
                 var_name="currency",
                 value_name="rate")
    df[['currency', 'operation_type']] = df['currency'].str.split('_', expand=True)
    df['transaction_source'] = 'unspecified'
    df['ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return df
