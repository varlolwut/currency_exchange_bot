from bs4 import BeautifulSoup as bs
import pandas as pd
import sqlite3
import requests
from datetime import datetime

DATABASE = './exchange_rate.sql'


def get_currency_rate_page(currency_rate_url):
    main_url = currency_rate_url
    r = requests.get(main_url)
    return r


def parse_eubank(html_page):
    soup = bs(html_page.text, "html.parser")
    exchange_types_tmp = ['В обменных пунктах', 'В Smartbank', 'Для юридических лиц', 'Золотые слитки']
    exchange_types = [val for val in exchange_types_tmp for _ in (0, 1)]
    tmp_result = []
    for tables, et in zip(soup.find_all('div', class_='exchange__col'), exchange_types):
        for _ in tables.find_all('h3', class_='exchange__title'):
            operation_type = tables.find('h3', class_='exchange__title')  # тип операции
            titles = tables.find_all('span', class_='exchange-table__title')  # валюта
            values = tables.find_all('span', class_='exchange-table__value')  # курс
            tmp_result.append([{'transaction_source': et, 'transaction_type': operation_type.text,
                                'currency': currency.text, 'rate': rate.text} for currency, rate in
                               zip(titles, values)])
    final_df = pd.DataFrame(sum(tmp_result, []))
    final_df['ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # print(final_df.head())
    return final_df


tmp = parse_eubank(get_currency_rate_page("https://eubank.kz/exchange-rates/"))


def sqlite_write(db_location):
    conn = sqlite3.connect(db_location)
    tmp.to_sql(name='eurasian_bank', con=conn)
    conn.close()
