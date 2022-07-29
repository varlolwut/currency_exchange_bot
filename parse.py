from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from datetime import datetime
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

ChromeOptions = Options()
ChromeOptions.headless = True


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
    final_df['bank_name'] = 'Евразийский Банк'
    # print(final_df.head())
    return final_df


def parse_sber(sel_driver, url):
    count = 0
    driver = Chrome(executable_path=sel_driver)
    driver.get(url)
    news_element = driver.find_element(By.CLASS_NAME, "rates__table").text
    words_to_list = news_element.split()

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
    # print(final_df.head())
    driver.close()
    return final_df


def parse_allbanks(html_page):
    soup = bs(html_page.text, "html.parser")
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
            print()
            df = df.append(
                {'bank_name': bank_name, 'USD_Покупка': usd_buy, 'USD_Продажа': usd_sell, 'EUR_Покупка': eur_buy,
                 'EUR_Продажа': eur_sell, 'RUB_Покупка': rub_buy, 'RUB_Продажа': rub_sell}, ignore_index=True)
            # df = df.concat()
    #         df['transaction_source'] = 'unspecified'
    #         df['ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = df.melt(id_vars=["bank_name"],
                 var_name="currency",
                 value_name="rate")
    df[['currency', 'operation_type']] = df['currency'].str.split('_', expand=True)
    df['transaction_source'] = 'unspecified'
    df['ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # print(df)
    return df



