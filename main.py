from parse import *
from sqlite_operator import *
from urllib.parse import urlparse

DATABASE = './exchange_rate.sql'
CHROMEDRIVER = "./chromedriver"
URL_LIST = ['https://sber.kz/', 'https://eubank.kz/exchange-rates/', 'https://allbanks.kz/exchange_rates']

if __name__ == '__main__':
    df_list = [parse_sber(CHROMEDRIVER, URL_LIST[0]), parse_eubank(get_currency_rate_page(URL_LIST[1])),
               parse_allbanks(get_currency_rate_page(URL_LIST[2]))]
    for frame, url in zip(df_list, URL_LIST):
        frame['rate'] = frame['rate'].astype(float)
        frame['ts'] = pd.to_datetime(frame['ts'])
        sqlite_write(DATABASE, frame, urlparse(url).netloc.split(".", 1)[0])
        # print(frame.head())
        # print(url)
        # print(urlparse(url).netloc.split(".", 1)[0])