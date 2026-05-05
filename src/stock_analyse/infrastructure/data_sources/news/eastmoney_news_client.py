#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Desc: 个股新闻数据
https://so.eastmoney.com/news/s?keyword=%E4%B8%AD%E5%9B%BD%E4%BA%BA%E5%AF%BF&pageindex=1&searchrange=8192&sortfiled=4
"""
import json
import logging
import re
from datetime import datetime
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
except Exception:
    webdriver = None
    Service = None

logger = logging.getLogger(__name__)


class stockNewsData:

    @staticmethod
    def _build_url(symbol: str, page_size: int) -> str:
        params = {
            "uid": "",
            "keyword": symbol,
            "type": ["cmsArticleWebOld"],
            "client": "web",
            "clientType": "web",
            "clientVersion": "curr",
            "param": {
                "cmsArticleWebOld": {
                    "searchScope": "default",
                    "sort": "default",
                    "pageIndex": 1,
                    "pageSize": page_size,
                    "preTag": "<em>",
                    "postTag": "</em>",
                }
            },
        }
        encoded_params = quote(json.dumps(params), safe='')
        return (
            'https://search-api-web.eastmoney.com/search/jsonp?'
            f'cb=jQuery35108613950799967576_1701396301284&param={encoded_params}&_=1701396301285'
        )

    @staticmethod
    def _parse_news_payload(data_text: str, symbol: str) -> pd.DataFrame:
        match = re.search(r'jQuery\d+_\d+\((.*)\)\s*$', data_text.strip(), re.DOTALL)
        if not match:
            raise ValueError('unexpected Eastmoney news payload')
        payload = json.loads(match.group(1))
        records = payload.get('result', {}).get('cmsArticleWebOld', [])
        temp_df = pd.DataFrame(records)
        if temp_df.empty:
            return pd.DataFrame(columns=['关键词', '新闻标题', '新闻内容', '发布时间', '文章来源', '新闻链接'])
        temp_df.rename(
            columns={
                'date': '发布时间',
                'mediaName': '文章来源',
                'code': '-',
                'title': '新闻标题',
                'content': '新闻内容',
                'url': '新闻链接',
                'image': '-',
            },
            inplace=True,
        )
        temp_df['关键词'] = symbol
        temp_df = temp_df[['关键词', '新闻标题', '新闻内容', '发布时间', '文章来源', '新闻链接']]
        for col in ['新闻标题', '新闻内容']:
            temp_df[col] = (
                temp_df[col]
                .fillna('')
                .astype(str)
                .str.replace(r'\(<em>', '', regex=True)
                .str.replace(r'</em>\)', '', regex=True)
                .str.replace(r'<em>', '', regex=True)
                .str.replace(r'</em>', '', regex=True)
                .str.replace(r'　', '', regex=True)
                .str.replace(r'\r\n', ' ', regex=True)
            )
        return temp_df

    @classmethod
    def _fetch_via_http(cls, symbol: str, page_size: int) -> pd.DataFrame:
        request = Request(
            cls._build_url(symbol, page_size),
            headers={
                'User-Agent': 'Mozilla/5.0',
                'Accept': '*/*',
                'Referer': 'https://so.eastmoney.com/',
            },
        )
        with urlopen(request, timeout=10) as response:
            data_text = response.read().decode('utf-8', errors='ignore')
        return cls._parse_news_payload(data_text, symbol)

    @classmethod
    def _fetch_via_selenium(cls, symbol: str, page_size: int, chrome_driver_path: str = '') -> pd.DataFrame:
        if webdriver is None or Service is None:
            raise RuntimeError('selenium webdriver unavailable')
        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('headless')
        service = Service(chrome_driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        try:
            driver.get(cls._build_url(symbol, page_size))
            return cls._parse_news_payload(driver.page_source, symbol)
        finally:
            driver.quit()

    @classmethod
    def stock_news_em(cls, symbol: str = '601628', pageSize: int = 10, chrome_driver_path='') -> pd.DataFrame:
        """
        东方财富-个股新闻-最近 100 条新闻
        https://so.eastmoney.com/news/s?keyword=%E4%B8%AD%E5%9B%BD%E4%BA%BA%E5%AF%BF&pageindex=1&searchrange=8192&sortfiled=4
        :param symbol: 股票代码
        :type symbol: str
        :return: 个股新闻
        :rtype: pandas.DataFrame
        """
        try:
            return cls._fetch_via_http(symbol, pageSize)
        except Exception as http_error:
            logger.warning('stock_news_em http failed | symbol=%s | error=%s', symbol, http_error)
        try:
            return cls._fetch_via_selenium(symbol, pageSize, chrome_driver_path)
        except Exception as selenium_error:
            logger.warning('stock_news_em selenium failed | symbol=%s | error=%s', symbol, selenium_error)
            return pd.DataFrame()

    def save_to_excel(df: pd.DataFrame, symbol: str):
        """
        将 DataFrame 写入 Excel 文件，并以股票代码加时间戳字符串为后缀保存
        :param df: DataFrame
        :param symbol: 股票代码
        :type df: pd.DataFrame
        :type symbol: str
        """
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
        filename_with_timestamp = f"{symbol}_{timestamp_str}.xlsx"
        df.to_excel(filename_with_timestamp, index=False)
        print(f"数据已保存至 {filename_with_timestamp}")
