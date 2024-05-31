import asyncio

import bs4
from bs4 import BeautifulSoup
import requests
import pandas as pd
import aiohttp
import time

host = 'https://old.reddit.com/r/all/'
# cur_path = '/catalog/female'
headers = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 6.0; rv:14.0) Gecko/20100101 '
                   'Firefox/14.0.1')
}
max_iter = 500


class Post:
    text: str
    vote_count: int

    def __init__(self, text: str, vote_count: int):
        self.text = text
        self.vote_count = vote_count


def process_page(url, results) -> str:
    with requests.Session() as session:
        try:
            time.sleep(2)
            response = session.get(url, headers=headers)
            data = response.content
            soup = BeautifulSoup(data, "html.parser")
            items = scrape_page(soup)
            results += items
            return func_or_null(soup.find("a", attrs={"rel": "nofollow next"}), lambda x: x.attrs["href"])
        except aiohttp.client_exceptions.ClientConnectorError:
            "Cannot connect to host"
            return None


def func_or_null(value, func):
    return func(value) if value else None


def int_or_null(value):
    return func_or_null(value, int)


def scrape_page(page: bs4.Tag) -> list[Post]:
    news_block = page.find_all("div", attrs={"data-subreddit": True})
    news_texts = list(
        map(lambda x: func_or_null(x.find("a", attrs={"data-event-action": "title"}), lambda tag: tag.text),
            news_block))
    news_votes = list(map(lambda x: int_or_null(x.attrs["data-score"]), news_block))
    return [Post(text, vote) for (text, vote) in zip(news_texts, news_votes)]


if __name__ == "__main__":
    results = []
    current_url = str(host)
    for i in range(max_iter):
        print(i)
        if current_url is None:
            break
        current_url = process_page(current_url, results)

    total_result = pd.DataFrame(list(map(lambda x: x.__dict__, results)))
    total_result.to_csv("./results-all.csv", index=False)
