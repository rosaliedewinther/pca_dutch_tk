from enum import Enum
import requests
import parslepy
import locale
import pandas as pd
import time
import asyncio
from tqdm import tqdm
from datetime import datetime
locale.setlocale(locale.LC_ALL, "nl_NL")

parselet_detail = parslepy.Parselet({
    'naam': 'h1:not(span)',
    'datum': '.u-pr--collapse-at-small > ul > li:first-child > div',
    'ondertekenaars(.m-list__item--variant-member)': [{
        'name': '.'
    }],
    'voor(table.u-text-color--primary)': ['tr'],
    'tegen(table:not(.u-text-color--primary))': ['tr']
})

session = requests.Session()
df = pd.DataFrame()
f = open('errors.txt', 'w')


def handle_link(response):
    extracted = parselet_detail.parse_fromstring(response.content)

    extracted['naam'] = extracted['naam'].split(': ', 1)[1]
    extracted['ondertekenaars'] = [name['name'].split(
        ', ')[0].removeprefix("Eerste ondertekenaar").removeprefix("Mede ondertekenaar").removeprefix("Indiener") for name in extracted['ondertekenaars']]

    extracted['ondertekenaars'] = ', '.join(
        extracted['ondertekenaars'])

    extracted['datum'] = datetime.strptime(
        extracted['datum'], "%d %B %Y")

    for partij in extracted['voor']:
        if partij is not None:
            extracted[partij.rsplit(' ', 1)[0]] = int(1)
    for partij in extracted['tegen']:
        if partij is not None:
            extracted[partij.rsplit(' ', 1)[0]] = int(0)
    del extracted['voor']
    del extracted['tegen']
    return extracted


async def main():
    loop = asyncio.get_event_loop()
    futures = []

    with open('motion_links.txt') as file:
        for line in file:
            futures.append(loop.run_in_executor(
                None, requests.get, line.rstrip()))

    responses = []
    for future in tqdm(futures):
        responses.append(await future)

    df = pd.DataFrame()
    for response in responses:
        extracted = handle_link(response)
        df = pd.concat([df, pd.DataFrame(extracted, index=[0])])

    df = df.reset_index(drop=True)
    df.to_csv("out.csv")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
