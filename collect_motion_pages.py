import requests
import parslepy
import tqdm
import asyncio


links = set()

parselet_list = parslepy.Parselet({
    'moties(.m-card__main)': [{
        'link': '.h-link-inverse::attr(href)'
    }]
})


async def main():
    loop = asyncio.get_event_loop()
    futures = []

    for i in range(1, 5000, 15):
        futures.append(loop.run_in_executor(
            None, requests.get, f'https://www.tweedekamer.nl/kamerstukken/moties?qry=*&cfg=tksearch&fld_tk_categorie=Kamerstukken&fld_prl_kamerstuk=Moties&srt=date%3Adesc%3Adate&sta={i}'))
    responses = []
    for future in tqdm.tqdm(futures):
        responses.append(await future)

    for response in responses:
        extracted = parselet_list.parse_fromstring(response.content)
        for link in [x['link'] for x in extracted['moties']]:
            links.add(f'https://www.tweedekamer.nl{link}')

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

with open('motion_links.txt', 'w') as f:
    for link in links:
        f.write(link + '\n')
