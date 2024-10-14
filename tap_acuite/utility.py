import os
import asyncio
from tenacity import retry, stop_after_attempt
from datetime import datetime


# constants
base_url = "https://api.acuite.io/"
base_page_size = 1000
sem = None
base_format = "%Y-%m-%dT%H:%M:%S"

# semaphore needs to be initialised within the main asyncio loop or it will make its own and cause issues
def initialise_semaphore():
    global sem
    sem = asyncio.Semaphore(32)


# requests don't normally fail, but sometimes there's an intermittent 500
@retry(stop=stop_after_attempt(10))
async def get_generic(session, source, url, qs={}):
    async with sem:
        query_string = build_query_string(qs)
        # print("### URL:", base_url + url + query_string)
        async with await session.get(
            base_url + url + query_string, raise_for_status=True
        ) as resp:
            return await resp.json()


async def get_all(session, source, url, extra_query_string={}):
    page_size = base_page_size if source != "people" else 100

    async def get_page(page_number):
        return (
            await get_generic(
                session,
                source,
                url,
                {
                    **extra_query_string,
                    "pageNumber": page_number,
                    "pageSize": page_size,
                },
            )
        )["Data"]

    json = await get_page(1)

    if json["NumberOfPages"] == 1:
        return json["Items"]

    pages = await asyncio.gather(
        *[get_page(page_number) for page_number in range(2, 1 + json["NumberOfPages"])]
    )
    pages.insert(0, json)

    return [row for page in pages for row in page["Items"]]


def format_date(dt, format=base_format):
    return datetime.strftime(dt, format)


def parse_date(str, format=base_format):
    try:
        return datetime.strptime(str, format)
    except:
        if format == base_format:
            raise
        else:
            return datetime.strptime(str, base_format)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def build_query_string(dict):
    if len(dict) == 0:
        return ""

    return "?" + "&".join(["{}={}".format(k, v) for k, v in dict.items()])
