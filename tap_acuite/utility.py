import os
import asyncio
from tenacity import retry, stop_after_attempt
import singer.metrics as metrics
from datetime import datetime


# constants
base_url = "https://api.acuite.co.nz/"
pageSize = 1000
sem = None

# semaphore needs to be initialised within the main asyncio loop or it will make its own and cause issues
def initialise_semaphore():
    global sem
    sem = asyncio.Semaphore(32)


# requests don't normally fail, but sometimes there's an intermittent 500
@retry(stop=stop_after_attempt(4))
async def get_generic(session, source, url, qs={}):
    async with sem:
        with metrics.http_request_timer(source) as timer:
            query_string = build_query_string(qs)
            async with await session.get(
                base_url + url + query_string, raise_for_status=True
            ) as resp:
                timer.tags[metrics.Tag.http_status_code] = resp.status
                return await resp.json()


async def get_all_pages(session, source, url, extra_query_string={}):
    current_page = 1

    while True:
        r = await get_generic(
            session,
            source,
            url,
            {**extra_query_string, "pageNumber": current_page, "pageSize": pageSize},
        )
        json = r["Data"]
        yield json["Items"]
        if json["NumberOfPages"] > json["CurrentPage"]:
            current_page = json["CurrentPage"] + 1
        else:
            break


def formatDate(dt):
    return datetime.strftime(dt, "%Y-%m-%dT%H:%M:%S")


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def build_query_string(dict):
    if len(dict) == 0:
        return ""

    return "?" + "&".join(["{}={}".format(k, v) for k, v in dict.items()])
