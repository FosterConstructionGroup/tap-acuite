import sys, os
import requests
import singer.metrics as metrics
from datetime import datetime


session = requests.Session()


class AuthException(Exception):
    pass


class NotFoundException(Exception):
    pass


# constants
base_url = "https://api.acuite.co.nz/"
pageSize = 1000


def get_generic(source, url, qs={}):
    with metrics.http_request_timer(source) as timer:
        query_string = build_query_string(qs)
        resp = session.request(method="get", url=base_url + url + query_string)

        if resp.status_code == 401:
            raise AuthException(resp.text)
        if resp.status_code == 403:
            raise AuthException(resp.text)
        if resp.status_code == 404:
            raise NotFoundException(resp.text)
        resp.raise_for_status()  # throw exception if not 200

        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp.json()


def get_all_pages(source, url, extra_query_string={}):
    current_page = 1

    while True:
        r = get_generic(
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


def blockStderr():
    sys.stderr = open(os.devnull, "w")


def enableStderr():
    sys.stderr = sys.__stderr__
