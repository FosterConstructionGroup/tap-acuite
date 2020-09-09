import singer
import singer.metrics as metrics
from singer import metadata
from singer.bookmarks import get_bookmark
from tap_acuite.utility import get_all_pages, formatDate


def get_paginated(resource):
    def get(schemas, state, mdata):
        pages = list(get_all_pages(resource, resource))
        return pages

    return get
