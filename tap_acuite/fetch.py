import singer
import singer.metrics as metrics
from singer import metadata
from singer.bookmarks import get_bookmark
from tap_acuite.utility import get_generic, get_all_pages, formatDate


def get_paginated(resource, url=""):
    extraction_time = singer.utils.now()

    if url == "":
        url = resource

    def get(schema, state, mdata):
        with metrics.record_counter(resource) as counter:
            for page in get_all_pages(resource, url):
                for row in page:
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(
                            row, schema, metadata=metadata.to_map(mdata)
                        )
                        singer.write_record(
                            resource, rec, time_extracted=extraction_time
                        )

                    counter.increment()

                    singer.write_bookmark(
                        state, resource, "since", formatDate(extraction_time),
                    )
            return state

    return get
                    counter.increment()
            return state

    return get
