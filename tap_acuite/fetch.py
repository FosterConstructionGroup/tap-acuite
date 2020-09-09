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
def get_projects(schemas, state, mdata):
    extraction_time = singer.utils.now()

    with metrics.record_counter("projects") as counter:
        with metrics.record_counter("attendances") as a_counter:
            for page in get_all_pages(
                "projects", "projects", {"includeArchived": "true"}
            ):
                for row in page:
                    # handle sites record
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(
                            row, schemas["projects"], metadata=metadata.to_map(mdata)
                        )
                        singer.write_record(
                            "projects", rec, time_extracted=extraction_time
                        )

                    counter.increment()

                    if schemas.get("audits"):
                        get_detailed(
                            "audits",
                            "projects/{}/audits".format(row["Id"]),
                            schemas,
                            state,
                            mdata,
                        )

                    if schemas.get("rfis"):
                        get_paginated("rfis", "projects/{}/rfi".format(row["Id"]))

    singer.write_bookmark(
        state, "projects", "since", formatDate(extraction_time),
    )
            return state


def get_detailed(resource, url, schemas, state, mdata):
    extraction_time = singer.utils.now()

    r = get_generic(resource, url)
    rows = r["Data"]

    with metrics.record_counter(resource) as counter:
        for row in rows:
            res = get_generic(resource, "{}/{}".format(url, row["Id"]))
            detail = res["Data"]

            with singer.Transformer() as transformer:
                rec = transformer.transform(
                    detail, schemas[resource], metadata=metadata.to_map(mdata)
                )
                singer.write_record(resource, rec, time_extracted=extraction_time)

            counter.increment()

    singer.write_bookmark(
        state, resource, "since", formatDate(extraction_time),
    )
    return state
