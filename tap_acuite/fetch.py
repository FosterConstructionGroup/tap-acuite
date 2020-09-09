import singer
import singer.metrics as metrics
from singer import metadata
from tap_acuite.utility import get_generic, get_all_pages, formatDate


def get_paginated(resource, url=""):
    extraction_time = singer.utils.now()

    if url == "":
        url = resource

    def get(schema, state, mdata):
        rows = [row for page in get_all_pages(resource, url) for row in page]
        write(rows, resource, schema, mdata, extraction_time)
        return write_bookmark(state, resource, extraction_time)

    return get


def get_projects(schemas, state, mdata):
    extraction_time = singer.utils.now()

    rows = [
        row
        for page in get_all_pages("projects", "projects", {"includeArchived": "true"})
        for row in page
    ]
    write(rows, "projects", schemas["projects"], mdata, extraction_time)

    for project in rows:
                    if schemas.get("audits"):
                        get_detailed(
                            "audits",
                "projects/{}/audits".format(project["Id"]),
                            schemas,
                            state,
                            mdata,
                        )

                    if schemas.get("rfis"):
            get_paginated("rfis", "projects/{}/rfi".format(project["Id"]))



    return write_bookmark(state, "projects", extraction_time)


def get_detailed(resource, url, schemas, state, mdata):
    extraction_time = singer.utils.now()

    def get_detail(row):
        r = get_generic(resource, "{}/{}".format(url, row["Id"]))
        return r["Data"]

    r = get_generic(resource, url)
    details = [get_detail(row) for row in r["Data"]]

    write(details, resource, schemas[resource], mdata, extraction_time)
    return write_bookmark(state, resource, extraction_time)


def write(rows, resource, schema, mdata, dt):
    with metrics.record_counter(resource) as counter:
        for row in rows:
            with singer.Transformer() as transformer:
                rec = transformer.transform(
                    row, schema, metadata=metadata.to_map(mdata)
                )
                singer.write_record(resource, rec, time_extracted=dt)
            counter.increment()


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", formatDate(dt))
    return state
