import singer
import singer.metrics as metrics
from singer import metadata
from tap_acuite.utility import (
    get_generic,
    get_all_pages,
    formatDate,
)


def handle_paginated(resource, url=""):
    extraction_time = singer.utils.now()

    if url == "":
        url = resource

    def get(schema, state, mdata):
        rows = [row for page in get_all_pages(resource, url) for row in page]
        write(rows, resource, schema, mdata, extraction_time)
        return write_bookmark(state, resource, extraction_time)

    return get


def handle_projects(schemas, state, mdata):
    extraction_time = singer.utils.now()

    rows = [
        row
        for page in get_all_pages("projects", "projects", {"includeArchived": "true"})
        for row in page
    ]
    write(rows, "projects", schemas["projects"], mdata, extraction_time)

    for project in rows:
        if schemas.get("audits"):
            handle_detailed(
                "audits",
                "projects/{}/audits".format(project["Id"]),
                schemas,
                state,
                mdata,
            )

        if schemas.get("hsevents"):
            handle_hsevents(
                "projects/{}/hse/events".format(project["Id"]), schemas, state, mdata
            )

        if schemas.get("rfis"):
            handle_paginated("rfis", "projects/{}/rfi".format(project["Id"]))(
                schemas["rfis"], state, mdata
            )

    return write_bookmark(state, "projects", extraction_time)


def handle_hsevents(url, schemas, state, mdata):
    extraction_time = singer.utils.now()

    def get_detail(row):
        r = get_generic("hsevents", "{}/{}".format(url, row["Id"]))
        return r["Data"]

    r = get_generic("hsevents", url)
    details = [get_detail(row) for row in r["Data"]]

    write(details, "hsevents", schemas["hsevents"], mdata, extraction_time)

    if schemas.get("categories"):
        categories_ids = set()
        categories = []
        for evt in details:
            c = evt["SubCategory"]["ParentCategory"]
            if c["Id"] not in categories_ids:
                categories_ids.add(c["Id"])
                categories.append(c)
        write(categories, "categories", schemas["categories"], mdata, extraction_time)

    if schemas.get("subcategories"):
        subcategories_ids = set()
        subcategories = []
        for evt in details:
            s = evt["SubCategory"]
            if s["Id"] not in subcategories_ids:
                subcategories_ids.add(s["Id"])
                subcategories.append(s)
        write(
            subcategories,
            "subcategories",
            schemas["subcategories"],
            mdata,
            extraction_time,
        )

    return write_bookmark(state, "hsevents", extraction_time)


def handle_detailed(resource, url, schemas, state, mdata):
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
