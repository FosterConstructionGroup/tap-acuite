import singer
import singer.metrics as metrics
from singer import metadata
from tap_acuite.utility import (
    get_generic,
    get_all_pages,
    formatDate,
)


def handle_paginated(resource, url="", func=None):
    extraction_time = singer.utils.now()

    if url == "":
        url = resource

    def get(schema, state, mdata):
        with metrics.record_counter(resource) as counter:
            for page in get_all_pages(resource, url):
                for row in page:
                    # optional transform function
                    if func != None:
                        row = func(row)

                    write_record(row, resource, schema, mdata, extraction_time)
                    counter.increment()
        return write_bookmark(state, resource, extraction_time)

    return get


def handle_projects(schemas, state, mdata):
    extraction_time = singer.utils.now()

    rows = [
        row
        for page in get_all_pages("projects", "projects", {"includeArchived": "true"})
        for row in page
    ]
    write_many(rows, "projects", schemas["projects"], mdata, extraction_time)

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
    r = get_generic("hsevents", url)

    def get_detail(row):
        r = get_generic("hsevents", "{}/{}".format(url, row["Id"]))
        return r["Data"]

    details = [get_detail(row) for row in r["Data"]]

    for row in details:
        # keep only first 500 characters of description as it's not needed for reporting, takes up space in Redshift, and Redshift tops out at 1k characters
        if "Description" in row:
            row["Description"] = row["Description"][:500]

    write_many(details, "hsevents", schemas["hsevents"], mdata, extraction_time)

    if schemas.get("categories"):
        categories_ids = set()
        categories = []
        for evt in details:
            c = evt["SubCategory"]["ParentCategory"]
            if c["Id"] not in categories_ids:
                categories_ids.add(c["Id"])
                categories.append(c)
        write_many(
            categories, "categories", schemas["categories"], mdata, extraction_time
        )

    if schemas.get("subcategories"):
        subcategories_ids = set()
        subcategories = []
        for evt in details:
            s = evt["SubCategory"]
            if s["Id"] not in subcategories_ids:
                subcategories_ids.add(s["Id"])
                subcategories.append(s)
        write_many(
            subcategories,
            "subcategories",
            schemas["subcategories"],
            mdata,
            extraction_time,
        )

    return write_bookmark(state, "hsevents", extraction_time)


def handle_detailed(resource, url, schemas, state, mdata):
    extraction_time = singer.utils.now()
    r = get_generic(resource, url)

    def get_detail(row):
        r = get_generic(resource, "{}/{}".format(url, row["Id"]))
        return r["Data"]

    with metrics.record_counter(resource) as counter:
        for row in r["Data"]:
            detail = get_detail(row)
            write_record(detail, resource, schemas[resource], mdata, extraction_time)
            counter.increment()

    return write_bookmark(state, resource, extraction_time)


# More convenient to use but has to all be held in memory, so use write_record instead for resources with many rows
def write_many(rows, resource, schema, mdata, dt):
    with metrics.record_counter(resource) as counter:
        for row in rows:
            write_record(row, resource, schema, mdata, dt)
            counter.increment()


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", formatDate(dt))
    return state
