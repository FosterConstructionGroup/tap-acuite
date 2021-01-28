import asyncio
import singer
import singer.metrics as metrics
from singer import metadata
from tap_acuite.utility import (
    get_generic,
    get_all,
    formatDate,
)


def handle_paginated(resource, url="", func=None):
    extraction_time = singer.utils.now()

    if url == "":
        url = resource

    async def get(session, schema, state, mdata):
        with metrics.record_counter(resource) as counter:
            for row in await get_all(session, resource, url):
                # optional transform function
                if func != None:
                    row = func(row)

                write_record(row, resource, schema, mdata, extraction_time)
                counter.increment()
        return (resource, extraction_time)

    return get


async def handle_projects(session, schemas, state, mdata):
    extraction_time = singer.utils.now()

    rows = await get_all(session, "projects", "projects", {"includeArchived": "true"})
    write_many(rows, "projects", schemas["projects"], mdata, extraction_time)

    def add_project_id(project):
        def add(row):
            row["ProjectId"] = project["Id"]
            return row

        return add

    subqueries = []
    for project in rows:
        if schemas.get("audits"):
            subqueries.append(
                handle_audits(session, project["Id"], schemas, state, mdata,)
            )
        if schemas.get("hsevents"):
            subqueries.append(
                handle_hsevents(session, project["Id"], schemas, state, mdata)
            )
        if schemas.get("rfis"):
            subqueries.append(
                handle_paginated(
                    "rfis",
                    f"projects/{project['Id']}/rfi",
                    func=add_project_id(project),
                )(session, schemas["rfis"], state, mdata)
            )
    await asyncio.gather(*subqueries)

    return ("projects", extraction_time)


async def handle_hsevents(session, project_id, schemas, state, mdata):
    url = f"projects/{project_id}/hse/events"
    extraction_time = singer.utils.now()

    sync_categories = schemas.get("categories")
    sync_subcategories = schemas.get("subcategories")
    categories_ids = set() if sync_categories else None
    subcategories_ids = set() if sync_subcategories else None

    res = await get_generic(session, "hsevents", url)
    # immediately discard everything except the ID to minimise memory footprint (could be holding this array for a while)
    row_ids = [row["Id"] for row in res["Data"]]

    columns_to_trim = [
        "Description",
        "PreventativeAction",
        "ActionTaken",
        "WeatherConditions",
    ]

    # do all processing at the row level, including writing records one at a time
    # this should minimise memory usage
    async def get_detail(id):
        r = await get_generic(session, "hsevents", f"{url}/{id}")
        row = r["Data"]
        # Project ID isn't returned in the record, so add it
        row["ProjectId"] = project_id

        # keep only first 500 characters of these columns as they aren't needed for reporting, take up space in Redshift, and Redshift tops out at 1k characters
        for col in columns_to_trim:
            if col in row and len(row[col]) > 500:
                row[col] = row[col][:500]

        write_record(row, "hsevents", schemas["hsevents"], mdata, extraction_time)

        if sync_categories:
            c = row["SubCategory"]["ParentCategory"]
            if c["Id"] not in categories_ids:
                categories_ids.add(c["Id"])
                write_record(
                    c, "categories", schemas["categories"], mdata, extraction_time
                )
        if sync_subcategories:
            s = row["SubCategory"]
            if s["Id"] not in subcategories_ids:
                subcategories_ids.add(s["Id"])
                write_record(
                    s, "subcategories", schemas["subcategories"], mdata, extraction_time
                )

    await asyncio.gather(*[get_detail(id) for id in row_ids])

    return ("hsevents", extraction_time)


async def handle_audits(session, project_id, schemas, state, mdata):
    url = f"projects/{project_id}/audits"
    resource = "audits"
    extraction_time = singer.utils.now()
    r = await get_generic(session, resource, url)

    with metrics.record_counter(resource) as counter:
        for row in r["Data"]:
            detail = await get_generic(session, resource, f"{url}/{row['Id']}")
            detail = detail["Data"]
            detail["ProjectId"] = project_id

            # add section ID to each question
            try:
                for section in detail["Sections"]:
                    for question in section["Questions"]:
                        question["section_id"] = section["Id"]
                        # Trim to max 500 characters as Redshift has max length 1k characters
                        question["Answer"] = question["Answer"][:500]
            except:
                pass

            write_record(detail, resource, schemas[resource], mdata, extraction_time)
            counter.increment()

    return (resource, extraction_time)


async def handle_detailed(session, resource, url, schemas, state, mdata):
    extraction_time = singer.utils.now()
    r = await get_generic(session, resource, url)

    with metrics.record_counter(resource) as counter:
        for row in r["Data"]:
            detail = await get_generic(session, resource, f"{url}/{row['Id']}")
            write_record(
                detail["Data"], resource, schemas[resource], mdata, extraction_time
            )
            counter.increment()

    return (resource, extraction_time)


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
